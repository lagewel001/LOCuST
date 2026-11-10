import json
import os
import re
from abc import ABC, abstractmethod
from rdflib import Graph, Namespace
from rdflib.namespace import SKOS, DCTERMS as DCT, QB, RDF
from typing import Tuple, Optional, List

import config
from models.generators.base_generator import BaseGenerator
from models.retrievers.base_retriever import BaseRetriever
from odata_graph import engine
from odata_graph.sparql_controller import QUDT, GECKO, XKOS, TOTAL_PATTERN
from s_expression import Table, Measure, Dimension, uri_to_code
from utils.custom_types import LLMAnswerStatus, LLMResponse, LLMPrompt, QueryType

# Dimension levels with at most this many codes stay literally listed in prompts even when the retriever
# did not link them (NL/LD/PV, small year lists. This is used as a safety net so unlinked questions
# degrade to "the model must pick from 12 provinces", never must know an unseen code.
SMALL_LEVEL_MAX = 15


def partition_info(table_graph: Graph, ns: Namespace) -> Tuple[dict, dict]:
    """
        Extract the table-scoped partition layer from an exploded table graph, for partition
        groups minted in the given namespace (onderwerp -> measure groups, dimensie ->
        dimension partitions): which member sits in which group, plus per-group label and
        whether the group denotes a total (gecko:Aggregate). Geo containment edges also use
        xkos:isPartOf, but their parents are plain area codes that belong to no scheme, so
        requiring skos:inScheme on the group drops them (the table-graph CONSTRUCT already
        emits only scheme-scoped partitions; the check makes this helper safe regardless).

        :param table_graph: graph as returned by SparqlEngine.get_table_graph
        :param ns: namespace the partition group nodes must live in
        :returns: ({member_code: group_uri}, {group_uri: {'label': str|None, 'total': bool}})
    """
    partition_of, partitions = {}, {}
    for member, group in table_graph.subject_objects(XKOS.isPartOf):
        group_uri = str(group)
        if not group_uri.startswith(str(ns)) or (group, SKOS.inScheme, None) not in table_graph:
            continue
        partition_of[uri_to_code(member)] = group_uri
        if group_uri not in partitions:
            label = table_graph.value(group, SKOS.prefLabel)
            partitions[group_uri] = {'label': str(label) if label else None,
                                     'total': (group, RDF.type, GECKO.Aggregate) in table_graph}
    return partition_of, partitions


class BaseLLMGenerator(BaseGenerator, ABC):
    """
        A baseline model for generating SQL queries using a LLM.

        :param retriever: table/node retriever that is used for populating the prompt going out to the LLM
        :param node_score_threshold: score threshold for node items in tables for adding to the prompt.
                                     If the retriever does not return node scores, this value is ignored.
                                     Set to 0 to add all nodes connected to a table in a prompt (might yield very large prompts!)
    """
    retriever: BaseRetriever
    node_score_threshold: float = 0.1
    enable_linking: bool = True  # rule-based geo/time entity-linking on retrieved tables; off for ablations
    _linker = None  # lazy class-level singleton (gazetteer builds once per process)

    @staticmethod
    def _get_linker():
        from models.retrievers.geo_time_linker import GeoTimeLinker  # heavy init; import lazily
        if BaseLLMGenerator._linker is None:
            BaseLLMGenerator._linker = GeoTimeLinker()
        return BaseLLMGenerator._linker

    def _build_prompt(self,
                      question: str,
                      tables: dict,
                      query_type: QueryType = 'sql',
                      max_nodes_per_table: int = 500) -> LLMPrompt:
        """
            Builds a prompt for the LLM based on the question and retrieved tables.

            :param question: question string
            :param tables: dictionary of ranked tables and their nodes
            :param max_nodes_per_table: maximum number of nodes to include per table
        """
        table_info = []
        for table_id, data in tables.items():
            # Get graph elements for tables
            table_node = Table(table_id)

            # Level summary for time/geo dimension groups. Tables with geo dims can have too many to enumerate in a
            # prompt. Instead, the group line lists its levels with code counts and only the codes the retriever/linker
            # tied to the question (score >= threshold) plus the codes of small levels are kept as literal entries.
            level_summaries = engine.get_dim_level_summary(table_node, include_codes_upto=SMALL_LEVEL_MAX)
            small_level_codes = {code for entries in level_summaries.values() for *_, codes in entries if codes for code in codes}

            node_scores = data.get('nodes', {})
            table_graph = engine.get_table_graph(table_node,
                                                 geo_time_codes=small_level_codes | set(node_scores))

            # Separate the dimension types into buckets for rule-based extraction of geo and time dims from the query
            dim_groups = {uri_to_code(s) for s in set(table_graph.objects(predicate=QB.dimension)) -
                          set(table_graph.subjects(predicate=SKOS.broader))}
            geo_dims = {uri_to_code(s) for s in
                        table_graph.subjects(predicate=RDF.type, object=GECKO.GeoDimension)} - dim_groups
            time_dims = {uri_to_code(s) for s in
                         table_graph.subjects(predicate=RDF.type, object=GECKO.TimeDimension)} - dim_groups
            schema_dims = {uri_to_code(s) for s in table_graph.objects(predicate=QB.dimension)} - dim_groups - geo_dims - time_dims

            # Units: the standardized QUDT unit plus the CBS unit, which also carries the multiplier (e.g. 'x 1 000'),
            # since aggregating over measures requires the same unit AND the same multiplier
            qudt_units = {uri_to_code(m): str(u).split('/')[-1] for m, u in table_graph.subject_objects(QUDT.unit)}
            cbs_units = {uri_to_code(m): str(u).strip() for m, u in table_graph.subject_objects(QUDT.unitOfSystem)}
            units = {code: ', '.join(dict.fromkeys(u for u in (qudt_units.get(code), cbs_units.get(code)) if u))
                     for code in qudt_units.keys() | cbs_units.keys()}
            concept_labels = {node: str(label)
                              for node, concept in table_graph.subject_objects(QB.concept)
                              if (label := table_graph.value(concept, SKOS.prefLabel)) is not None}

            # Setup prompt
            table_labels = set(table_graph.objects(table_node.uri, DCT.title) or {''})
            if len(table_labels) == 0:
                table_labels = {''}
            table_info.append(f"Table: {table_id} "
                              f"(label: \"{str(table_labels.pop())}\") "
                              f"(score: {data.get('score', '-')})")

            # Build a mapping from dimension codes to their parent group via skos:broader
            dim_to_group = {}
            for child, parent in table_graph.subject_objects(SKOS.broader):
                child_code = uri_to_code(child)
                parent_code = uri_to_code(parent)
                if parent_code in dim_groups:
                    dim_to_group[child_code] = parent_code

            # Partition layer (table-scoped group schemes): measures partition into measure groups, dimension codes into
            # their dimension partitions. Aggregate-marked partitions denote totals, surfaced in the prompt so the LLM
            # can respect the coherence rules (aggregate within ONE partition; never sum a total with its parts).
            msr_partition_of, msr_partitions = partition_info(table_graph, Measure.rdf_ns)
            dim_partition_of, dim_partitions = partition_info(table_graph, Dimension.rdf_ns)

            # Build node entries, filtering out irrelevant dimensions
            node_scores = data.get('nodes', {})
            measure_buckets = {}  # measure-group uri (or None when ungrouped) -> [measure entries]
            group_entries = {}  # group_code -> (group_entry, [child_entries])
            ungrouped = []

            for code, label in concept_labels.items():
                node_code = uri_to_code(code)

                if node_code in dim_groups:
                    node_type = "DimensionGroup"
                elif node_code in schema_dims | geo_dims | time_dims:
                    node_type = "Dimension"
                else:
                    node_type = "Measure"

                score = node_scores.get(node_code, {}).get('score')
                if node_scores and score and score < self.node_score_threshold:
                    continue

                # Unlinked time/geo codes are covered by their group's level summary line; only the retriever/linker-tied
                # ones (e.g. the gemeente named in the question) and the codes of small levels stay literal
                if (node_scores and score is None and node_type == "Dimension"
                        and node_code in geo_dims | time_dims
                        and node_code not in small_level_codes
                        and dim_to_group.get(node_code) in level_summaries):
                    continue

                if node_type == "Measure":
                    # Measures render under their MeasureGroup heading; the (total) mark uses the
                    # table-scoped label, mirroring the measure-coherence validator: a total may
                    # not be combined with the measures of its own group
                    is_total = bool(re.match(TOTAL_PATTERN, str(label), flags=re.IGNORECASE))
                    measure_buckets.setdefault(msr_partition_of.get(node_code), []).append(
                        (node_code, node_type, label, score, None, is_total))
                    continue

                partition = dim_partitions.get(dim_partition_of.get(node_code), {})
                entry = (node_code, node_type, label, score, partition.get('label'), partition.get('total', False))
                if node_type == "DimensionGroup":
                    if node_code not in group_entries:
                        group_entries[node_code] = (entry, [])
                    else:
                        group_entries[node_code] = (entry, group_entries[node_code][1])
                elif node_code in dim_to_group:
                    parent = dim_to_group[node_code]
                    if parent not in group_entries:
                        group_entries[parent] = (None, [])
                    group_entries[parent][1].append(entry)
                else:
                    ungrouped.append(entry)

            # Sort each category: scored first (highest score on top), then unscored
            def _sort_key(e):
                return (e[3] is not None, e[3] or 0)

            def _best_member_key(entries: dict):
                scores = [e[3] for e in entries if e[3] is not None]
                return (len(scores) > 0, max(scores) if scores else 0)

            # Build ordered list with measures first. Ungrouped ones (typically the table-wide totals) on top,
            # then each measure group as a heading followed by its members, then dimension groups with their children,
            # then ungrouped dimensions.
            ordered_entries = sorted(measure_buckets.pop(None, []), key=_sort_key, reverse=True)
            for part_uri, entries in sorted(measure_buckets.items(), key=lambda kv: _best_member_key(kv[1]), reverse=True):
                partition = msr_partitions.get(part_uri, {})
                # (node_code, node_type, label, score, part_label, is_total)
                ordered_entries.append((None, "MeasureGroup", partition.get('label') or '',
                                        None, None, partition.get('total', False)))
                ordered_entries.extend(sorted(entries, key=_sort_key, reverse=True))

            # Sort groups by their best child score (or own score)
            def _group_sort_key(item):
                group_code, (group_entry, children) = item
                scores = [c[3] for c in children if c[3] is not None]
                if group_entry and group_entry[3] is not None:
                    scores.append(group_entry[3])
                return (len(scores) > 0, max(scores) if scores else 0)

            for group_code, (group_entry, children) in sorted(group_entries.items(),
                                                              key=_group_sort_key, reverse=True):
                if group_entry:
                    ordered_entries.append(group_entry)
                children.sort(key=_sort_key, reverse=True)
                ordered_entries.extend(children)

            ordered_entries.extend(sorted(ungrouped, key=_sort_key, reverse=True))

            nodes_info = []
            for node_code, node_type, label, score, part_label, is_total in ordered_entries[:max_nodes_per_table]:
                if node_type == "MeasureGroup":
                    # heading only: measure groups are not selectable codes, they organize the
                    #  measures below them into one aggregation-coherent partition
                    nodes_info.append(f"- MeasureGroup: \"{label}\""
                                      f"{' (group of total measures)' if is_total else ''}")
                    continue
                line = (f"- {node_type}: {node_code} "
                        f"(label: \"{label}\")"
                        f"{' (unit: ' + units[node_code] + ')' if node_code in units else ''}"
                        f"{' (partition: "' + part_label + '")' if part_label else ''}"
                        f"{' (total)' if is_total else ''}"
                        f"{' (relevance: ' + f'{score:.4f}' + ')' if score is not None else ''}")
                if node_type == "DimensionGroup" and node_code in level_summaries:
                    line += (f" (levels: "
                             f"{' | '.join(f'{tok}: {lvl} ({n})' for tok, lvl, n, _ in level_summaries[node_code])})")
                nodes_info.append(line)

            if nodes_info:
                table_info.append("Nodes:")
                table_info.extend(nodes_info)
            table_info.append("")

        table_info_str = chr(10).join(table_info)

        file_name = f"{config.LANGUAGE}_prompt_{query_type}.txt"
        prompt_path = os.path.join(os.path.dirname(__file__), file_name)
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompt_template = f.read()

        system_prompt = prompt_template.split('[SYSTEM]')[1].split('[USER]')[0].strip()
        user_prompt_template = prompt_template.split('[USER]')[1].strip()

        user_prompt = user_prompt_template.format(question=question, table_info=table_info_str)
        return LLMPrompt(system_prompt=system_prompt, user_prompt=user_prompt)

    def prepare_prompt(self,
                       question: str,
                       retrieved_tables: Optional[dict] = None,
                       *,
                       k: int = 5,
                       query_type: QueryType = 'sql') -> Optional[LLMPrompt]:
        """
            Retrieves node scores, links geo/time entities and builds the prompt for the given question.

            :param question: natural language question
            :param retrieved_tables: full retriever output dict with table scores and optional node scores.
            :param k: number of tables to retrieve for the LLM to process
            :param query_type: type of query to generate (sexp, sql or pivot_sql)
            :returns: the system and user prompt, or None when no tables are available
        """
        if retrieved_tables is None:
            # End-to-end: no tables supplied, so retrieve them (node scores included).
            retrieved_tables = self.retriever.retrieve_tables(question, k=k)
        elif not any(data.get('nodes') for data in retrieved_tables.values()):
            # Tables supplied without node scores (e.g. golden tables in the 'query-only' evaluation task are passed
            # as bare ids). Fetch node scores for exactly these tables. The retriever should search to a fixed depth
            # regardless of k, so a large k only widens the returned table tail; the k=1_000 here ensures the
            # (arbitrarily ranked) golden tables fall within that returned set and thus keep their node scores.
            table_node_scores = self.retriever.retrieve_tables(question, k=max(k, 1_000))
            retrieved_tables = {t: table_node_scores.get(t, {}) for t in retrieved_tables}

        if not retrieved_tables:
            return None

        if self.enable_linking:
            # Surface question-named regions/periods ('Amsterdam', 'eerste kwartaal 2023') as
            # scored nodes: the semantic retrievers do not index time/geo codes, and the
            # compacted prompt only lists linked codes literally.
            retrieved_tables = self._get_linker().enrich(question, retrieved_tables)

        return self._build_prompt(question, retrieved_tables, query_type=query_type)

    def generate_query(self,
                       question: str,
                       retrieved_tables: Optional[dict] = None,
                       *,
                       k: int = 5,
                       query_type: QueryType = 'sql',
                       llm_prompt: Optional[LLMPrompt] = None,
                       remarks: Optional[List[Tuple[str, str]]] = None) -> LLMResponse:
        """
            Generates a query for the given question.

            :param question: natural language question
            :param retrieved_tables: full retriever output dict with table scores and optional node scores.
            :param k: number of tables to retrieve for the LLM to process
            :param query_type: type of query to generate (sexp, sql or pivot_sql)
            :param llm_prompt: prompt from `prepare_prompt`; when given, k/retrieved_tables/query_type are ignored
                               and the prompt is not rebuilt
            :param remarks: optional list of (previous_response_json, error_message) tuples from failed attempts,
                            appended to the conversation so the model can correct itself.
            :returns: tuple of (parsed JSON response dict, token counts).
                      The dict has keys: query (str), status (OK|ALTERNATIVE|UNANSWERABLE), remarks (str).
        """
        if llm_prompt is None:
            llm_prompt = self.prepare_prompt(question, k=k, retrieved_tables=retrieved_tables, query_type=query_type)
        if llm_prompt is None:
            return LLMResponse(query="", input_token_count=0, output_token_count=0,
                               status=LLMAnswerStatus.UNANSWERABLE,
                               remarks="Geen tabellen beschikbaar.")

        # Retries with the execution error (production only; the benchmark generates a single attempt)
        messages = [{"role": "user", "content": llm_prompt.user_prompt}]
        query_name = {'sexp': ('S-expressie', 'S-expression')}.get(query_type, ('SQL-query', 'SQL query'))
        for previous_response_json, error_message in (remarks or []):
            messages.append({"role": "assistant", "content": previous_response_json})
            messages.append({"role": "user", "content":
                (
                    f"De query die je genereerde gaf de volgende fout bij uitvoering:\n{error_message}\n\n"
                    f"Genereer een gecorrigeerd JSON-antwoord met een verbeterde {query_name[0]}. "
                    f"Gebruik hetzelfde JSON-formaat."
                ) if config.LANGUAGE == 'nl' else (
                    f"The query that you generated gave the following exception:\n{error_message}\n\n"
                    f"Generate a JSON answer with a corrected {query_name[1]} if possible. "
                    f"Use the same JSON format."
                )
            })

        raw_response, num_tokens = self._call_llm(llm_prompt.system_prompt, messages)
        parsed = self._parse_response(raw_response, token_counts=num_tokens)
        return parsed

    @abstractmethod
    def _call_llm(self, system_prompt: str, messages: list) -> Tuple[str, Tuple[int, int]]:
        """
            Calls the LLM endpoint and returns the generated query.

            :param system_prompt: system prompt string
            :param messages: list of message dicts, e.g. [{"role": "user", "content": "..."}]
        """
        raise NotImplementedError()

    @staticmethod
    def _extract_response(text: str) -> str:
        """
            Return the JSON envelope ({"query", "status", "remarks"}) that _parse_response loads.
            Pull the fenced block if present, then the outermost {...} object. Falls back to the
            stripped text (e.g. a bare S-expression or SQL query) when no JSON object is found.
        """
        text = text.strip()
        if not text:
            return ""
        # A fenced block anywhere in the response (```json ... ``` or ``` ... ```).
        fence = re.search(r"```(?:\w+)?\s*(.*?)```", text, re.DOTALL)
        if fence:
            text = fence.group(1).strip()
        # The JSON envelope, ignoring any surrounding prose. Only when it parses: a bare SQL query can contain
        # braces itself, e.g. the quantifiers in regexp_matches(Perioden, '^\d{4}KW\d{2}$').
        obj = re.search(r"\{.*\}", text, re.DOTALL)
        if obj:
            try:
                json.loads(obj.group(0))
                return obj.group(0).strip()
            except json.JSONDecodeError:
                pass
        return text

    @staticmethod
    def _parse_response(raw: str, token_counts: Tuple[int, int]) -> LLMResponse:
        """Parse the LLM's JSON response into a structured dict."""
        try:
            parsed = json.loads(raw)
            return LLMResponse(
                query=parsed.get("query", ""),
                input_token_count=token_counts[0],
                output_token_count=token_counts[1],
                status=LLMAnswerStatus(parsed.get("status", "OK")),
                remarks=parsed.get("remarks", "")
            )
        except (json.JSONDecodeError, AttributeError, ValueError, KeyError):
            # Fallback: if the model returned a raw S-expression or SQL query instead of JSON
            raw = raw.strip()
            if raw.startswith("(") or re.match(r"(SELECT|WITH)\b", raw, flags=re.IGNORECASE):
                return LLMResponse(
                    query=raw,
                    input_token_count=token_counts[0],
                    output_token_count=token_counts[1],
                    status=LLMAnswerStatus.OK, remarks=""
                )
            # The tokens were spent even though the answer is unusable
            return LLMResponse(
                query="",
                input_token_count=token_counts[0], output_token_count=token_counts[1],
                status=LLMAnswerStatus.UNANSWERABLE,
                remarks=f"Er is iets misgegaan met het verwerken van het antwoord.")
