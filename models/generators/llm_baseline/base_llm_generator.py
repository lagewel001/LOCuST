import os
from abc import ABC
from itertools import groupby
from operator import itemgetter
from rdflib import Literal
from rdflib.namespace import SKOS, DCTERMS as DCT, QB, RDF, XSD
from typing import Tuple, Optional, List

import config
from models.generators.base_generator import BaseGenerator
from models.generators.geo_dim_extractor import match_region
from models.generators.time_dim_extractor import extract_tc
from models.retrievers.colbert.colbert_retriever import ColBERTRetriever
from odata_graph import engine
from odata_graph.sparql_controller import QUDT
from s_expression import Table, uri_to_code


class BaseLLMGenerator(BaseGenerator, ABC):
    """A baseline model for generating SQL queries using a LLM."""
    retriever: ColBERTRetriever

    @staticmethod
    def _build_prompt(question: str, tables: dict, query_type: str = 'sql') -> Tuple[str, str]:
        """
            Builds a prompt for the LLM based on the question and retrieved tables.

            :param question: question string
            :param tables: dictionary of ranked tables and their nodes
        """
        table_info = []
        for table_id, data in tables.items():
            # Get graph elements for tables
            table_node = Table(table_id)
            table_graph = engine.get_table_graph(table_node, include_time_geo_dims=True)

            # Separate the dimension types into buckets for rule-based extraction of geo and time dims from the query
            dim_groups = {uri_to_code(s) for s in set(table_graph.objects(predicate=QB.dimension)) -
                          set(table_graph.subjects(predicate=SKOS.broader))}
            geo_dims = {uri_to_code(s) for s in
                table_graph.subjects(predicate=RDF.type, object=Literal("GeoDimension", datatype=XSD.string if config.LOCAL_GRAPH else None))
            } - dim_groups
            time_dims = {uri_to_code(s) for s in
                table_graph.subjects(predicate=RDF.type, object=Literal("TimeDimension", datatype=XSD.string if config.LOCAL_GRAPH else None))
            } - dim_groups
            schema_dims = {uri_to_code(s) for s in table_graph.objects(predicate=QB.dimension)} - dim_groups - geo_dims - time_dims

            geo_constraints = match_region(query=question, available_geo_constraints=geo_dims)
            time_constraints = extract_tc(query=question, available_time_constraints=time_dims)

            units = dict(map(lambda x: (uri_to_code(x[0]), x[1].split('/')[-1]), table_graph.subject_objects(QUDT.unit)))
            concepts = set(map(lambda x: (x[1], x[0]), table_graph.subject_objects(QB.concept)))
            labels = set(table_graph.subject_objects(SKOS.prefLabel))

            # Inner join concepts with labels
            def inner_join(a, b):
                L = a + b
                L.sort(key=itemgetter(0))  # sort by the first column
                for _, group in groupby(L, itemgetter(0)):
                    row_a, row_b = next(group), next(group, None)
                    if row_b is not None:  # join
                        yield row_a[1:] + row_b[1:]  # cut 1st column from 2nd row

            concept_labels = dict(inner_join(list(concepts), list(labels)))

            # Setup prompt
            table_labels = set(table_graph.objects(table_node.uri, DCT.title) or {''})
            if len(table_labels) == 0:
                table_labels = {''}
            table_info.append(f"Table: {table_id} "
                              f"(label: \"{str(table_labels.pop())}\") "
                              f"(score: {data.get('score', '-')})")

            nodes_info = []
            for code, label in concept_labels.items():
                node_code = uri_to_code(code)
                # Skip time and geo dimensions which are not extracted from the question
                if node_code in (geo_dims | time_dims) - (geo_constraints | time_constraints):
                    continue

                if node_code in dim_groups:
                    node_type = "DimensionGroup"
                elif node_code in schema_dims | geo_dims | time_dims:
                    node_type = "Dimension"
                else:
                    node_type = "Measure"

                nodes_info.append(f"- {node_type}: {node_code} "
                                  f"(label: \"{label}\") "
                                  f"{' (unit: ' + units[node_code] + ')' if node_code in units else ''}")
                if len(nodes_info) == 50:  # Don't make list too long
                    break

            if nodes_info:
                table_info.append("Nodes:")
                table_info.extend(nodes_info)
            table_info.append("")

        table_info_str = chr(10).join(table_info)

        file_name = f"{config.LANGUAGE}_prompt{'_simplified' if query_type == 'simplified_sql' else ''}.txt"
        prompt_path = os.path.join(os.path.dirname(__file__), file_name)
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompt_template = f.read()

        system_prompt = prompt_template.split('[SYSTEM]')[1].split('[USER]')[0].strip()
        user_prompt_template = prompt_template.split('[USER]')[1].strip()

        user_prompt = user_prompt_template.format(question=question, table_info=table_info_str)
        return system_prompt, user_prompt

    def _call_llm(self, system_prompt: str, user_prompt: str) -> Tuple[str, Tuple[int, int]]:
        """Calls the Azure ML endpoint and returns the generated SQL query."""
        raise NotImplementedError()

    def generate_query(self, question: str, k: int = 5, golden_tables: Optional[List[str]] = None, query_type: str = 'sql') -> Tuple[str, Tuple[int, int]]:
        """Generates a SQL query for the given question."""
        if golden_tables is None:
            retrieved_tables = self.retriever.retrieve_tables(question, k=k)
        else:
            retrieved_tables = {t: {} for t in golden_tables}

        if not retrieved_tables:
            return "", (0, 0)  # Cannot generate a query

        system_prompt, user_prompt = self._build_prompt(question, retrieved_tables, query_type=query_type)
        sql_query, num_tokens = self._call_llm(system_prompt, user_prompt)
        return sql_query, num_tokens
