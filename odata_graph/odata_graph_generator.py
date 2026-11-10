import copy
import json
import math
import os.path
import numpy as np
import pandas as pd
import re
import requests
import textwrap
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from rdflib import Graph, RDF, SKOS, DCTERMS as DCT, DCAT, QB, XSD, Namespace
from rdflib.term import _is_valid_uri, URIRef
from tqdm import tqdm
from traceback import print_exception
from typing import Optional, Tuple, List

import config
from odata_graph import engine
from odata_graph.sparql_controller import SparqlEngine, QUDT, XKOS, GECKO, TOTAL_PATTERN
from s_expression.schema_item import Table, Measure, Dimension
from odata_graph.ttl_to_py.freq_convertor import FREQ_DICT
from odata_graph.ttl_to_py.unit_convertor import UNIT_DICT
from utils.custom_types import GraphEngineKind

logger = config.logging.getLogger(__name__)

FAILED_TABLE_PATH = re.sub(r"\.(nt|trig)", "",
                           f"./odata_graph/graphs/failed_tables_{config.GRAPH_FILE.split('/')[-1]}_"
                           f"{datetime.now().strftime("%d%m%Y%H%M%S")}.txt")

# Shared HTTP session: reuses TCP + TLS across the many OData calls per table.
_SESSION = requests.Session()

_FLUSH_TRIPLES_EVERY = 500
# Hard cap on the triple lines buffered before a flush. Virtuoso rejects an INSERT whose
# *generated SQL* exceeds 10k lines (error SP031), and each N-Triple expands to several lines of
# that SQL, so the triple budget must sit well under 10k, not merely below it.
_MAX_INSERT_LINES = 2_000
_LOCAL_SERIALIZE_EVERY = 600  # Save progress every N tables. Know that serialization takes a long time.
_TABLE_WORKERS = min(4, (os.process_cpu_count() - 2) or 1)  # always keep at least 2 vcores available for the API

# Concept (prefLabel+definition dedup) nodes are minted under this namespace. Keeping it distinct
# from the dimensie/onderwerp namespaces lets concept lookups be scoped by URI prefix even on
# single-graph backends (Virtuoso), where the concepts/dimensies named-graph split collapses into
# one graph and can no longer isolate label concepts from the dimension-group concept nodes.
_CONCEPT_NS = "https://vocabs.cbs.nl/def/concept/"

# Static ontologies (TBox) bootstrapped into the graph on generator start-up when absent
_ONTOLOGIES = (
    ('gecko',                                                   # sub-graph name
     f"{config.PATH_ROOT}/odata_graph/ontology/gecko.ttl",      # ontology ttl file
     "https://vocabs.cbs.nl/def/gecko#Aggregate"),              # maker URI for checking if the ontology is loaded correctly
    ('qudt',
     f"{config.PATH_ROOT}/odata_graph/ontology/QUDT-3.4.0-SHACL.ttl",
     "http://qudt.org/schema/qudt/Unit"),
)
_ONTOLOGY_INSERT_CHUNK = 10_000  # triples per INSERT DATA request when loading over HTTP

_AGGREGATE_PATTERN = r".*\b(totaal|waarde|totale|total)\b.*" # Regex pattern for finding gecko:Aggregate measures/dimensions


def _nt_literal(value: str, lang: Optional[str] = None) -> str:
    """
        Serialize a literal as strict N-Triples. Not the same as rdflib's Literal.n3(): n3()
        emits multiline strings as triple-quoted (Turtle) literals with raw newlines, which the
        SPARQL parser accepted but the N-Triples fast path (insert_raw) rejects. CBS table
        descriptions contain bare \\n, so newlines, quotes and backslashes must be escaped per
        the N-Triples ECHAR production.
    """
    value = (str(value).replace('\\', '\\\\').replace('"', '\\"')
             .replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t'))
    return f'"{value}"@{lang}' if lang else f'"{value}"'


def uid(code: str):
    """
        Normalize and validate OData4 codes to ensure they can be passed as valid URI's

        :param code: OData4 code to validate
        :returns: false if code would yield an invalid URI, the normalized code otherwise
    """
    code = code.replace(' ', '').strip()
    invalid_uri = not code or not _is_valid_uri(code)
    if invalid_uri:
        raise ValueError(f"Invalid identifier for URI given: {code}")
    return code


class TableGraphGenerator(object):
    """
        Class for generating RDF graphs from OData tables. Scrapes metadata from tables in the OData API
        and saves the metadata in a local or online graph. The scraper looks at the latest modified entries
        for tables to determine what tables to scrape from OData.

        The order of operations is as follows:
          0. Fetch a list of table IDs that need to be updated in the graph, and get their metadata
             props from OData (check_modified_tables)
          1. Create # _TABLE_WORKERS workers to process each table (insert_tables)
          2. Create an insert query for table triples (_process_table)
          3. Create an insert query for dimension triples (extract_dimensions)
            3a. Craete skos:Concepts for all title-difinition pair found on dimensions. Reuse existing pairs.
            3b. Flush all # _FLUSH_TRIPLES_EVERY dimensions to the graph
          4. Create an insert query for measure triples (extract_measures)
            4a. Craete skos:Concepts for all title-difinition pair found on dimensions. Reuse existing pairs.
          5. Flush all table and measure triples to the graph
          6. If building a local graph, serialize the graph every # _LOCAL_SERIALIZE_EVERY tabes.
    """
    tables: pd.DataFrame

    dimensions: pd.DataFrame
    dimension_codes: pd.DataFrame
    timedim_codes: pd.DataFrame
    geodim_codes: pd.DataFrame

    measure_groups: pd.DataFrame
    measure_codes: pd.DataFrame

    def __init__(self, engine: SparqlEngine):
        self.engine = engine
        if not os.path.exists(config.GRAPH_FILE) and config.GRAPH_ENGINE == GraphEngineKind.LOCAL:
            # Make empty file (although the LocalEngine should already have thrown an error at this point)
            open(config.GRAPH_FILE, 'w').close()

        self.failed_tables = set()

        # Concept cache: (preflabel, definition_or_None) -> concept_uri. Populated
        # lazily on first use, then maintained in-memory to avoid the per-concept
        # SELECT + COUNT(*) round-trips.
        self._concepts: Optional[dict] = None
        self._concept_count: int = 0
        self._concept_lock = threading.Lock()

        # SPARQLWrapper isn't thread-safe (single self.sparql with setQuery state),
        # so engine calls are serialized. rdflib graph writes are also not reentrant.
        self._engine_lock = threading.Lock()
        self._graph_lock = threading.Lock()
        self._failed_lock = threading.Lock()

        self.ensure_ontologies()
        self._load_dimension_type_maps()

    def _load_dimension_type_maps(self):
        """
            Build the geo prefix-to-class and time infix-to-class lookup maps by querying the
            gecko TBox (guaranteed present by ensure_ontologies). Ontology-driven on purpose:
            adding a new area/period class to gecko.ttl extends the generator without code changes.
        """
        rows = self.engine.select(textwrap.dedent(f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX gecko: <{GECKO}>
            SELECT ?cls ?prefix ?infix WHERE {{
                {{ ?cls rdfs:subClassOf gecko:GeoDimension ; gecko:codePrefix ?prefix }}
                UNION
                {{ ?cls rdfs:subClassOf gecko:TimeDimension ; gecko:codeInfix ?infix }}
            }}
        """))
        self._geo_prefix_classes = {r['prefix']['value']: r['cls']['value'] for r in rows if 'prefix' in r}
        self._time_infix_classes = {r['infix']['value']: r['cls']['value'] for r in rows if 'infix' in r}
        # Match longest markers first so 3-character prefixes (G04/G40) can never lose to a
        #  shorter overlapping one, and 2-letter infixes win over the 1-letter 'W'.
        self._geo_prefixes_desc = sorted(self._geo_prefix_classes, key=len, reverse=True)
        self._time_infixes_desc = sorted(self._time_infix_classes, key=len, reverse=True)

    def classify_geo_code(self, code: str) -> str:
        """
            Resolve a geographic code to its gecko area class by its prefix (e.g. GM0363 ->
            gecko:Municipality); unrecognized prefixes land on the gecko:UnknownRegion fallback.
        """
        for prefix in self._geo_prefixes_desc:
            if code.startswith(prefix):
                return self._geo_prefix_classes[prefix]
        return str(GECKO.UnknownRegion)

    def classify_time_code(self, code: str) -> Optional[str]:
        """
            Resolve a period code to its gecko granularity class. CBS period codes follow
            <four digits: year><infix><two digits> (e.g. 2022JJ00 -> gecko:Year); a digits-only
            code like 19950101 is a calendar date. Returns None for unrecognized shapes.
        """
        if re.fullmatch(r'\d{8}', code):
            return str(GECKO.Date)
        for infix in self._time_infixes_desc:
            if (code[4:4 + len(infix)] == infix and code[:4].isdigit()
                    and code[4 + len(infix):].isdigit() and len(code) > 4 + len(infix)):
                return self._time_infix_classes[infix]
        return None

    def ensure_ontologies(self):
        """
            Bootstrap the static TBox ontologies (gecko, QUDT) into the graph when they are not
            present yet. Presence is probed with a single-triple SELECT on a marker subject, so
            an already-populated graph pays one round-trip per ontology and nothing more.
        """
        for graph_name, ttl_path, marker in _ONTOLOGIES:
            if self.engine.select(f"SELECT ?p WHERE {{ <{marker}> ?p ?o }} LIMIT 1"):
                continue  # ontology already loaded

            if not os.path.exists(ttl_path):
                raise FileNotFoundError(f"Ontology <{marker}> is missing from the graph and its source "
                                        f"file {ttl_path} does not exist.")

            logger.info(f"Loading ontology {os.path.basename(ttl_path)} into the '{graph_name}' graph...")
            if self.engine.local:
                with self._graph_lock:
                    self.engine.graph.get_context(URIRef(self.engine.graph_for(graph_name))).parse(ttl_path, format='turtle')
            else:
                # Skolemize first: blank-node labels don't survive chunked INSERT DATA requests
                # (each request would mint fresh bnodes, severing e.g. owl:unionOf lists).
                onto = Graph()
                onto.parse(ttl_path, format='turtle')
                triples = [t for t in onto.skolemize().serialize(format='nt').splitlines() if t.strip()]
                for i in range(0, len(triples), _ONTOLOGY_INSERT_CHUNK):
                    self._run_insert({graph_name: '\n'.join(triples[i:i + _ONTOLOGY_INSERT_CHUNK])}, URIRef(marker))

            logger.info(f"Ontology {os.path.basename(ttl_path)} loaded.")

    @staticmethod
    def _fetch_all_odata(url: str) -> List[dict]:
        """Fetch an OData collection, following @odata.nextLink pagination."""
        rows = []
        while url:
            payload = _SESSION.get(url).json()
            rows += payload['value']
            url = payload.get('@odata.nextLink')
        return rows

    def ensure_geo_containment(self):
        """
            Bootstrap the global geographic containment layer: xkos:isPartOf/hasPart edges
            between area codes (municipality -> province / COROP / functional regions,
            province -> landsdeel -> country). Geo containment is global and per-code immutable
            (municipal reorganizations mint NEW codes), so it is loaded once as reference data
            rather than scraped per table, as the per-table <Dimension>Groups does not encode it
            reliably (they are organizational level-groupings).

            Sources:
              - 70739ned 'Gebieden; overzicht vanaf 1830': the code -> province chain for every
                area code ever minted, defunct codes included (province given by NAME, mapped to
                its PV code via the table's own RegioSCodes);
              - the yearly 'Gebieden in Nederland' tables (discovered from the catalog; only the
                OData4 era, 2013+, exposes Observations): the full classification matrix per
                municipality (province, landsdeel, COROP, arbeidsmarktregio, GGD-regio, ...).

            Presence is probed with a single SELECT (any code isPartOf a province), so an
            already-populated graph pays one round-trip. Called from the regen entrypoint, not
            __init__, as the first load performs a few dozen HTTP calls.
        """
        if self.engine.select(f"""
            SELECT ?s WHERE {{
                ?s <{XKOS.isPartOf}> ?pv .
                FILTER (STRSTARTS(STR(?pv), "{Dimension.rdf_ns}PV"))
            }} LIMIT 1
        """):
            return  # containment layer already loaded

        logger.info("Loading the geographic containment layer (Gebieden in Nederland)...")
        base = "https://datasets.cbs.nl/odata/v1/CBS/"
        edges: set = set()

        # Fetch historical admin chain: every area code ever, incl. defunct municipalities
        try:
            pv_by_name = {re.sub(r'\s*\(PV\)$', '', c['Title']).strip(): c['Identifier'].strip()
                          for c in self._fetch_all_odata(f"{base}70739ned/RegioSCodes?$filter=startswith(Identifier,'PV')")}
            for row in self._fetch_all_odata(f"{base}70739ned/Observations?$filter=Measure eq 'PV0000_1'"):
                code = (row.get('RegioS') or '').strip()
                province = pv_by_name.get((row.get('StringValue') or '').strip())
                if code and province and code != province:
                    edges.add((code, province))
        except Exception as e:
            logger.warning(f"Skipping the historical containment source 70739ned: {e}")

        # Fetch yearly classification matrices (functional regions + landsdeel chain)
        catalog = self._fetch_all_odata(f"{base}Datasets?$filter=startswith(Title,'Gebieden in Nederland')")
        for tid in sorted(t['Identifier'] for t in catalog):
            try:
                if 'Observations' not in {v['name'] for v in _SESSION.get(f"{base}{tid}").json()['value']}:
                    continue  # pre-2013 tables are metadata-only stubs in OData4
                code_measures = {m['Identifier'] for m in self._fetch_all_odata(f"{base}{tid}/MeasureCodes")
                                 if (m.get('Title') or '').strip() == 'Code'}
                parents_of: dict = {}  # municipality -> {code prefix: parent code}
                for row in self._fetch_all_odata(f"{base}{tid}/Observations"):
                    if row['Measure'] not in code_measures:
                        continue
                    subj, obj = (row.get('RegioS') or '').strip(), (row.get('StringValue') or '').strip()
                    # != skips the municipality's own-code measure; the prefix guard drops the bare
                    # numeric ids some older year-tables emit for a few classifications
                    if subj and obj and subj != obj and re.match(r'^[A-Z]', obj):
                        edges.add((subj, obj))
                        parents_of.setdefault(subj, {})[obj[:2]] = obj
                for parents in parents_of.values():  # derive the chain above the municipalities
                    if 'PV' in parents and 'LD' in parents:
                        edges.add((parents['PV'], parents['LD']))
                        edges.add((parents['LD'], 'NL01'))
            except Exception as e:
                logger.warning(f"Skipping Gebieden table {tid}: {e}")

        chunk_edges = _ONTOLOGY_INSERT_CHUNK // 2
        triples = ""
        for i, (subj, obj) in enumerate(sorted(edges)):
            try:
                subj_uri, obj_uri = Dimension.rdf_ns.term(uid(subj)), Dimension.rdf_ns.term(uid(obj))
            except ValueError:
                continue  # malformed code in the source data
            triples += f"<{subj_uri}> <{XKOS.isPartOf}> <{obj_uri}> .\n"
            triples += f"<{obj_uri}> <{XKOS.hasPart}> <{subj_uri}> .\n"

            if (i + 1) % chunk_edges == 0:
                self._run_insert({'dimensies': triples}, Dimension.rdf_ns.term('NL01'))
                triples = ""

        self._run_insert({'dimensies': triples}, Dimension.rdf_ns.term('NL01'))
        logger.info(f"Geographic containment layer loaded ({len(edges)} containment edges).")

    def check_modified_tables(self, fetch_table_ids: Optional[List[str]] = None):
        """
            Get tables already in the graph and exclude them if they have not been modified since their last update.
            By default (no fetch_table_ids given), only the 'Regular' (live/active) tables in OData3/4 are retrieved.

            :param fetch_table_ids: Pre-determined list of table IDs to check
        """
        modified_query = textwrap.dedent(f"""
            PREFIX dct: <http://purl.org/dc/terms/>
            SELECT ?id ?modified
            FROM <{self.engine.graph_for('datasets')}>
            WHERE {{
                ?tab dct:identifier ?id .
                ?tab dct:modified ?modified .
            }}
        """)
        graph_data = self.engine.select(modified_query)
        existing_tables = {table['id']['value']: table['modified']['value'].replace(" +", "+") for table in graph_data}

        if config.LANGUAGE == 'en':
            pre_url = "https://opendata.cbs.nl/ODataCatalog/Tables"
            status_col = 'OutputStatus'
            status = 'Regular'
        else:
            pre_url = "https://datasets.cbs.nl/odata/v1/CBS/Datasets"
            status_col = 'Status'
            status = 'Regulier'

        datasets_url = f"{pre_url}?$filter=Language eq '{config.LANGUAGE}'&$format=json"

        r = _SESSION.get(datasets_url).json()
        table_modified = {t['Identifier']: datetime.fromisoformat(t['Modified']) for t in r['value'] if
                          fetch_table_ids is not None or t[status_col] == status}
        table_ids = []
        for table in table_modified if not fetch_table_ids else fetch_table_ids:
            if not table[0].isalpha() and table[0] != '9':  # Skip datasets that are not for the public
                if (table not in existing_tables or table_modified[table] > datetime.fromisoformat(
                        existing_tables[table])):
                    table_ids.append(table)
        self.tables = pd.DataFrame(
            columns=['Identifier', 'Title', 'Language', 'ReleaseDate', 'Modified', 'StartDate',
                     'EndDate', 'Temporal', 'Catalog', 'Description', 'AccrualPeriodicity'],
            dtype=str)
        self.tables['Identifier'] = table_ids

    def _populate_concept_cache(self):
        """Populate the in-memory concept cache on first use."""
        if self._concepts is not None:
            return

        cache_query = textwrap.dedent(f"""
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX qudt: <http://qudt.org/schema/qudt/>
            SELECT ?s ?label ?def ?unit
            FROM <{self.engine.graph_for('concepts')}>
            WHERE {{
                ?s a skos:Concept .
                ?s skos:prefLabel ?label .
                OPTIONAL {{ ?s skos:definition ?def }}
                OPTIONAL {{ ?s qudt:unitOfSystem ?unit }}
                # Restrict to genuine label concepts by URI namespace. On single-graph backends
                # (Virtuoso) the concepts and dimensies graphs merge, so without this filter the
                # dimension-group concept nodes (also `a skos:Concept` with a skos:prefLabel)
                # would pollute the cache and get reused as label concepts via qb:concept.
                FILTER(STRSTARTS(STR(?s), "{_CONCEPT_NS}"))
            }}
        """)
        with self._engine_lock:
            if self._concepts is not None:  # second cache check prevents race condition
                return
            result = self.engine.select(cache_query)

            concepts = {}
            max_minted_id = 0
            for row in result:
                label = row['label']['value']
                definition = row['def']['value'] if 'def' in row else None
                unit = row['unit']['value'] if 'unit' in row else None
                concepts[(label, definition, unit)] = row['s']['value']
                # Track the highest minted C-number: the counter must continue from there.
                suffix = row['s']['value'].rsplit('C', 1)[-1]
                if suffix.isdigit():
                    max_minted_id = max(max_minted_id, int(suffix))
            self._concept_count = max_minted_id
            self._concepts = concepts

    def add_concept(self, element_uri: str, table_uri: str, preflabel: str, definition: str, unit: str) -> Tuple[str, str]:
        """
            Check if a concept, uniquely defined by prefLabel+definition, is already in the
            graph and return its URI; otherwise mint the next URI and include its definition
            in the returned triples fragment. The fragment is meant to be placed inside a
            GRAPH <concepts> { ... } clause by the caller. This folds concept inserts into
            the surrounding batch instead of a dedicated INSERT per concept.

            :param element_uri: uri of the element that should be connected with a concept
            :param table_uri: all concepts should be connected to a specific table
            :param preflabel: skos:prefLabel of the concept
            :param definition: skos:definition of the concept
            :param unit: qudt:unitOfSystem of the concept
            :return: (concept_uri, triples_for_concepts_graph)
        """
        IDENTIFIER_LENGTH = 8  # NOTE: 8 digits is sufficient for 1.2K tables, but a more extensive graph might need more
        CONCEPT_BASE_URI = f"{_CONCEPT_NS}C"

        self._populate_concept_cache()

        preflabel = preflabel.replace('\\', '').replace('"', "'").strip()
        if definition:
            definition = definition.replace('\\', '').replace('"', "'").replace("\r\n", " ").replace("\t", " ").strip()
            definition = re.sub(r'\s+', ' ', definition)
        key = (preflabel, definition if definition else None, unit if unit else None)

        with self._concept_lock:
            # 1. check if concept already exists and get its URI
            concept_uri = self._concepts.get(key)
            is_new = concept_uri is None
            if is_new:
                if self._concept_count >= math.pow(10, IDENTIFIER_LENGTH):
                    raise KeyError(f"Ran out of concept URI identifiers! Increase the identifier length (current: {IDENTIFIER_LENGTH})")
                self._concept_count += 1
                count = self._concept_count
                concept_uri = f"{CONCEPT_BASE_URI}{'0' * max(0, IDENTIFIER_LENGTH - len(str(count)))}{count}"
                self._concepts[key] = concept_uri

        triples = ""
        if is_new:
            triples += f"<{concept_uri}> <{RDF.type}> <{SKOS.Concept}> .\n"
            triples += f"<{concept_uri}> <{SKOS.prefLabel}> {_nt_literal(preflabel, config.LANGUAGE)} .\n"
            if definition:
                triples += f"<{concept_uri}> <{SKOS.definition}> {_nt_literal(definition, config.LANGUAGE)} .\n"
        triples += f"<{element_uri}> <{QB.concept}> <{concept_uri}> .\n"
        triples += f"<{concept_uri}> <{DCT.isPartOf}> <{table_uri}> .\n"

        return concept_uri, triples

    @staticmethod
    def title_to_valid_uri_str(title: str) -> str:
        """Take a (usually OData3) title and translate it to a valid RDF URI."""
        identifier = title[0] + title.title()[1:].replace("-", "").replace("_", "")  # Camel case Title
        identifier = ''.join([c for c in identifier if c.isalpha()])  # Remove non alpha characters
        return identifier

    def create_triples(self, insert_query: str, elements: pd.Series, mapping: dict, uri: str,
                       concept_triples: list) -> str:
        """
            Loop through all the elements of a measure or dimension code and add
            them to the insert query with the correct triple.

            :param insert_query: string with the current insert query
            :param elements: dictionary with fields of a measure or dimension
            :param mapping: dictionary containing information of which triples need to be created
            :param uri: string with the URI of the measure or dimension
            :param concept_triples: mutable list onto which concept-graph triples are appended;
                                    the caller flushes these in a GRAPH <concepts> { ... } clause
            :return: insert_query string with the updated insert query
        """
        mapping = copy.deepcopy(mapping)
        total = bool(re.match(_AGGREGATE_PATTERN, elements['Title'], flags=re.IGNORECASE))
        if total:
            insert_query += f"<{uri}> <{RDF.type}> <{GECKO.Aggregate}> .\n"
        unit = elements.get('Unit', None)

        # For skos:prefLabel (Title) and skos:definition (Description) create and/or link a Concept
        inverted_map = {v: k for k, v in mapping.items()}
        if SKOS.prefLabel in inverted_map or SKOS.definition in inverted_map:
            preflabel = elements.get(inverted_map[SKOS.prefLabel], '').replace("\r\n", " ")
            definition = elements.get(inverted_map[SKOS.definition])
            if definition:
                definition = definition.replace('\\', '').replace('"', "'").replace("\r\n", " ").replace("\t", " ").strip()
                definition = re.sub(r'\s+', ' ', definition)
            concept_uri, concept_fragment = self.add_concept(uri, inverted_map[SKOS.Concept], preflabel, definition, unit)
            concept_triples.append(concept_fragment)
            if unit:
                concept_triples.append(f"<{concept_uri}> <{QUDT.unitOfSystem}> {_nt_literal(unit)} .\n")
            del mapping[inverted_map[SKOS.prefLabel]]
            del mapping[inverted_map[SKOS.definition]]
            del mapping[inverted_map[SKOS.Concept]]

        for e in elements.index:
            if e in mapping:
                if e in ['Description', 'Title']:
                    if elements[e]:
                        elements[e] = elements[e].replace("\r\n", " ")
                        insert_query += f"<{uri}> <{mapping[e]}> {_nt_literal(elements[e], config.LANGUAGE)} .\n"
                else:
                    insert_query += f"<{uri}> <{mapping[e]}> {_nt_literal(elements[e])} .\n"

        return insert_query

    def _run_insert(self, clauses: dict, log_uri: URIRef) -> None:
        """
            Flush raw N-Triples fragments ({logical sub-graph name: triples string}) to the
            graph via SparqlEngine.insert_raw. The local engine parses the fragments straight
            into the store for significant wall time reduction.

            :param clauses: {logical sub-graph name (e.g. 'dimensies', 'onderwerpen'): raw
                             N-Triples string to insert into it}
            :param log_uri: uri identifier for this flush in the logs/error messages only.
                            This is not part of any query and technically need not be a subject actually being inserted.
        """
        with self._engine_lock:
            self.engine.insert_raw(clauses, log_uri)

    @staticmethod
    def _is_root_group(parent_id: str) -> bool:
        """A group is the root of its hierarchy when it has no parent (OData ParentId is null)."""
        return pd.isna(parent_id) or str(parent_id).strip().lower() in ('', 'none', 'nan')

    @staticmethod
    def _clean_pd_string_cell(row: pd.Series, column: str) -> str:
        """A row cell as a clean string: nulls/NaN become '', newlines are flattened."""
        value = row.get(column)
        if value is None or pd.isna(value):
            return ''
        return str(value).replace("\r\n", " ").strip()

    @staticmethod
    def _build_group_scheme(groups: pd.DataFrame, anchor_uri: URIRef, table_uri: URIRef,
                            ns: Optional[Namespace] = None, scheme_id: Optional[str] = None,
                            scheme_title: Optional[str] = None,
                            aggregate_pattern: str = _AGGREGATE_PATTERN) -> Tuple[str, Optional[URIRef], dict]:
        """
            Turn an OData group hierarchy (columns Id, Title, ParentId, optionally Description)
            into a table-scoped SKOS ConceptScheme, so aggregations can enforce partition
            coherence. Works for dimension groups (<Dimension>Groups/CategoryGroups, anchored on
            the dimension node) and measure groups (MeasureGroups/TopicGroups, anchored on the
            table node in the onderwerp namespace).

            The root group (ParentId is null) plays the role of the scheme and is linked from the
            anchor via qb:codeList; the remaining groups become skos:Concepts inside it, wired to
            their parent through skos:broader/topConceptOf and carrying gecko:Aggregate when their
            title matches the aggregate pattern. When the source has no wrapper root but several
            facet roots (common in OData3 CategoryGroups and in MeasureGroups), a synthetic root
            titled `scheme_title` is added and the facet roots are re-pointed to it, so they
            become top concepts of the scheme without partitive (XKOS) edges. Facets partition
            the whole independently and do not aggregate into each other. Group ids are only
            unique within their table, so the scheme and group nodes are namespaced by the table.

            :param groups: the group hierarchy (columns Id, Title, ParentId, optionally Description)
            :param anchor_uri: node the scheme is linked from via qb:codeList (dimension or table)
            :param table_uri: URI of the table the scheme is scoped to
            :param ns: namespace to mint the scheme/group nodes in (default: Dimension.rdf_ns)
            :param scheme_id: id of the scheme node within the table scope (default: derived from
                              the anchor URI's last path segment, i.e. the dimension id)
            :param scheme_title: skos:prefLabel for the scheme when a synthetic root is needed
            :param aggregate_pattern: regex marking group titles that denote a total
            :returns: (triples, scheme_uri, {group_id: node_uri}); scheme_uri is None when the
                      groups response has no identifiable root group.
        """
        ns = ns if ns is not None else Dimension.rdf_ns
        table_id = str(table_uri).rstrip('/').rsplit('/', 1)[-1]
        scheme_id = scheme_id or str(anchor_uri).rstrip('/').rsplit('/', 1)[-1]

        root_ids = [str(g['Id'].replace(' ', '')) for _, g in groups.iterrows()
                    if TableGraphGenerator._is_root_group(g.get('ParentId'))]
        if not root_ids:
            return "", None, {}
        if len(root_ids) > 1:
            groups = groups.copy()
            groups.loc[groups['Id'].astype(str).isin(root_ids), 'ParentId'] = '__root__'
            groups = pd.concat([pd.DataFrame([{'Id': '__root__', 'Title': scheme_title or '',
                                               'ParentId': None}]), groups], ignore_index=True)
            root_id = '__root__'  # cannot collide: real group ids are numeric or measure-code-like
        else:
            root_id = root_ids[0]

        # The root group is the scheme; other groups are table-scoped concept nodes under it
        scheme_uri = ns.term(f"{table_id}/{scheme_id}")
        group_uri_by_id = {
            str(g['Id'].replace(' ','')): (scheme_uri if str(g['Id'].replace(' ', '')) == root_id else ns.term(f"{table_id}/g{g['Id'].replace(' ','')}"))
            for _, g in groups.iterrows()
        }

        triples = f"<{scheme_uri}> <{RDF.type}> <{SKOS.ConceptScheme}> .\n"
        triples += f"<{scheme_uri}> <{DCT.isPartOf}> <{table_uri}> .\n"
        triples += f"<{anchor_uri}> <{QB.codeList}> <{scheme_uri}> .\n"

        for _, group in groups.iterrows():
            gid = str(group['Id'].replace(' ',''))
            title = TableGraphGenerator._clean_pd_string_cell(group, 'Title')
            definition = TableGraphGenerator._clean_pd_string_cell(group, 'Description')
            node_uri = scheme_uri if gid == root_id else group_uri_by_id[gid]
            if title:
                triples += f"<{node_uri}> <{SKOS.prefLabel}> {_nt_literal(title, config.LANGUAGE)} .\n"
            if definition:
                triples += f"<{node_uri}> <{SKOS.definition}> {_nt_literal(definition, config.LANGUAGE)} .\n"
            if gid == root_id:
                continue

            triples += f"<{node_uri}> <{RDF.type}> <{SKOS.Concept}> .\n"
            triples += f"<{node_uri}> <{SKOS.inScheme}> <{scheme_uri}> .\n"
            triples += f"<{node_uri}> <{DCT.isPartOf}> <{table_uri}> .\n"
            if re.match(aggregate_pattern, title, flags=re.IGNORECASE):
                triples += f"<{node_uri}> <{RDF.type}> <{GECKO.Aggregate}> .\n"

            parent_uri = group_uri_by_id.get(str(group.get('ParentId')), scheme_uri)
            if parent_uri == scheme_uri:  # direct child of the root becomes a top concept
                # Top-level facets partition the whole independently and do not aggregate *into* the
                # scheme, so they get the plain SKOS structural link, not a partitive (XKOS) one.
                triples += f"<{node_uri}> <{SKOS.topConceptOf}> <{scheme_uri}> .\n"
                triples += f"<{scheme_uri}> <{SKOS.hasTopConcept}> <{node_uri}> .\n"
            else:
                triples += f"<{node_uri}> <{SKOS.broader}> <{parent_uri}> .\n"
                triples += f"<{parent_uri}> <{SKOS.narrower}> <{node_uri}> .\n"
                # A nested sub-group aggregates into its parent group: mark the relation partitive.
                # xkos:hasPart/isPartOf are sub-properties of skos:narrower/broader (kept above so
                # traversal works without a reasoner).
                triples += f"<{node_uri}> <{XKOS.isPartOf}> <{parent_uri}> .\n"
                triples += f"<{parent_uri}> <{XKOS.hasPart}> <{node_uri}> .\n"

        return triples, scheme_uri, group_uri_by_id

    @staticmethod
    def _normalize_odata3_groups(groups: List[dict]) -> pd.DataFrame:
        """
            Reshape one dimension's slice of the OData3 CategoryGroups response into the column
            shape consumed by _build_group_scheme. Ids are stringified straight from the raw
            JSON: OData3 emits them as ints (OData4 as strings), and the nulls in ParentID would
            coerce a dtype=str DataFrame column to float ('4.0' != '4').

            :param groups: raw CategoryGroups rows of a single DimensionKey (columns ID/Title/ParentID)
            :returns: DataFrame in the <Dimension>Groups shape (columns Id, Title, Description, ParentId)
        """
        return pd.DataFrame([{'Id': str(g['ID']),
                              'Title': g.get('Title') or '',
                              'Description': g.get('Description') or '',
                              'ParentId': None if g.get('ParentID') is None else str(g['ParentID'])}
                             for g in groups], dtype=str)

    def insert_dimensions(self,
                          dimensions: pd.DataFrame,
                          table_uri: URIRef,
                          group_endpoints: frozenset = frozenset()):
        """
            Loops through all the dimensions and dimension codes and inserts them into the graph in batches.

            :param dimensions: dictionary containing information of dimensions
            :param table_uri: string with the URI of the table
            :param group_endpoints: names of the table's OData endpoints, used to detect which group
                                    hierarchies exist (per-dimension <Dimension>Groups on NL/OData4,
                                    the table-level CategoryGroups endpoint on EN/OData3)
        """
        dimension_mapping = {'Identifier': DCT.identifier,
                             'Title': SKOS.prefLabel,
                             'Description': SKOS.definition,
                             table_uri: SKOS.Concept} # map all concepts to this table
        id_col = 'Identifier'
        type_col = 'Kind'

        dim_type_mapping = {'TimeDimension': 'TimeDimension',
                            'GeoDimension': 'GeoDimension',
                            'GeoDetailDimension': 'GeoDimension'}
        try:
            # OData3 (EN) lists the group hierarchies of ALL dimensions in one table-level
            # CategoryGroups response; fetch it once and slice per dimension inside the loop.
            category_groups: dict = {}
            if config.LANGUAGE == 'en' and 'CategoryGroups' in group_endpoints:
                for group in _SESSION.get(f"{str(table_uri)}/CategoryGroups").json()['value']:
                    category_groups.setdefault(group['DimensionKey'], []).append(group)

            for _, dimension in dimensions.iterrows():
                dim_triples = ""
                concept_triples: list = []
                dimension_uri = Dimension.rdf_ns.term(uid(dimension[id_col]))
                dim_triples += f"<{dimension_uri}> <{DCT.isPartOf}> <{table_uri}> .\n"
                dim_triples += f"<{table_uri}> <{QB.dimension}> <{dimension_uri}> .\n"
                if dimension[type_col] in dim_type_mapping:
                    # The OData string-literal type can still be used to match on type by the existing SPARQL queries.
                    # The gecko URI class is the proper semantic form.
                    dim_triples += f"<{dimension_uri}> <{RDF.type}> \"{dim_type_mapping[dimension[type_col]]}\" .\n"
                    dim_triples += f"<{dimension_uri}> <{RDF.type}> <{GECKO.term(dim_type_mapping[dimension[type_col]])}> .\n"
                dim_triples += f"<{dimension_uri}> <{RDF.type}> <{QB.DimensionProperty}> .\n"

                dim_triples = self.create_triples(dim_triples, dimension, dimension_mapping, dimension_uri, concept_triples)

                # Model the dimension's group hierarchy as a table-scoped SKOS ConceptScheme so aggregations can later
                # enforce partition coherence. NL/OData4 exposes a per-dimension <Dimension>Groups endpoint (which
                # some dimensions, e.g. Marges, lack); EN/OData3 groups come from the table-level CategoryGroups
                # response prefetched above (normalized to the OData4 shape first). Time/geo dimensions are skipped:
                # the coherence validator exempts them (they get their own hierarchy logic from the
                # `ensure_geo_containment` logic).
                scheme_uri, group_uri_by_id = None, {}
                groups = None
                if dimension[type_col] not in dim_type_mapping:
                    if config.LANGUAGE == 'nl' and f"{dimension['Identifier']}Groups" in group_endpoints:
                        groups = pd.DataFrame(
                            _SESSION.get(f"{str(table_uri)}/{dimension['Identifier']}Groups").json()['value'], dtype=str)
                    elif dimension['Identifier'] in category_groups:
                        groups = self._normalize_odata3_groups(category_groups[dimension['Identifier']])
                if groups is not None:
                    group_triples, scheme_uri, group_uri_by_id = self._build_group_scheme(
                        groups, dimension_uri, table_uri,
                        scheme_title=dimension.get('Title') or dimension['Identifier'])
                    dim_triples += group_triples

                if config.LANGUAGE == 'en':
                    codes_url = f"{str(table_uri)}/{dimension['Identifier']}"
                else:
                    codes_url = f"{str(table_uri)}/{dimension['Identifier']}Codes"

                dimension_codes = pd.DataFrame(_SESSION.get(codes_url).json()['value'], dtype=str)
                dimension_codes.rename(columns={'Key': id_col, 'CategoryGroupID': 'DimensionGroupId'}, inplace=True)
                dimension_codes.replace(np.nan, None, inplace=True)
                if config.LANGUAGE == 'en' and 'DimensionGroupId' in dimension_codes.columns:
                    # OData3 emits CategoryGroupID as an int and its nulls coerce the column to
                    # float, so the dtype=str cast reads '4.0': strip the suffix to line the
                    # membership ids up with the (stringified) group ids.
                    dimension_codes['DimensionGroupId'] = dimension_codes['DimensionGroupId'].str.replace(r'\.0$', '', regex=True)

                for i, (_, code) in enumerate(dimension_codes.iterrows()):
                    code_uri = Dimension.rdf_ns.term(uid(code[id_col]))

                    dim_triples = self.create_triples(dim_triples, code, dimension_mapping, code_uri, concept_triples)
                    dim_triples += f"<{code_uri}> <{SKOS.broader}> <{dimension_uri}> .\n"
                    dim_triples += f"<{dimension_uri}> <{SKOS.narrower}> <{code_uri}> .\n"
                    dim_triples += f"<{code_uri}> <{DCT.isPartOf}> <{table_uri}> .\n"
                    dim_triples += f"<{table_uri}> <{QB.dimension}> <{code_uri}> .\n"
                    if dimension[type_col] in dim_type_mapping:
                        level = re.findall(r'([A-Z]{2})\d+', code[id_col])
                        if level:
                            dim_triples += f"<{code_uri}> <{SKOS.scopeNote}> \"{level[0]}\" .\n"

                        # Emit the OData native 'kind' type as a string literal and give the proper semantic
                        # gecko type for Geo- and TimeDimensions.
                        dim_triples += f"<{code_uri}> <{RDF.type}> \"{dim_type_mapping[dimension[type_col]]}\" .\n"
                        if dim_type_mapping[dimension[type_col]] == 'GeoDimension':
                            code_class = self.classify_geo_code(code[id_col])
                        else:
                            code_class = self.classify_time_code(code[id_col]) or str(GECKO.TimeDimension)
                        dim_triples += f"<{code_uri}> <{RDF.type}> <{code_class}> .\n"
                    dim_triples += f"<{code_uri}> <{RDF.type}> <{QB.DimensionProperty}> .\n"

                    # Attach the code to its group within the table-scoped scheme (partition metadata).
                    # Codes sitting directly under the root group become top concepts of the scheme.
                    if scheme_uri is not None:
                        group_uri = group_uri_by_id.get(str(code.get('DimensionGroupId')))
                        if group_uri == scheme_uri:
                            dim_triples += f"<{code_uri}> <{SKOS.topConceptOf}> <{scheme_uri}> .\n"
                            dim_triples += f"<{scheme_uri}> <{SKOS.hasTopConcept}> <{code_uri}> .\n"
                            dim_triples += f"<{code_uri}> <{SKOS.inScheme}> <{scheme_uri}> .\n"
                        elif group_uri is not None:
                            dim_triples += f"<{code_uri}> <{SKOS.broader}> <{group_uri}> .\n"
                            dim_triples += f"<{group_uri}> <{SKOS.narrower}> <{code_uri}> .\n"
                            dim_triples += f"<{code_uri}> <{SKOS.inScheme}> <{scheme_uri}> .\n"
                            # The code is a partition member that aggregates into its group; mark
                            # it partitive (XKOS) so the aggregation validator can read it directly.
                            dim_triples += f"<{code_uri}> <{XKOS.isPartOf}> <{group_uri}> .\n"
                            dim_triples += f"<{group_uri}> <{XKOS.hasPart}> <{code_uri}> .\n"

                    if i > 0 and i % _FLUSH_TRIPLES_EVERY == 0:
                        self._run_insert({'dimensies': dim_triples, 'concepts': ''.join(concept_triples)}, dimension_uri)
                        dim_triples = ""
                        concept_triples = []

                # Flush the last dims left
                self._run_insert({'dimensies': dim_triples, 'concepts': ''.join(concept_triples)}, dimension_uri)
        except Exception as e:
            logger.error(e)
            print_exception(e)
            with self._failed_lock:
                self.failed_tables.add(table_uri)
                with open(FAILED_TABLE_PATH, 'w') as f:
                    f.write(json.dumps([str(t) for t in self.failed_tables]))

    def insert_measures(self,
                        measures: pd.DataFrame,
                        table_uri: URIRef,
                        scheme_uri: Optional[URIRef] = None,
                        group_uri_by_id: Optional[dict] = None):
        """
            Loops through a dictionary with measure information and appends it to the given
            measure-triples buffer. Concept-graph triples produced along the way are appended
            to `concept_triples` so the caller can flush both in a single INSERT. When the
            table has a measure scheme, each measure is attached to its group within it
            (partition metadata for the coherence validator).

            :param measures: dictionary containing information of measures
            :param table_uri: string with the URI of the table
            :param scheme_uri: the table-scoped measure ConceptScheme, if the table has one
            :param group_uri_by_id: {group id: group node URI} within that scheme
        """
        measure_mapping = {'Id': DCT.identifier,
                           'Identifier': DCT.identifier,
                           'Title': SKOS.prefLabel,
                           'Description': SKOS.definition,
                           table_uri: SKOS.Concept}  # map all (new) concepts to this table

        msr_triples = ""
        concept_triples = []
        for _, measure in measures.iterrows():
            id_col = 'Identifier' if 'Identifier' in measure and str(measure['Identifier']) else 'Id'

            if (identifier := measure[id_col]) == '':
                identifier = self.title_to_valid_uri_str(measure['Title'])

            measure_uri = Measure.rdf_ns.term(uid(identifier))

            if 'Unit' in measure:
                u_dict = UNIT_DICT.get(measure['Unit'], "")
                if u_dict:
                    unit = u_dict.get('unit')
                    multiplier = u_dict.get('multiplier')
                    if unit:
                        msr_triples += f"<{measure_uri}> <{QUDT.unit}> <{unit}> .\n"
                    if multiplier:
                        # Full datatype URI: the fragments are parsed as plain N-Triples, which
                        #  does not allow prefixed names like xsd:integer
                        multiplier_type = f"^^<{XSD.integer}>" if multiplier.is_integer() else f"^^<{XSD.decimal}>"
                        msr_triples += f"<{measure_uri}> <{QUDT.conversionMultiplier}> \"{multiplier}\"{multiplier_type} .\n"

            total = bool(re.match(_AGGREGATE_PATTERN, measure['Title'], flags=re.IGNORECASE))
            if total:
                msr_triples += f"<{measure_uri}> <{RDF.type}> <{GECKO.Aggregate}> .\n"
            msr_triples += f"<{measure_uri}> <{DCT.isPartOf}> <{table_uri}> .\n"
            msr_triples += f"<{table_uri}> <{QB.measure}> <{measure_uri}> .\n"
            msr_triples += f"<{measure_uri}> <{RDF.type}> <{QB.MeasureProperty}> .\n"
            msr_triples = self.create_triples(msr_triples, measure, measure_mapping, measure_uri, concept_triples)

            # Attach the measure to its group within the table-scoped measure scheme (partition metadata). A measure
            # without a group id sits directly under the scheme root and becomes a top concept — typically the table-wide
            # total that the sibling groups partition, which is exactly what makes it land in its own partition for the
            # coherence validator. Group ids are only meaningful within their own table (MeasureGroups ids look like
            # measure codes, TopicGroup ids are row numbers), so they are never minted as global measure nodes.
            if scheme_uri is not None:
                gid = measure.get('MeasureGroupId')
                group_uri = scheme_uri if self._is_root_group(gid) else (group_uri_by_id or {}).get(str(gid))
                if group_uri == scheme_uri:
                    msr_triples += f"<{measure_uri}> <{SKOS.topConceptOf}> <{scheme_uri}> .\n"
                    msr_triples += f"<{scheme_uri}> <{SKOS.hasTopConcept}> <{measure_uri}> .\n"
                    msr_triples += f"<{measure_uri}> <{SKOS.inScheme}> <{scheme_uri}> .\n"
                elif group_uri is not None:
                    msr_triples += f"<{measure_uri}> <{SKOS.broader}> <{group_uri}> .\n"
                    msr_triples += f"<{group_uri}> <{SKOS.narrower}> <{measure_uri}> .\n"
                    msr_triples += f"<{measure_uri}> <{SKOS.inScheme}> <{scheme_uri}> .\n"
                    # The measure is a partition member that aggregates into its group; mark it
                    #  partitive (XKOS) so the aggregation validator can read it directly.
                    msr_triples += f"<{measure_uri}> <{XKOS.isPartOf}> <{group_uri}> .\n"
                    msr_triples += f"<{group_uri}> <{XKOS.hasPart}> <{measure_uri}> .\n"

            # Flush on accumulated triple-line count rather than a measure counter: a filtered
            # measure_codes slice (OData3/EN) carries a non-contiguous index, so an `i % N`
            # guard would rarely fire and dump every measure in one INSERT, tripping Virtuoso's
            # ~10k-line query limit. Counting lines caps the request size regardless of index shape.
            if msr_triples.count('\n') + sum(f.count('\n') for f in concept_triples) >= _MAX_INSERT_LINES:
                self._run_insert({'onderwerpen': msr_triples, 'concepts': ''.join(concept_triples)}, table_uri)
                msr_triples = ""
                concept_triples = []

        self._run_insert({'onderwerpen': msr_triples, 'concepts': ''.join(concept_triples)}, table_uri)

    def delete_update_tables(self, identifier: str):
        """Remove an entire table from the graph."""
        delete_query = textwrap.dedent(f"""
            PREFIX dct: <http://purl.org/dc/terms/>

            DELETE {{
              ?tab ?rel ?value .
              ?value2 ?rel2 ?tab .
            }}
            WHERE {{
              ?tab dct:identifier "{identifier}" .
              ?tab ?rel ?value .
              ?value2 ?rel2 ?tab .
            }}
        """)
        with self._engine_lock:
            self.engine.insert(delete_query, identifier)

    _TABLE_MAPPING = {'Identifier': DCT.identifier, 'Title': DCT.title, 'Language': DCT.language,
                      'ReleaseDate': DCT.issued, 'Modified': DCT.modified, 'StartDate': DCAT.startDate,
                      'EndDate': DCAT.endDate, 'Temporal': DCT.temporal, 'Catalog': DCAT.catalog,
                      'Description': DCT.description, 'AccrualPeriodicity': DCT.accrualPeriodicity}

    def _process_table(self, props: pd.Series):
        """
            Take the retrieved props from an OData table, retrieve dimensions
            and measures, and flush everything asynchronously to the graph.
        """
        unique_id = props['Identifier']
        logger.debug(f"Deleting table {unique_id} before inserting into graph")
        self.delete_update_tables(unique_id)
        logger.debug(f"Deleted table {unique_id}")
        logger.debug(f"Inserting table: {unique_id}")

        if config.LANGUAGE == 'en':
            base_url = "https://opendata.cbs.nl/ODataApi/OData/"
        else:
            base_url = "https://datasets.cbs.nl/odata/v1/CBS/"

        table_uri = Table.rdf_ns.term(uid(unique_id))

        try:
            if config.LANGUAGE == 'en':
                props_url = f"{base_url}{unique_id}/TableInfos"
                props = pd.Series(_SESSION.get(props_url).json()['value'][0])
                props['Id'] = props['ID']
                props['TemporalCoverage'] = props['Period']
            else:
                props_url = f"{base_url}{unique_id}/Properties"
                props = pd.Series(_SESSION.get(props_url).json())
                props['Id'] = props['Identifier']

            table_period = re.findall(r'\b[1,2]\d{3}\b', props['TemporalCoverage'])
            if len(table_period) > 1:
                table_period = table_period[::len(table_period) - 1]
                if len(table_period) > 0:
                    props['Temporal'] = f"{table_period[0]}/{table_period[-1]}"
                    props['StartDate'] = table_period[0]
                    props['EndDate'] = table_period[-1]
            props['AccrualPeriodicity'] = FREQ_DICT[props['Frequency']]

            # Dataset triples (table_mapping doesn't reference SKOS.Concept, so no concept fragments are produced)
            dataset_triples = ""
            dataset_concept_triples: list = []
            dataset_triples += f"<{table_uri}> <{RDF.type}> <{DCAT.Dataset}> .\n"
            dataset_triples += f"<{table_uri}> <{RDF.type}> <{QB.DataSet}> .\n"
            dataset_triples += f"<{table_uri}> <{DCT.creator}> \"Centraal Bureau voor de Statistiek\" .\n"
            dataset_triples = self.create_triples(dataset_triples, props, self._TABLE_MAPPING,
                                                  table_uri, dataset_concept_triples)

            # === Dimensions ===
            # The table's endpoint list tells which group hierarchies exist:
            # per-dimension <Dimension>Groups endpoints on NL/OData4, one table-level
            # CategoryGroups endpoint on EN/OData3.
            urls = [name['name'] for name in _SESSION.get(f"{base_url}{unique_id}").json()['value']]
            group_endpoints = frozenset(urls)
            if config.LANGUAGE == 'en':
                filters_url = f"{base_url}{unique_id}/DataProperties"
                filters = pd.DataFrame(_SESSION.get(filters_url).json()['value'], dtype=str)
                filters.rename(columns={'ID': 'Id', 'Key': 'Identifier', 'Type': 'Kind',
                                        'ParentID': 'ParentId'}, inplace=True)
                filters.replace(np.nan, None, inplace=True)
                for c in filters.columns:
                    filters[c] = filters[c].str.replace(r'\.0$', '', regex=True)  # Remove trailing '.0'

                dimensions = filters[filters['Kind'].str.contains('Dimension')]
                measure_groups = filters[filters['Kind'] == 'TopicGroup']
                # A Topic's ParentId points at its TopicGroup row: same role as MeasureGroupId in OData4
                measure_codes = filters[filters['Kind'] == 'Topic'].rename(columns={'ParentId': 'MeasureGroupId'})
            else:
                dimensions = pd.DataFrame(_SESSION.get(f"{base_url}{unique_id}/Dimensions").json()['value'], dtype=str)
                if 'MeasureGroups' in urls:
                    measure_groups = pd.DataFrame(_SESSION.get(f"{base_url}{unique_id}/MeasureGroups").json()['value'], dtype=str)
                else:
                    measure_groups = pd.DataFrame()
                measure_codes = pd.DataFrame(_SESSION.get(f"{base_url}{unique_id}/MeasureCodes").json()['value'],
                                             dtype=str)

            self.insert_dimensions(dimensions, table_uri, group_endpoints)

            # === Measures ===
            # The group hierarchy (MeasureGroups on OData4, TopicGroups on OData3)
            # becomes a table-scoped ConceptScheme anchored on the table node. Measures are
            # collapsed table-wide, there is no dimension node to anchor on, so aggregations
            # over measures can enforce partition coherence just like dimension collapses.
            measure_triples = ""
            msr_scheme_uri, msr_group_uri_by_id = None, {}
            if not measure_groups.empty:
                scheme_triples, msr_scheme_uri, msr_group_uri_by_id = self._build_group_scheme(
                    measure_groups, table_uri, table_uri, ns=Measure.rdf_ns, scheme_id='Measure',
                    scheme_title='Measure', aggregate_pattern=TOTAL_PATTERN)
                measure_triples += scheme_triples
                self._run_insert({'onderwerpen': measure_triples}, table_uri)

            self.insert_measures(measure_codes, table_uri, msr_scheme_uri, msr_group_uri_by_id)

            self._run_insert({'datasets': dataset_triples, 'concepts': ''.join(dataset_concept_triples)}, table_uri)
        except Exception as e:
            print_exception(e)
            logger.error(e)
            with self._failed_lock:
                self.failed_tables.add(table_uri)
                with open(FAILED_TABLE_PATH, 'w') as f:
                    f.write(json.dumps([str(t) for t in self.failed_tables]))

    def insert_tables(self):
        """
            Get a list of tables to insert into the graph, and insert the general metadata
            and all the measure and dimension information into the graph. Per-table work is
            parallelized across a thread pool. Engine writes and (in LOCAL mode) graph
            serialization are serialized via locks.
        """
        total = len(self.tables)
        logger.info(f"Starting inserting {total} table(s).")

        rows = [props for _, props in self.tables.iterrows()]
        completed = 0

        with ThreadPoolExecutor(max_workers=_TABLE_WORKERS) as executor:
            futures = [executor.submit(self._process_table, props) for props in rows]
            for fut in tqdm(as_completed(futures), total=total, desc="Building table graph",
                            bar_format=config.TQDM_BAR_FMT):
                try:
                    fut.result()
                except Exception as e:
                    logger.error(f"Table worker raised: {e}")
                completed += 1

                # Periodic LOCAL-mode flush to disk so progress survives a crash.
                if config.GRAPH_ENGINE == GraphEngineKind.LOCAL and (
                        completed == total or (completed > 0 and completed % _LOCAL_SERIALIZE_EVERY == 0)):
                    with self._graph_lock:
                        logger.info("Serializing local graph as N-triples file...")
                        self.engine.graph.serialize(config.GRAPH_FILE.replace('.trig', '.nt'),
                                                    format='nt', encoding='utf-8')
                        logger.info("Serializing local graph as Trig file...")
                        self.engine.graph.serialize(config.GRAPH_FILE.replace('.nt', '.trig'),
                                                    format='trig', encoding='utf-8')

        logger.info(f"Finished inserting {total} table(s).")


if __name__ == '__main__':
    generator = TableGraphGenerator(engine=engine)
    generator.ensure_geo_containment()

    if config.LANGUAGE == 'nl':
        table_ids = config.NL_TRAIN_TABLES + config.NL_TEST_TABLES
    else:
        table_ids = config.EN_TRAIN_TABLES + config.EN_TEST_TABLES

    # Unit test table ids
    # table_ids = ['80781ned', '84957NED', '85302NED']

    generator.check_modified_tables(fetch_table_ids=table_ids)
    generator.insert_tables()
