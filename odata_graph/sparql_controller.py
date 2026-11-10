import json
import logging
import re
import textwrap
import threading
from abc import ABC, abstractmethod
from rdflib import Graph, ConjunctiveGraph, Namespace, Literal
from SPARQLWrapper import JSON, TURTLE, SPARQLWrapper
from typing import List, NamedTuple, Optional, Set, Dict, Tuple, Union

import config
from odata_graph.namespaces._QUDT_UNIT import QUDT_UNIT
from s_expression import Table, Measure, Dimension, uri_to_code
from utils.custom_types import PartitionCoherenceError, UnitCompatibilityError

logger = logging.getLogger(__name__)

SCOT = Namespace("http://statistics.gov.scot/def/dimension/")  # Great scot!
QUDT = Namespace("http://qudt.org/schema/qudt/")
XKOS = Namespace("http://rdf-vocabulary.ddialliance.org/xkos#")  # SKOS extension for statistical classifications
GECKO = Namespace("https://vocabs.cbs.nl/def/gecko#")  # project-owned classes; see odata_graph/ontology/gecko.ttl

# Totals-only title pattern (no 'waarde': that marks value-vs-volume groupings, not totals).
# Shared between the generator (measure-group gecko:Aggregate marking) and the measure-coherence
# validator (table-scoped totals detection on measure labels).
TOTAL_PATTERN = r".*\b(totaal|totale|total)\b.*"


def _optional_value(row: dict, key: str) -> Optional[str]:
    """
        Optional binding value from a SELECT result row: unbound OPTIONAL vars are absent on
        the remote engines but present with a stringified None on the local rdflib engine.
    """
    val = row.get(key, {}).get('value')
    return None if val in (None, 'None') else val


class LevelSelection(NamedTuple):
    """
        Result of resolving an intensional `(LEVEL <level> [<scope>])` dimension selection
        against the metadata graph (see SparqlEngine.resolve_level_codes).

        :param codes: the resolved dimension code identifiers, sorted for determinism
        :param cls: full URI of the gecko level class the codes carry
        :param label: human-readable level label (in config.LANGUAGE)
        :param sql_pattern: compact SQL predicate proven EXACT against the table's full code
                            list: ('like', 'WK0363%') or ('regex', r'^\\d{4}KW\\d{2}$');
                            None when no pattern exists or it would over/under-select
    """
    codes: List[str]
    cls: str
    label: str
    sql_pattern: Optional[Tuple[str, str]]


class SparqlEngine(ABC):
    _SUBGRAPHS = {
        'concepts': 'http://concepts',
        'datasets': 'http://datasets',
        'dimensies': 'http://dimensies',
        'gecko': 'http://gecko',
        'onderwerpen': 'http://onderwerpen',
        'qudt': 'http://qudt',
    }

    # Max N-Triple lines per remote INSERT DATA. Kept well below the point where Virtuoso's generated SQL hits its 10k-line ceiling.
    _MAX_TRIPLES_PER_INSERT = 1_000

    # Max URIs per `VALUES ?obs { ... }` list in get_table_graph. Virtuoso's SPARQL->SQL compiler
    # caps the argument count of the internal built-in a large VALUES compiles into (error SP030),
    # a COMPILE-time limit no runtime setting relaxes.
    _MAX_VALUES_PER_CONSTRUCT = 1_000

    _request_lock = threading.Lock()

    local: bool
    graph: Graph
    sparql: SPARQLWrapper

    @abstractmethod
    def __init__(self):
        raise NotImplementedError()

    def graph_for(self, name: str) -> str:
        """
            Resolve a logical sub-graph name (concepts/datasets/dimensies/onderwerpen/qudt)
            to the IRI to use in GRAPH <...> { ... } or FROM <...> clauses.
        """
        return self._SUBGRAPHS[name]

    def timegeo_classes(self) -> Dict[str, str]:
        """
            Map every gecko time/geo class (the bases included) to its base class
            (gecko:TimeDimension / gecko:GeoDimension). Enumerated once from the TBox and cached;
            queries inline the closure as VALUES because dimension codes carry specific level
            classes (gecko:Year, gecko:Municipality, ...) and matching them with a
            `a/rdfs:subClassOf*` property path is prohibitively slow on rdflib's SPARQL engine.
        """
        if getattr(self, '_timegeo_closure', None) is None:
            rows = self.select(f"""
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT ?cls ?base WHERE {{
                    {{ 
                        ?cls rdfs:subClassOf* <{GECKO.TimeDimension}> . 
                        BIND(<{GECKO.TimeDimension}> AS ?base) 
                    }}
                    UNION
                    {{ 
                        ?cls rdfs:subClassOf* <{GECKO.GeoDimension}> .
                        BIND(<{GECKO.GeoDimension}> AS ?base)
                    }}
                }}
            """)
            self._timegeo_closure = {r['cls']['value']: r['base']['value'] for r in rows}
            if not self._timegeo_closure:  # gecko TBox absent (pre-migration graph)
                raise ValueError(
                    "GECKO TBox not found in the graph. Make sure the GECKO ontology is loaded into the graph.")

        return self._timegeo_closure

    @staticmethod
    def _prefix_gecko_uri(uri: str) -> str:
        return f"gecko:{uri[len(str(GECKO)):]}" if uri.startswith(str(GECKO)) else f"<{uri}>"

    def _timegeo_values(self, base: Optional[str] = None) -> str:
        """The time/geo class closure as SPARQL VALUES tokens."""
        return ' '.join(self._prefix_gecko_uri(c) for c, b in self.timegeo_classes().items() if base is None or b == base)

    def _type_marker_pairs(self) -> str:
        """(specific class -> normalized marker) VALUES pairs for the TC/GC/aggregate indication."""
        pairs = [f'({self._prefix_gecko_uri(c)} {self._prefix_gecko_uri(b)})' for c, b in
                 self.timegeo_classes().items()]
        pairs.append(f'({self._prefix_gecko_uri(str(GECKO.Aggregate))} {self._prefix_gecko_uri(str(GECKO.Aggregate))})')
        return ' '.join(pairs)

    def select(self, query: str) -> list:
        if self.local:
            result = self.graph.query(query)
            # Map the rdflib.query result to the same format as SPARQLWrapper.queryAndConvert
            return [{str(k): {
                'type': 'literal' if isinstance(v, Literal) else 'uri',
                'value': str(v)
            } for k, v in b.items()} for b in result.bindings]
        else:
            with self._request_lock:  # the shared wrapper's config only holds until queryAndConvert reads it
                self._configure_query_request('POST', JSON)
                self.sparql.setQuery(query)

                try:
                    res = self.sparql.queryAndConvert()
                except Exception as e:
                    raise RuntimeError(f"Failed to perform SELECT query: {e}")

            if isinstance(res, (bytes, bytearray)):
                res = json.loads(res.decode("utf-8"))
            return res['results']['bindings']

    def construct(self, query: str) -> ConjunctiveGraph | Graph | None:
        if self.local:
            result = self.graph.query(query)
            return result.graph
        else:
            with self._request_lock:  # the shared wrapper's config only holds until queryAndConvert reads it
                self._configure_query_request('POST', TURTLE)
                self.sparql.setQuery(query)

                try:
                    res = self.sparql.queryAndConvert()  # subgraph following from candidate nodes
                except Exception as e:
                    raise RuntimeError(f"Failed to perform CONSTRUCT query: {e}")
            if isinstance(res, (Graph, ConjunctiveGraph)):
                return res
            if isinstance(res, (bytes, bytearray)):
                g = Graph()
                g.parse(data=res.decode("utf-8"), format="turtle")
                return g
            return res

    def insert_raw(self, clauses: Dict[str, str], log_uri: str) -> None:
        """
            Insert raw N-Triples fragments into the named sub-graphs, given as a mapping of
            {logical sub-graph name: triples string}. Remote engines wrap the fragments in one or
            more INSERT DATA updates (chunked to stay under Virtuoso's SP031 generated-SQL limit,
            parsed natively server-side); LocalEngine overrides this with a direct store parse for
            significant speed increase. Use this only for non-local environments. Fragments must be
            plain N-Triples. Full URIs only, no prefixed names.

            :param clauses: {logical sub-graph name: raw N-Triples string to insert into it};
                            entries with empty triples are dropped, and the call is a no-op if nothing remains
            :param log_uri: identifies this insert in logs/error messages only. It is not part of the generated query
        """
        non_empty = {name: triples for name, triples in clauses.items() if triples}
        if not non_empty:
            return
        # Virtuoso caps the SQL it generates for an INSERT DATA at 10k lines (error SP031) and each
        # N-Triple expands to several of those lines, so a large batch must be split. The triples
        # are independent, so chunk each sub-graph's lines into requests of at most
        # _MAX_TRIPLES_PER_INSERT. This guards every caller regardless of how it batches upstream.
        for name, triples in non_empty.items():
            graph_uri = self.graph_for(name)
            lines = [ln for ln in triples.split('\n') if ln.strip()]
            for start in range(0, len(lines), self._MAX_TRIPLES_PER_INSERT):
                chunk = '\n'.join(lines[start:start + self._MAX_TRIPLES_PER_INSERT])
                self.insert(f"INSERT DATA {{\nGRAPH <{graph_uri}> {{\n{chunk}\n}}\n}}", log_uri)

    def insert(self, query: str, log_uri: str, verbose: bool = False):
        """
            Execute a SPARQL UPDATE (e.g. INSERT DATA / DELETE ... WHERE) against the graph.

            :param query: the full SPARQL UPDATE string to execute
            :param log_uri: NOT used to build the query. Ut only identifies the affected subject(s)
                            in the debug/error log lines, so callers can trace a failing update back
                            to what it was trying to write. A single update can touch many subjects
                            (e.g. a chunked bulk insert), in which case callers pass one representative
                            or marker URI rather than every subject involved.
            :param verbose: log a debug line confirming the insert on success
        """
        try:
            if self.local:
                self.graph.update(query)
            else:
                with self._request_lock:  # the shared wrapper's config only holds until query() reads it
                    self._configure_update_request(JSON)
                    self.sparql.setQuery(query)
                    self.sparql.query()

            if verbose:
                logger.debug(f"Inserted item with uri {log_uri} into graph.")
        except Exception as e:
            logger.error(f"Failed to perform INSERT query: {e}")
            logger.debug(f"Failed to insert query: {query}")
            raise e

    def _configure_query_request(self, method: str, return_format: str):
        """Set the correct headers for a SELECT/CONSTRUCT query request to the Graph instance"""
        self.sparql.endpoint = self.DEFAULT_ENDPOINT
        self.sparql.setHTTPAuth(self._http_auth)
        self.sparql.setReturnFormat(return_format)
        self.sparql.setMethod(method)
        self.sparql.setTimeout(config.GRAPH_DB_TIMEOUT)

    def _configure_update_request(self, return_format: str):
        """Set the correct headers for a INSERT request to the Graph instance"""
        self.sparql.updateEndpoint = self.DEFAULT_INSERT_ENDPOINT
        self.sparql.setHTTPAuth(self._http_auth)
        self.sparql.setReturnFormat(return_format)
        self.sparql.setMethod('POST')
        self.sparql.setTimeout(config.GRAPH_DB_TIMEOUT)

    def explode_subgraph(self, nodes: list, table_cutoff: int = 5, verbose=False) -> Graph:
        """
            Construct a subgraph from the GraphDB based on a list of given table, measure and
            dimension nodes. The graph returned will contain all the measure/dimension-table
            relations and the corresponding metadata of and hierarchies between all nodes.

            :param nodes: list of URI's (table, measure or dimension) of nodes to explode
            :param table_cutoff: maximum number of tables to create full subgraphs for
            :param verbose: print the full parsed and executed query
            :returns: subgraph
        """
        explode_tables = [n for n in nodes if n in Table.rdf_ns][:table_cutoff]  # TODO: with 5+ tables exploding the subgraph takes a long time
        table_filter = ("?s IN (<" + '>, <'.join(explode_tables) + ">)") if explode_tables else ""
        explode_obs = [n for n in nodes if n in Measure.rdf_ns or n in Dimension.rdf_ns][:10]  # TODO: request header becomes too large with too many observation nodes. 'Use scroll api'
        obs_filter = ("?o IN (<" + '>, <'.join(explode_obs) + ">)") if explode_obs else ""

        if not explode_tables and not explode_obs:  # nothing to explode -> empty FILTER would be invalid SPARQL
            return Graph()

        tg_values = self._timegeo_values()
        marker_pairs = self._type_marker_pairs()

        query = textwrap.dedent(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX xkos: <http://rdf-vocabulary.ddialliance.org/xkos#>
            PREFIX gecko: <https://vocabs.cbs.nl/def/gecko#>
            PREFIX qudt: <http://qudt.org/schema/qudt/>

            CONSTRUCT {{
                ?s ?p ?obs ;                                                            # return table-msr/dim triples
                   dct:title ?t_title .
                ?obs ?hierarchy ?group ;                                                # return dim hierarchy triples
                     ?has_type ?type ;                                                  # return optional TC/GC/aggregate indication
                     qb:concept ?concept ;
                     qb:codeList ?scheme ;                                              # return the dimension's table-scoped code list
                     xkos:isPartOf ?partition ;                                         # return the code's partition group
                     ?has_cbs_unit ?cbs_unit ;                                          # return optional units for measures
                     ?has_qudt_unit ?qudt_unit .
                ?partition skos:prefLabel ?p_label ;                                    # return partition metadata
                           skos:definition ?p_def ;
                           skos:inScheme ?p_scheme ;
                           ?p_has_type ?p_agg .
                ?concept dct:isPartOf ?s ;                                              # return concepts linked to tables and msr/dims
                         skos:prefLabel ?obs_label ;
                         ?has_def ?obs_def .
            }} WHERE {{
                {{                                                                      # get all tables and their MSRs/DIMs
                    SELECT DISTINCT ?s WHERE {{
                        ?s qb:measure|qb:dimension ?o .
                        FILTER NOT EXISTS {{                                            # don't explode time and geo dims
                            VALUES ?dim {{ {tg_values} }}
                            ?o a ?dim .                                                 # closure of the specific level classes
                        }}
                        FILTER ({table_filter} {'||' if explode_tables and explode_obs else ''}     # select specific table relevant triples
                                {obs_filter}) .                                                     # select specific MSR/DIM triples
                    }}
                }}
                ?s ?p ?obs                                                              # explode!
                FILTER (?p = qb:measure || ?p = qb:dimension) .
                ?s dct:title ?t_title .                                                 # fetch titles and labels for all subjects
                ?obs qb:concept ?concept .                                              # labels for all msrs and dims
                ?concept skos:prefLabel ?obs_label ;
                         dct:isPartOf ?s .
                FILTER (?p = qb:measure || ?p = qb:dimension) .
                FILTER NOT EXISTS {{
                    VALUES ?ex_type {{ {tg_values} }}
                    ?obs a ?ex_type ;
                         skos:broader ?ex_d .                                           # only drop the time/geo codes, not the dims themselves
                }}
                OPTIONAL {{
                    ?obs ?hierarchy ?group                                              # get hierarchy between dimensions (broader)
                    FILTER (?hierarchy = skos:broader || ?hierarchy = skos:narrower)
                    FILTER NOT EXISTS {{                                                # don't get hierarchy for time and geo dims
                        VALUES ?hx_type {{ {tg_values} }}
                        ?obs a ?hx_type .
                    }}
                    ?s qb:measure|qb:dimension ?group .                                 # only keep hierarchy groups that are a msr/dim of the table
                }}
                OPTIONAL {{                                                             # TC/GC/aggregate indication, normalized to the base class
                    VALUES (?type_cls ?type) {{ {marker_pairs} }}
                    ?obs a ?type_cls .
                    BIND (rdf:type AS ?has_type)
                }}
                OPTIONAL {{                                                             # the dimension's table-scoped code list (partition scheme)
                    ?obs qb:codeList ?scheme .
                    ?scheme dct:isPartOf ?s .
                }}
                OPTIONAL {{                                                             # the code's partition group within this table's code list
                    ?obs xkos:isPartOf ?partition .
                    ?partition skos:inScheme ?p_scheme .
                    ?p_scheme dct:isPartOf ?s .                                         # scope to this table: shared codes partition per table
                    OPTIONAL {{ ?partition skos:prefLabel ?p_label }}
                    OPTIONAL {{ ?partition skos:definition ?p_def }}
                    OPTIONAL {{
                        ?partition a ?p_agg .                                           # mark total partitions (gecko:Aggregate)
                        FILTER (?p_agg = gecko:Aggregate)
                        BIND (rdf:type AS ?p_has_type)
                    }}
                }}
                OPTIONAL {{                                                             # get OData4 units of measure (non-standardised)
                    ?obs qudt:unitOfSystem ?cbs_unit .
                    BIND (qudt:unitOfSystem AS ?has_cbs_unit)                           # predicate is constant; BIND (not a re-lookup) keeps the estimator happy
                }}
                OPTIONAL {{                                                             # get units of measure
                    ?obs qudt:unit ?qudt_unit .
                    BIND (qudt:unit AS ?has_qudt_unit)
                }}
                OPTIONAL {{                                                             # get msr/dim definitions
                    ?concept skos:definition ?obs_def .
                    BIND (skos:definition AS ?has_def)
                }}
            }}
        """)

        if verbose:
            logger.debug(query)

        return self.construct(query)

    def explode_subgraph_msr_dims_only(self, nodes: list, table_cutoff: int = 5, verbose: bool = False) -> Graph:
        """
            Construct a subgraph from the GraphDB based on a list of given table, measure and
            dimension nodes. The graph returned will contain all the measure/dimension-table
            relations. This function omits all hierarchical relations between measures and
            dimensions, labels and units! Use `explode_subgraph()` to obtain the full graph if
            needed.

            :param nodes: list of URI's (table, measure or dimension) of nodes to explode
            :param table_cutoff: maximum number of tables to create full subgraphs for
            :param verbose: print the full parsed and executed query
            :returns: subgraph
        """
        explode_tables = [n for n in nodes if n in Table.rdf_ns][:table_cutoff]
        table_filter = ("?s IN (<" + '>, <'.join(explode_tables) + ">)") if explode_tables else ""
        explode_obs = [n for n in nodes if n in Measure.rdf_ns or n in Dimension.rdf_ns][:20]
        obs_filter = ("?o IN (<" + '>, <'.join(explode_obs) + ">)") if explode_obs else ""

        if not explode_tables and not explode_obs:  # nothing to explode -> empty FILTER would be invalid SPARQL
            return Graph()

        tg_values = self._timegeo_values()

        query = textwrap.dedent(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX gecko: <https://vocabs.cbs.nl/def/gecko#>

            CONSTRUCT {{
                ?s ?p ?obs .                                                            # return table-msr/dim triples
            }} WHERE {{
                {{                                                                      # get all tables and their MSRs/DIMs
                    SELECT DISTINCT ?s WHERE {{
                        ?s qb:measure|qb:dimension ?o .
                        FILTER NOT EXISTS {{                                            # don't explode time and geo dims
                            VALUES ?dim {{ {tg_values} }}
                            ?o a ?dim .                                                 # closure of the specific level classes
                        }}
                        # select specific table relevant triples and select specific MSR/DIM triples
                        FILTER ({table_filter} {'||' if explode_tables and explode_obs else ''}
                                {obs_filter}) .
                    }}
                }}
                ?s ?p ?obs                                                              # explode!
                FILTER (?p = qb:measure || ?p = qb:dimension)
                FILTER NOT EXISTS {{                                                    # don't return time/geo codes
                    VALUES ?ex_type {{ {tg_values} }}
                    ?obs a ?ex_type ;
                         skos:broader ?ex_d .
                }}
            }}
        """)

        if verbose:
            logger.debug(query)

        return self.construct(query)

    def get_table_graph(self, table: Table, include_time_geo_dims: bool = False,
                        geo_time_codes: Optional[Set[str]] = None) -> Graph:
        """
            Construct a subgraph from the GraphDB based a single given table. The graph
            returned will contain all the measure/dimension-table relations and the
            corresponding metadata of and hierarchies between all nodes.

            :param table: table node to construct graph for
            :param include_time_geo_dims: if false, skips the time and geo dim codes, which can become huge
            :param geo_time_codes: when given, overrides include_time_geo_dims and includes ONLY these specific time/geo
                                   code identifiers (plus all measures and non-time/geo dims). Lets a caller that will
                                   keep only a handful of codes (linked + small-level) avoid pulling the thousands of
                                   wijk/buurt codes it would immediately discard.
            :returns: subgraph
        """
        tg_values = self._timegeo_values()
        marker_pairs = self._type_marker_pairs()

        drop_timegeo_codes = drop_timegeo_hierarchy = ''
        wanted = None
        if geo_time_codes is not None:
            # Scoped mode: bind ?obs to an explicit, small set (measures + dimension groups + non-time/geo dim codes +
            # the allowlisted time/geo codes) computed with one cheap SELECT, rather than pulling every code and dropping
            # the unwanted ones with a table-wide `FILTER NOT EXISTS` inside the CONSTRUCT.
            wanted = {r['o']['value'] for r in self.select(f"""
                PREFIX qb: <http://purl.org/linked-data/cube#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                SELECT ?o WHERE {{
                    <{table.uri}> qb:measure|qb:dimension ?o .
                    FILTER NOT EXISTS {{                                        # exclude time/geo leaf codes; keep groups (no broader) + regular dims
                        ?o a ?c .
                        ?c rdfs:subClassOf ?base .
                        FILTER (?base IN (<{GECKO.GeoDimension}>, <{GECKO.TimeDimension}>))
                        ?o skos:broader ?x .
                    }}
                }}
            """)}
            wanted |= {str(Dimension(c).uri) for c in geo_time_codes}
            if not wanted:
                return Graph()
        elif not include_time_geo_dims:
            drop_timegeo_codes = textwrap.dedent(f"""
                FILTER NOT EXISTS {{
                    VALUES ?ex_type {{ {tg_values} }}
                    ?obs a ?ex_type ;
                         skos:broader ?ex_d .                                   # only drop the time/geo codes, not the dims themselves
                }}
            """)
            drop_timegeo_hierarchy = textwrap.dedent(f"""
                FILTER NOT EXISTS {{                                            # don't get hierarchy for time and geo dims
                    VALUES ?hx_type {{ {tg_values} }}
                    ?obs a ?hx_type .
                }}
            """)

        def build_query(obs_values: str) -> str:
            return textwrap.dedent(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX xkos: <http://rdf-vocabulary.ddialliance.org/xkos#>
            PREFIX gecko: <https://vocabs.cbs.nl/def/gecko#>
            PREFIX qudt: <http://qudt.org/schema/qudt/>

            CONSTRUCT {{
                ?s ?p ?obs ;                                                            # return table-msr/dim triples
                   dct:title ?t_title .
                ?obs ?hierarchy ?group ;                                                # return dim hierarchy triples
                     ?has_type ?type ;                                                  # return optional TC/GC/aggregate indication
                     qb:concept ?concept ;
                     qb:codeList ?scheme ;                                              # return the dimension's table-scoped code list
                     xkos:isPartOf ?partition ;                                         # return the code's partition group
                     ?has_cbs_unit ?cbs_unit ;                                          # return optional units for measures
                     ?has_qudt_unit ?qudt_unit .
                ?partition skos:prefLabel ?p_label ;                                    # return partition metadata
                           skos:definition ?p_def ;
                           skos:inScheme ?p_scheme ;
                           ?p_has_type ?p_agg .
                ?concept dct:isPartOf ?s ;                                              # return concepts linked to tables and msr/dims
                         skos:prefLabel ?obs_label ;
                         ?has_def ?obs_def .
            }} WHERE {{
                VALUES ?s {{ <{table.uri}> }}
                {obs_values}
                {{ ?s qb:measure ?obs . BIND(qb:measure AS ?p) }}
                UNION
                {{ ?s qb:dimension ?obs . BIND(qb:dimension AS ?p) }}
                {drop_timegeo_codes}
                ?s dct:title ?t_title .
                ?obs qb:concept ?concept .                                              # labels for all msrs and dims
                ?concept skos:prefLabel ?obs_label ;
                         dct:isPartOf ?s .
                OPTIONAL {{                                                             # get hierarchy between dimensions (broader/narrower)
                    {{ ?obs skos:broader ?group . BIND(skos:broader AS ?hierarchy) }}   # predicate bound for the estimator, kept for CONSTRUCT
                    UNION
                    {{ ?obs skos:narrower ?group . BIND(skos:narrower AS ?hierarchy) }}
                    {drop_timegeo_hierarchy}
                    ?s qb:measure|qb:dimension ?group .                                 # only keep hierarchy groups that are a msr/dim of the table
                }}
                OPTIONAL {{                                                             # TC/GC/aggregate indication, normalized to the base class
                    VALUES (?type_cls ?type) {{ {marker_pairs} }}
                    ?obs a ?type_cls .
                    BIND (rdf:type AS ?has_type)
                }}
                OPTIONAL {{                                                             # the dimension's table-scoped code list (partition scheme)
                    ?obs qb:codeList ?scheme .
                    ?scheme dct:isPartOf ?s .
                }}
                OPTIONAL {{                                                             # the code's partition group within this table's code list
                    ?obs xkos:isPartOf ?partition .
                    ?partition skos:inScheme ?p_scheme .
                    ?p_scheme dct:isPartOf ?s .                                         # scope to this table: shared codes partition per table
                    OPTIONAL {{ ?partition skos:prefLabel ?p_label }}
                    OPTIONAL {{ ?partition skos:definition ?p_def }}
                    OPTIONAL {{
                        ?partition a ?p_agg .                                           # mark total partitions (gecko:Aggregate)
                        FILTER (?p_agg = gecko:Aggregate)
                        BIND (rdf:type AS ?p_has_type)
                    }}
                }}
                OPTIONAL {{                                                             # get OData4 units of measure (non-standardised)
                    ?obs qudt:unitOfSystem ?cbs_unit .
                    BIND (qudt:unitOfSystem AS ?has_cbs_unit)                           # predicate is constant; BIND (not a re-lookup) keeps the estimator happy
                }}
                OPTIONAL {{                                                             # get units of measure
                    ?obs qudt:unit ?qudt_unit .
                    BIND (qudt:unit AS ?has_qudt_unit)
                }}
                OPTIONAL {{                                                             # get msr/dim definitions
                    ?concept skos:definition ?obs_def .
                    BIND (skos:definition AS ?has_def)
                }}
            }}
        """)

        # Non-scoped mode (unbounded ?obs, or the time/geo drop-filters): one query, no VALUES list.
        if wanted is None:
            return self.construct(build_query(''))

        # Scoped mode: chunk the VALUES ?obs list to stay under Virtuoso's SP030 compile-time
        # argument cap (see _MAX_VALUES_PER_CONSTRUCT), unioning the per-batch subgraphs.
        obs = sorted(wanted)
        graph = Graph()
        for i in range(0, len(obs), self._MAX_VALUES_PER_CONSTRUCT):
            chunk = obs[i:i + self._MAX_VALUES_PER_CONSTRUCT]
            graph += self.construct(build_query(f"VALUES ?obs {{ <{'> <'.join(chunk)}> }}"))
        return graph

    def get_table_geo_dims(self, table: Table,
                           levels: Union[str, Set[str], None] = None) -> Dict[str, Tuple[str, str]]:
        """
            Get the geo dimension codes (not groups!) of a table, with their label and level token.

            :param table: the table whose codes to fetch
            :param levels: which geo levels to include, as canonical level tokens:
                           None (default) keeps every level EXCEPT the finest wijk/buurt levels
                           (WK/BU), whose thousands of codes would bloat per-request payloads;
                           'all' includes everything (e.g. for the entity-linking gazetteer);
                           a set of tokens selects specific levels.
            :returns: {code: (prefLabel, level_token)}
        """
        geo_classes = [c for c, meta in self.level_classes().items() if meta['base'] == str(GECKO.GeoDimension)]
        query = (f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

            SELECT DISTINCT ?id ?prefLabel ?cls WHERE {{
                VALUES ?s {{ <{table.uri}> }}
                ?s qb:dimension ?dim .
                ?dim dct:identifier ?id ;
                     qb:concept ?concept ;
                     a ?cls ;                                                           # specific area class -> level token
                     skos:broader ?d .                                                  # only codes, not the dim groups
                ?concept dct:isPartOf ?s ;
                         skos:prefLabel ?prefLabel .
                FILTER (?cls IN ({', '.join(f'<{c}>' for c in geo_classes)}))
            }}
        """)

        if levels is None:
            included = lambda token: token not in {'WK', 'BU'}
        elif levels == 'all':
            included = lambda token: True
        else:
            included = lambda token: token in levels

        res = self.select(query)
        return {d['id']['value']: (d['prefLabel']['value'], token) for d in res
                if included(token := self._level_token(d['cls']['value']))}

    def get_table_time_dims(self, table: Table) -> dict:
        """Get all the time dimension codes (not groups!) of a table."""
        query = (f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX gecko: <https://vocabs.cbs.nl/def/gecko#>

            SELECT DISTINCT ?dim ?prefLabel WHERE {{
                VALUES ?s {{ <{table.uri}> }}
                ?s qb:dimension ?dim .
                ?dim dct:identifier ?id ;
                     qb:concept ?concept .
                ?concept dct:isPartOf ?s ;
                         skos:prefLabel ?prefLabel .
                FILTER EXISTS {{
                    VALUES ?time_cls {{ {self._timegeo_values(base=str(GECKO.TimeDimension))} }}
                    ?dim a ?time_cls ;                                                  # closure of the specific granularity classes
                         skos:broader ?d .
                }}
            }}
        """)

        res = self.select(query)
        return {uri_to_code(d['dim']['value']): d['prefLabel']['value'] for d in res}

    def level_classes(self) -> Dict[str, dict]:
        """
            Metadata for every specific gecko time/geo level class (the bases excluded):
            {class_uri: {'base': base_uri, 'labels': {'en': ..., 'nl': ...},
                         'prefix': codePrefix|None, 'infix': codeInfix|None, 'level': int|None}}.
            Enumerated once from the TBox and cached, like timegeo_classes.
        """
        if getattr(self, '_level_classes', None) is None:
            rows = self.select(f"""
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX gecko: <{GECKO}>
                SELECT ?cls ?base ?label_en ?label_nl ?prefix ?infix ?level WHERE {{
                    VALUES ?base {{ <{GECKO.TimeDimension}> <{GECKO.GeoDimension}> }}
                    ?cls rdfs:subClassOf ?base .
                    OPTIONAL {{ ?cls rdfs:label ?label_en FILTER (lang(?label_en) = 'en') }}
                    OPTIONAL {{ ?cls rdfs:label ?label_nl FILTER (lang(?label_nl) = 'nl') }}
                    OPTIONAL {{ ?cls gecko:codePrefix ?prefix }}
                    OPTIONAL {{ ?cls gecko:codeInfix ?infix }}
                    OPTIONAL {{ ?cls gecko:level ?level }}
                }}
            """)
            self._level_classes = {r['cls']['value']: {
                'base': r['base']['value'],
                'labels': {'en': _optional_value(r, 'label_en'), 'nl': _optional_value(r, 'label_nl')},
                'prefix': _optional_value(r, 'prefix'),
                'infix': _optional_value(r, 'infix'),
                'level': int(lvl) if (lvl := _optional_value(r, 'level')) else None,
            } for r in rows}
        return self._level_classes

    def _level_label(self, cls: str) -> str:
        """The level class' label in the application language, falling back to EN/local name."""
        meta = self.level_classes()[cls]
        return meta['labels'].get(config.LANGUAGE) or meta['labels'].get('en') or cls.split('#')[-1]

    def _level_token(self, cls: str) -> str:
        """
            The canonical `(LEVEL <token>)` selection token for a level class: its code
            prefix/infix (WK, GM, JJ, ...), falling back to the class local name for the
            classes without one (Date, UnknownRegion).
        """
        meta = self.level_classes()[cls]
        return meta['prefix'] or meta['infix'] or cls.split('#')[-1]

    def _level_candidates(self, token: str) -> List[str]:
        """
            Level class URIs whose canonical token (see _level_token) matches, case-insensitively.
            Tokens are unique in the current TBox, but the caller still disambiguates by which
            class carries codes in the queried dimension, guarding against future token reuse.
        """
        t = token.strip().lower()
        return [cls for cls in self.level_classes() if t == self._level_token(cls).lower()]

    def _available_levels(self, table: Table, dim_group: Dimension) -> List[str]:
        """The level tokens (with labels) present among a dimension's codes in the given table."""
        rows = self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT DISTINCT ?cls WHERE {{
                <{table.uri}> qb:dimension ?code .
                ?code skos:broader <{dim_group.uri}> ;
                      a ?cls .
                FILTER (?cls IN ({', '.join(f'<{c}>' for c in self.level_classes())}))
            }}
        """)
        return sorted(f"{self._level_token(r['cls']['value'])} ({self._level_label(r['cls']['value'])})"
                      for r in rows)

    def resolve_level_codes(self, table: Table, dim_group: Dimension, level: str,
                            scope: Optional[str] = None) -> LevelSelection:
        """
            Resolve an intensional `(LEVEL <level> [<scope>])` dimension selection to the
            table's concrete code identifiers, so queries can name a whole level (all wijken,
            all years) instead of enumerating thousands of codes. The gecko level classes on
            the codes are the source of truth; the class' codePrefix/codeInfix additionally
            yields a compact SQL predicate when proven exact against the table's full code
            list (hybrid resolution).

            A scope restricts the level to the codes contained in the given area/period:
              - code embedding: WK/BU codes embed their parent's digits (WK0363.. sits in
                GM0363) and period codes start with their year (2023KW01 sits in 2023),
                so no graph edges are needed;
              - graph containment: xkos:isPartOf/xkos:hasPart edges between area codes
                (e.g. municipalities in a province).

            :param table: the table whose code list scopes the resolution
            :param dim_group: the dimension group holding the codes (e.g. RegioS, Perioden)
            :param level: the level's canonical token (case-insensitive): its code prefix/infix (WK, GM,
                          JJ, KW, ...), or the gecko class local name for the levels without one (Date)
            :param scope: optional containing area/period code (e.g. GM0363, or a year like 2023)
            :raises ValueError: unknown level, level not present in the table, or empty scope
        """
        candidates = self._level_candidates(level)
        if not candidates:
            known = sorted({f"{self._level_token(cls)} ({self._level_label(cls)})"
                            for cls in self.level_classes()})
            raise ValueError(f"Unknown level '{level}' in `(LEVEL {level}{f' {scope}' if scope else ''})`. "
                             f"Known levels: {', '.join(known)}.")

        # A token can in theory match multiple classes; the one carrying codes in this
        #  table's dimension wins (levels never mix within one dimension group).
        codes, cls = [], None
        for cand in candidates:
            rows = self.select(f"""
                PREFIX qb: <http://purl.org/linked-data/cube#>
                PREFIX dct: <http://purl.org/dc/terms/>
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                SELECT DISTINCT ?id WHERE {{
                    <{table.uri}> qb:dimension ?code .
                    ?code skos:broader <{dim_group.uri}> ;
                          a <{cand}> ;
                          dct:identifier ?id .
                }}
            """)
            if rows:
                codes, cls = sorted(r['id']['value'] for r in rows), cand
                break
        if not codes:
            available = ', '.join(self._available_levels(table, dim_group)) or 'none'
            raise ValueError(f"Level '{level}' has no codes in dimension '{dim_group}' of "
                             f"table '{table}'. Available levels: {available}.")
        meta = self.level_classes()[cls]

        scope_digits, embedded = '', set()
        if scope:
            # Code-embedding rule: WK/BU codes embed the gemeente digits (and BU the wijk
            # digits), period codes start with their year. Coarser geo levels do NOT prefix
            # their children (GM codes don't embed PV digits), so embedding only applies to
            # the chain below the municipality level; anything else needs containment edges.
            if meta['base'] == str(GECKO.GeoDimension):
                scope_prefix = re.match(r'^[A-Za-z]+', scope)
                scope_digits = re.sub(r'^[A-Za-z]+', '', scope)
                if (scope_prefix and scope_digits and meta['prefix']
                        and (scope_prefix.group(), meta['prefix']) in {('GM', 'WK'), ('GM', 'BU'), ('WK', 'BU')}):
                    embedded = {c for c in codes if c.startswith(meta['prefix'] + scope_digits)}
                else:
                    scope_digits = ''  # digits are not embeddable -> also no scoped LIKE pattern
            elif re.fullmatch(r'\d{4}', scope):
                embedded = {c for c in codes if c.startswith(scope)}

            # Graph containment rule (area containment edges, both edge directions occur)
            rows = self.select(f"""
                PREFIX dct: <http://purl.org/dc/terms/>
                PREFIX xkos: <http://rdf-vocabulary.ddialliance.org/xkos#>
                SELECT DISTINCT ?id WHERE {{
                    {{ ?code xkos:isPartOf+ <{Dimension(scope).uri}> }}
                    UNION
                    {{ <{Dimension(scope).uri}> xkos:hasPart+ ?code }}
                    ?code dct:identifier ?id .
                }}
            """)
            contained = {r['id']['value'] for r in rows}

            scoped = sorted(embedded | (set(codes) & contained))
            if not scoped:
                raise ValueError(f"Scope '{scope}' in `(LEVEL {level} {scope})` contains no "
                                 f"'{level}' codes of dimension '{dim_group}' in table '{table}'.")
            codes = scoped

        return LevelSelection(codes=codes, cls=cls, label=self._level_label(cls),
                              sql_pattern=self._level_sql_pattern(table, dim_group, codes, meta, scope, scope_digits, embedded))

    def _level_sql_pattern(self, table: Table, dim_group: Dimension, codes: List[str], meta: dict,
                           scope: Optional[str], scope_digits: str, embedded: Set[str]) -> Optional[Tuple[str, str]]:
        """
            The compact SQL predicate for a resolved level selection: ('like', 'WK0363%') for
            geo prefixes or ('regex', '^\\d{4}KW\\d{2}$') for time infixes. Only returned when
            proven EXACT against the table's full code list for the dimension, so the predicate
            can safely replace the enumerated IN-list in generated SQL; None otherwise
            (e.g. a graph-edge-scoped selection, which no code pattern can express).
        """
        pattern = None
        if meta['base'] == str(GECKO.GeoDimension) and meta['prefix']:
            if not scope:
                pattern = ('like', f"{meta['prefix']}%")
            elif scope_digits and embedded and set(codes) == embedded:
                pattern = ('like', f"{meta['prefix']}{scope_digits}%")
        elif meta['base'] == str(GECKO.TimeDimension):
            year = scope if scope and re.fullmatch(r'\d{4}', scope) else r'\d{4}'
            if meta['infix']:
                pattern = ('regex', rf"^{year}{re.escape(meta['infix'])}\d{{2}}$")
            elif meta['level'] == 5 and not scope:  # gecko:Date: digits-only codes
                pattern = ('regex', r'^\d{8}$')
        if pattern is None:
            return None

        # The pattern must select precisely the resolved codes among all the table's codes for this dimension;
        # otherwise fall back to the IN-list.
        rows = self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT DISTINCT ?id WHERE {{
                <{table.uri}> qb:dimension ?code .
                ?code skos:broader <{dim_group.uri}> ;
                      dct:identifier ?id .
            }}
        """)
        all_codes = {r['id']['value'] for r in rows}
        kind, pat = pattern
        matches = ({c for c in all_codes if c.startswith(pat[:-1])} if kind == 'like'
                   else {c for c in all_codes if re.fullmatch(pat[1:-1], c)})
        return pattern if matches == set(codes) else None

    def get_dim_level_summary(self, table: Table,
                              include_codes_upto: int = 0) -> Dict[str, List[Tuple[str, str, int, Optional[List[str]]]]]:
        """
            Per time/geo dimension group of the table: the levels its codes carry as
            (selection token, label, code count, codes), e.g. {'RegioS': [('PV', 'Provincie',
            12, ['PV20', ...]), ('GM', 'Gemeente', 605, None)]}. Hierarchical chain levels come
            first (by gecko:level), then the non-standardized groupings by descending count.
            Used to compact prompts: one level line replaces thousands of geo/time code lines
            (models select a level with `(DIM <group> (LEVEL <token>))`).

            :param table: the table whose dimensions to summarize
            :param include_codes_upto: levels with at most this many codes also return their
                                       concrete code list (small levels stay cheap to list
                                       literally in prompts); larger levels return None
        """
        rows = self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT DISTINCT ?group ?cls ?id WHERE {{
                <{table.uri}> qb:dimension ?code , ?group .
                ?code skos:broader ?group ;
                      dct:identifier ?id ;
                      a ?cls .
                FILTER (?cls IN ({', '.join(f'<{c}>' for c in self.level_classes())}))
            }}
        """)
        codes_per_level: Dict[Tuple[str, str], List[str]] = {}
        for r in rows:
            codes_per_level.setdefault((uri_to_code(r['group']['value']), r['cls']['value']),
                                       []).append(r['id']['value'])

        summary: Dict[str, List[Tuple[str, str, Optional[int], int, Optional[List[str]]]]] = {}
        for (group, cls), codes in codes_per_level.items():
            summary.setdefault(group, []).append(
                (self._level_token(cls), self._level_label(cls), self.level_classes()[cls]['level'],
                 len(codes), sorted(codes) if len(codes) <= include_codes_upto else None))
        return {group: [(token, label, n, codes) for token, label, _, n, codes in
                        sorted(entries, key=lambda e: (e[2] is None, e[2] or 0, -e[3]))]
                for group, entries in summary.items()}

    def validate_table_measures(self, table: Table, measures: Set[Measure]) -> None:
        """
            Check that every selected measure is a measure of the table. An unknown code silently returns
            an empty result from OData, for example when selecting a MeasureGroup. For these, the error
            lists the group's member measures, so the LLM can correct its selection in the retry loop.

            :param table: the table the measures are selected from
            :param measures: the selected measures
            :raises ValueError: when a selected measure is not a measure of the table
        """
        if not measures:
            return
        known = {uri_to_code(r['msr']['value']) for r in self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            SELECT ?msr WHERE {{
                VALUES ?msr {{ <{'> <'.join(sorted(str(m.uri) for m in measures))}> }}
                <{table.uri}> qb:measure ?msr .
            }}
        """)}
        if not (unknown := sorted({str(m) for m in measures} - known)):
            return

        # Resolve which unknown codes are measure groups of this table, with their (possibly nested) member measures
        group_uris = {str(Measure.rdf_ns.term(f"{table}/g{code}")): code for code in unknown}
        members: Dict[str, Set[str]] = {}
        for r in self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX xkos: <http://rdf-vocabulary.ddialliance.org/xkos#>
            SELECT ?group ?member WHERE {{
                VALUES ?group {{ <{'> <'.join(sorted(group_uris))}> }}
                ?group skos:inScheme ?scheme .
                OPTIONAL {{
                    ?member xkos:isPartOf+ ?group .
                    <{table.uri}> qb:measure ?member .
                }}
            }}
        """):
            group_members = members.setdefault(group_uris[r['group']['value']], set())
            if 'member' in r:
                group_members.add(uri_to_code(r['member']['value']))

        errors = []
        for code in unknown:
            if code in members:
                errors.append(f"'{code}' is a MeasureGroup of table '{table}', not a measure, and can't be selected. "
                              f"Select one or more of its measures instead: {self._truncated_codes(members[code])}.")
            else:
                errors.append(f"'{code}' is not a measure of table '{table}'.")
        raise ValueError(f"Invalid measure selection: {' '.join(errors)}\n"
                         f"Please only select measure codes that are listed as a Measure of the table.")

    def get_table_titles(self, nodes: list) -> dict:
        explode_tables = [n for n in nodes if n in Table.rdf_ns]
        if not explode_tables:  # no tables to look up -> empty FILTER would be invalid SPARQL
            return {}
        table_filter = "?s IN (<" + '>, <'.join(explode_tables) + ">)"

        query = (f"""
            PREFIX dct: <http://purl.org/dc/terms/>

            SELECT DISTINCT ?id ?title ?description
            WHERE {{ 
                ?s dct:identifier ?id ;
                   dct:title ?title ;
                   dct:description ?description .
                FILTER ({table_filter}) .
            }}
        """)

        res = self.select(query)
        return {uri_to_code(d['id']['value']): {
            'title': d['title']['value'],
            'description': d['description']['value']
        } for d in res}

    def validate_partition_coherence(self, table: Table, dimension: Dimension, codes: Set[Dimension]) -> None:
        """
            Validate that an aggregation collapsing `dimension` over `codes` is partition-coherent,
            i.e. the code-set sits within exactly one dimension group (partition) of the table's
            code list and does not mix a total/aggregate with sibling codes. Incoherent sets
            double-count: a grand total equals the sum of each partition, and sibling partitions
            (e.g. destination vs season) each sum to the same whole.

            Time/geo dimensions get the level-based (same-class) rule instead of the group rule:
            their OData groups are organizational (e.g. wijken grouped per gemeente), not
            aggregational partitions, while overlap follows containment (a province total already
            contains its municipalities, a year its quarters).

            The verdict is static (metadata-only) and fails open when no metadata exists:
              - dimensions without a table-scoped code list (e.g. Marges, EN OData3 tables)
                carry no partition metadata, so there is nothing to enforce;
              - time/geo codes without gecko classes (pre-migration graphs) likewise.
            Only the table-scoped group-level gecko:Aggregate flag is trusted; the code-level
            flag is global and polluted by titles from unrelated tables (see L999996).

            :param table: the table whose code list scopes the check
            :param dimension: the dimension being collapsed by the aggregation
            :param codes: the selected codes; an empty set means an open `()` selection
            :raises PartitionCoherenceError: when the code-set is incoherent
        """
        timegeo = self.select(f"""
            SELECT ?t WHERE {{
                <{dimension.uri}> a ?t .
                FILTER (?t IN (<{GECKO.TimeDimension}>, <{GECKO.GeoDimension}>))
            }} LIMIT 1
        """)
        if timegeo:
            return self._validate_timegeo_coherence(table, dimension, codes)

        scheme = self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            SELECT ?scheme WHERE {{
                <{dimension.uri}> qb:codeList ?scheme .
                ?scheme dct:isPartOf <{table.uri}> .
            }} LIMIT 1
        """)
        if not scheme:
            return

        context = (f"Aggregating over dimension '{dimension}' of table '{table}' "
                   f"with {'an open () selection' if not codes else f'codes {sorted(str(c) for c in codes)}'}")
        self._enforce_group_coherence(scheme[0]['scheme']['value'], codes, 'qb:DimensionProperty', context,
                                      noun='codes')

    def validate_measure_coherence(self, table: Table, measures: Set[Measure]) -> None:
        """
            Measure-side coherence check, for aggregations that collapse the Measure axis `MSR ()`.
            Unlike dimension groups, measure groups do not each sum to the same whole: they are usually
            thematic families ('Varkens' | 'Kippen' | 'Rundvee'), and combining measures across them
            (piglets + laying hens) is a legitimate composite. What is blocked, is combining a TOTAL with
            what it totals:
              - a *grand total* (member of a gecko:Aggregate-marked group, or an ungrouped
                top-level measure whose table-scoped label denotes a total) combined with any
                other measure of the scheme;
              - a *group total* (totals-labelled member of a group) combined with other members
                of its OWN group. Totals of different groups may combine freely.
            Totality is derived from the table-scoped scheme (aggregate flag on group nodes) and
            table-scoped concept labels. Never from the global measure node, whose title-based
            flag is polluted across tables.

            :param table: the table whose measure scheme scopes the check
            :param measures: the selected measures; an empty set means all of the table's measures
            :raises PartitionCoherenceError: when the measure-set is incoherent
        """
        scheme = self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            SELECT ?scheme WHERE {{
                <{table.uri}> qb:codeList ?scheme .
                ?scheme dct:isPartOf <{table.uri}> .
                FILTER (STRSTARTS(STR(?scheme), "{Measure.rdf_ns}"))
            }} LIMIT 1
        """)
        if not scheme:
            return
        scheme_uri = scheme[0]['scheme']['value']

        item_filter = f"FILTER (?item IN (<{'>, <'.join(sorted(str(m.uri) for m in measures))}>))" if measures else ''
        rows = self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX xkos: <http://rdf-vocabulary.ddialliance.org/xkos#>
            PREFIX gecko: <{GECKO}>
            SELECT ?item ?group ?agg ?label WHERE {{
                ?item skos:inScheme <{scheme_uri}> ;
                      a qb:MeasureProperty .
                OPTIONAL {{
                    ?item xkos:isPartOf ?group .
                    ?group skos:inScheme <{scheme_uri}> .
                    # the aggregate flag of the item's OWN group; an EXISTS outside this OPTIONAL would see
                    #  ?group unbound for ungrouped items and match any aggregate group in the graph
                    OPTIONAL {{ ?group a <{GECKO.Aggregate}> . BIND (true AS ?agg) }}
                }}
                OPTIONAL {{
                    # table-scoped label: the concept is linked to THIS table, so a totals title
                    #  from an unrelated table can never mark this measure
                    ?item qb:concept ?c .
                    ?c dct:isPartOf <{table.uri}> ;
                       skos:prefLabel ?label .
                }}
                {item_filter}
            }}
        """)
        if not rows:
            return  # scheme carries no memberships -> no metadata to enforce

        _TOP = '(top-level)'
        group_of: Dict[str, str] = {}
        grand_totals, group_totals = set(), set()
        for r in rows:
            item = uri_to_code(r['item']['value'])
            group = _optional_value(r, 'group') or _TOP
            group_of[item] = group
            is_total_label = bool(re.match(TOTAL_PATTERN, _optional_value(r, 'label') or '', flags=re.IGNORECASE))
            if _optional_value(r, 'agg') == 'true' or (group == _TOP and is_total_label):
                grand_totals.add(item)
            elif is_total_label:
                group_totals.add(item)

        selected = {str(m) for m in measures} if measures else set(group_of)
        context = (f"Aggregating the measures of table '{table}' "
                   f"with {'an open selection' if not measures else f'measures {sorted(selected)}'}")

        if unknown := selected - set(group_of):
            raise PartitionCoherenceError(
                f"{context} is not allowed: measures {sorted(unknown)} are not part of the "
                f"table's measure scheme, so their totality cannot be verified.")

        if len(selected) > 1:
            if grand := selected & grand_totals:
                raise PartitionCoherenceError(
                    f"{context} is not allowed: {sorted(grand)} are table-wide total measures "
                    f"and already aggregate the other measures, so combining them double-counts."
                    f"\nRewrite the query to select either the total on its own, or only "
                    f"non-total measures.")
            for total in selected & group_totals:
                if siblings := {i for i in selected if i != total and group_of[i] == group_of[total]}:
                    raise PartitionCoherenceError(
                        f"{context} is not allowed: '{total}' is the total of its measure group "
                        f"and already aggregates {sorted(siblings)}, so combining them "
                        f"double-counts.\nRewrite the query to select either the group total on "
                        f"its own, or only the non-total measures of the group.")

    def _enforce_group_coherence(self, scheme_uri: str, items: Set, member_class: str,
                                 context: str, noun: str) -> None:
        """
            The dimension-side group rule behind validate_partition_coherence: within the given
            table-scoped scheme, the selected items must (1) all be known scheme members,
            (2) sit in exactly one partition (group) and (3) not mix a total/aggregate item
            with siblings. Only the group-level gecko:Aggregate flag is trusted; the item-level
            flag is global and polluted by titles from unrelated tables.

            :param scheme_uri: the table-scoped ConceptScheme to check membership against
            :param items: selected Measure/Dimension schema items; empty means the whole scheme
            :param member_class: prefixed class the scheme members must have
                                 (qb:DimensionProperty or qb:MeasureProperty)
            :param context: human-readable description of the aggregation, prefixed to errors
            :param noun: what the items are called in error messages ('codes'/'measures')
        """
        item_filter = f"FILTER (?item IN (<{'>, <'.join(sorted(str(i.uri) for i in items))}>))" if items else ''
        rows = self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX xkos: <http://rdf-vocabulary.ddialliance.org/xkos#>
            PREFIX gecko: <{GECKO}>
            SELECT ?item ?group ?label ?agg WHERE {{
                ?item skos:inScheme <{scheme_uri}> ;
                      a {member_class} .
                OPTIONAL {{
                    ?item xkos:isPartOf ?group .
                    ?group skos:inScheme <{scheme_uri}> .
                    OPTIONAL {{ ?group skos:prefLabel ?label }}
                    # the aggregate flag of the item's OWN group; an EXISTS outside this OPTIONAL would see
                    #  ?group unbound for ungrouped items and match any aggregate group in the graph
                    OPTIONAL {{ ?group a <{GECKO.Aggregate}> . BIND (true AS ?agg) }}
                }}
                {item_filter}
            }}
        """)
        if not rows:
            return  # scheme carries no memberships -> no partition metadata to enforce

        _TOP = '(top-level)'  # sentinel partition for items sitting directly under the scheme root
        group_of, group_label, aggregates = {}, {_TOP: _TOP}, set()
        for r in rows:
            item = uri_to_code(r['item']['value'])
            group = _optional_value(r, 'group') or _TOP
            group_of[item] = group
            label = _optional_value(r, 'label')
            group_label[group] = f"'{label}'" if label else _TOP
            if _optional_value(r, 'agg') == 'true':
                aggregates.add(item)

        selected = {str(i) for i in items} if items else set(group_of)

        if unknown := selected - set(group_of):
            raise PartitionCoherenceError(
                f"{context} is not allowed: {noun} {sorted(unknown)} are not part of the table's "
                f"{noun[:-1]} scheme, so their partition membership cannot be verified.")

        if len(groups_hit := {group_of[i] for i in selected}) > 1:
            per_group = {group_label[g]: sorted(i for i in selected if group_of[i] == g) for g in groups_hit}
            raise PartitionCoherenceError(
                f"{context} is not allowed: the selection spans multiple partitions, "
                f"which double-counts (every partition sums to the same whole).\n"
                f"Selected {noun} per partition: {per_group}\n"
                f"Rewrite the query to select {noun} from exactly one partition, "
                f"or the lone total {noun[:-1]}.")

        if len(selected) > 1 and (totals := selected & aggregates):
            raise PartitionCoherenceError(
                f"{context} is not allowed: total/aggregate {noun} {sorted(totals)} are selected "
                f"alongside other {noun}, which double-counts (a total already equals the sum of "
                f"its siblings).\nRewrite the query to select either the total on its own, or "
                f"only non-total {noun}.")

    def _validate_timegeo_coherence(self, table: Table, dimension: Dimension, codes: Set[Dimension]) -> None:
        """
            Same-class coherence rule for time/geo dimensions: every selected code (all of the
            table's codes for this dimension, when the selection is open) must carry the same
            gecko area/period class (all provinces, all years, all labour-market regions, etc.)

            Overlap WITHIN the derived period classes (rolling years VJ, reporting periods XX,
            4-year averages G4) is not detected and is considered an edge case.
        """
        code_filter = f"FILTER (?code IN (<{'>, <'.join(sorted(c.uri for c in codes))}>))" if codes else ''
        rows = self.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT ?code ?cls WHERE {{
                <{table.uri}> qb:dimension ?code .
                ?code skos:broader <{dimension.uri}> ;
                      a ?cls .
                FILTER (?cls IN ({', '.join(f'<{c}>' for c in self.timegeo_classes())}))
                {code_filter}
            }}
        """)
        if not rows:
            return  # codes carry no gecko level classes (pre-migration graph) -> nothing to enforce

        # Base classes (gecko:TimeDimension/GeoDimension) say nothing about the level, so only
        # the specific level classes are compared; a code typed with only a base is unclassified.
        bases = set(self.timegeo_classes().values())
        classes_of: Dict[str, Set[str]] = {}
        for r in rows:
            level_classes = classes_of.setdefault(uri_to_code(r['code']['value']), set())
            if r['cls']['value'] not in bases:
                level_classes.add(r['cls']['value'])

        selected = {str(c) for c in codes} if codes else set(classes_of)
        context = (f"Aggregating over dimension '{dimension}' of table '{table}' "
                   f"with {'an open () selection' if not codes else f'codes {sorted(selected)}'}")

        if unclassified := selected - {c for c, cls in classes_of.items() if cls}:
            raise PartitionCoherenceError(
                f"{context} is not allowed: codes {sorted(unclassified)} are not part of this "
                f"table's dimension or carry no known area/period level class, so their aggregation "
                f"level cannot be verified.")

        codes_per_level: Dict[frozenset, List[str]] = {}
        for c in selected:
            codes_per_level.setdefault(frozenset(classes_of[c]), []).append(c)

        if len(codes_per_level) > 1:
            per_level = dict(sorted(
                (' + '.join(sorted(self._prefix_gecko_uri(cls) for cls in level)), self._truncated_codes(level_codes))
                for level, level_codes in codes_per_level.items()))
            raise PartitionCoherenceError(
                f"{context} is not allowed: the selection mixes different area/period levels, "
                f"which double-counts (a coarser level already contains the finer ones, e.g. a "
                f"country total contains its provinces).\n"
                f"Selected codes per level: {per_level}\n"
                f"Rewrite the query to select codes of exactly one level "
                f"(e.g. only provinces, or only years).")

    @staticmethod
    def _truncated_codes(codes: List[str], limit: int = 10) -> List[str]:
        """Sorted codes, capped at `limit` so open selections over e.g. all municipalities stay readable."""
        codes = sorted(codes)
        return codes if len(codes) <= limit else codes[:limit] + [f'... (+{len(codes) - limit} more)']

    def validate_msr_unit_compatibility(self, measures: Set[Measure], allow_different_scaling: bool = True) -> Dict[
        str, Dict[str, str]]:
        """
            Validate whether an operation performed on the measures is allowed based on the measures' units.
            Throws an TypeError when the operation is now allowed due to a unit mismatch.

            :param measures: the set of measures with URIs to validate
            :param allow_different_scaling: allow different units with a different scaling unit (e.g. kilograms + grams).
                                            Requires checks on conversion multipliers as well. This is not implemented for
                                            SQL execution, but is available for S-expressions.
            :returns: dictionary containing unit information
        """
        query = (f"""
            PREFIX qudt: <http://qudt.org/schema/qudt/>
            PREFIX quantitykind: <http://qudt.org/vocab/quantitykind/>

            SELECT ?msr ?unit ?scalingOf ?multiplier ?is_dimensionless WHERE {{
                VALUES ?msr {{ <{'> <'.join([m.uri for m in measures])}> }}
                OPTIONAL {{
                    ?msr qudt:unit ?q_unit .
                    OPTIONAL {{
                        ?q_unit qudt:scalingOf ?scalingOf .
                    }}
                }}
                OPTIONAL {{ ?msr qudt:unitOfSystem ?cbs_unit . }}
                OPTIONAL {{ ?msr qudt:conversionMultiplier ?multiplier . }}
                BIND(COALESCE(?q_unit, ?cbs_unit) AS ?unit)
                BIND(IF(BOUND(?q_unit),
                    EXISTS {{
                        VALUES ?kind {{ quantitykind:Dimensionless quantitykind:DimensionlessRatio }}
                        ?q_unit qudt:hasQuantityKind ?kind .
                    }},
                    EXISTS {{
                        VALUES ?kind {{ quantitykind:Dimensionless quantitykind:DimensionlessRatio }}
                        ?cbs_unit qudt:hasQuantityKind ?kind .
                    }}
                ) AS ?is_dimensionless)
                FILTER (BOUND(?unit) || BOUND(?multiplier))
            }}
        """)

        res = self.select(query)
        measure_units = {uri_to_code(d['msr']['value']): {
            'unit': (u := d.get('unit', {'value': None})['value']),
            # combination qudt:unit and qudt:unitOfSystem; see SPARQL above
            'scalingOf': d['scalingOf']['value'] if 'scalingOf' in d and d['scalingOf']['value'] not in ['None', None] else u,
            'multiplier': d.get('multiplier', {'value': None})['value'],
            'is_dimensionless': d.get('is_dimensionless', {'value': 'false'})['value'] == 'true',
        } for d in res}

        # 1. Check if any measure's unit dimension is dimensionless, except for qudt:Num, which should give a warning
        #    Units with a dimensionless vector (e.g. qudt:PERCENT) are non-aggregatable
        if len(measure_units) > 1 and any(
                d['is_dimensionless'] and d['unit'] != str(QUDT_UNIT.COUNT) for d in measure_units.values()):
            raise UnitCompatibilityError(
                f"Trying to aggregate over two or more measures with a dimensionless unit:\n{measure_units}"
            )

        # 2. Check if qudt:unit are present and equal for all measures
        #    If the units are equal for all measures, they are aggregatable
        units = [d['unit'] for d in measure_units.values()]
        if None not in units and len(set(units)) == 1:
            if str(QUDT_UNIT.COUNT) in units:
                logger.warning(f"Aggregating over measures with dimensionless qudt:Count as unit. "
                               f"This could lead to improper aggregations. Measures and their units:\n"
                               f"{measure_units}")
            # Check if conversion multipliers are equal when scaling is not allowed
            if not allow_different_scaling:
                multipliers = [d['multiplier'] for d in measure_units.values()]
                if len(set(multipliers)) > 1:
                    raise UnitCompatibilityError(
                        f"Trying to aggregate over multiple measures with different conversion multipliers:\n"
                        f"{measure_units}"
                    )
            return measure_units

        # 3. If not, check if the scaling unit (qudt:scalingOf) is equal for all measures
        #    If qudt:scalingOf is equal for all measures, they are aggregatable
        # FIXME: the qudt:scalingOf for these units themselves must be applied first in order to make them aggregatable
        scales = [d['scalingOf'] for d in measure_units.values()]
        if allow_different_scaling and None not in scales and len(set(scales)) == 1:
            return measure_units

        # Too bad. The measures are incompatible
        raise UnitCompatibilityError(f"Trying to aggregate over measures with a different unit: {measure_units}")
