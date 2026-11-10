import regex as re
from SPARQLWrapper import SPARQLWrapper, JSON, TURTLE, DIGEST

import config
from logs import logging
from odata_graph.sparql_controller import SparqlEngine

logger = logging.getLogger(__name__)


class VirtuosoEngine(SparqlEngine):
    def __init__(self):
        self.DEFAULT_ENDPOINT = f"{config.GRAPH_DB_HOST}/sparql"
        self.DEFAULT_INSERT_ENDPOINT = f"{config.GRAPH_DB_HOST}/sparql-auth"
        self._http_auth = DIGEST

        self.sparql = SPARQLWrapper(self.DEFAULT_ENDPOINT)
        self.sparql.setCredentials(config.GRAPH_DB_USERNAME, config.GRAPH_DB_PASSWORD)

        # Check whether the credentials are configured correctly. Raises an exception if not
        self._configure_query_request('GET', JSON)
        self.sparql.setQuery("ASK WHERE { ?s a ?o }")
        self.sparql.query()

        self.local = False
        logger.info("SPARQL SparqlEngine is running from Virtuoso!")

    def graph_for(self, name: str) -> str:
        # The Virtuoso bootstrap consolidates every sub-graph into the single
        # language-scoped graph (config.GRAPH_DB_REPO doubles as the graph IRI),
        # so the logical name is irrelevant here.
        return config.GRAPH_DB_REPO

    def insert(self, query: str, log_uri: str, verbose: bool = False):
        query = self._scope_update_to_graph(query, config.GRAPH_DB_REPO)
        super().insert(query, log_uri, verbose)

    @staticmethod
    def _scope_update_to_graph(query: str, graph: str) -> str:
        """
            Wrap the body of an INSERT DATA / INSERT { } WHERE / DELETE { } WHERE in
            GRAPH <graph> { ... } so Virtuoso writes land in the target named graph.
            Leaves queries that already contain GRAPH untouched.
        """
        if re.search(r"\bGRAPH\b", query, flags=re.IGNORECASE):
            return query
        # INSERT DATA { ... }  or  INSERT { ... } WHERE  or  DELETE { ... } WHERE
        pattern = r"\b(INSERT\s+DATA|INSERT|DELETE)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}"

        def _wrap(m):
            return f"{m.group(1)} {{ GRAPH <{graph}> {{{m.group(2)}}} }}"

        return re.sub(pattern, _wrap, query, count=1, flags=re.IGNORECASE | re.DOTALL)

    def _configure_query_request(self, method: str, return_format: str):
        # clearParameter prevents default-graph-uri values from accumulating across calls,
        # since addDefaultGraph appends rather than replaces.
        self.sparql.clearParameter("default-graph-uri")
        self.sparql.addDefaultGraph(config.GRAPH_DB_REPO)
        super()._configure_query_request(method, return_format)

        # Force Virtuoso to return a MIME type SPARQLWrapper can parse
        self.sparql.customHttpHeaders.pop("Accept", None)
        if return_format == JSON:
            self.sparql.addCustomHttpHeader("Accept", "application/sparql-results+json")
        elif return_format == TURTLE:
            self.sparql.addCustomHttpHeader("Accept", "text/turtle")
