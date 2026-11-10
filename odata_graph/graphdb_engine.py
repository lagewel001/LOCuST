from SPARQLWrapper import SPARQLWrapper, JSON, BASIC

import config
from logs import logging
from odata_graph.sparql_controller import SparqlEngine

logger = logging.getLogger(__name__)


class GraphDBEngine(SparqlEngine):
    def __init__(self):
        self.DEFAULT_ENDPOINT = f"{config.GRAPH_DB_HOST}/repositories/{config.GRAPH_DB_REPO}"
        self.DEFAULT_INSERT_ENDPOINT = f"{self.DEFAULT_ENDPOINT}/statements"
        self._http_auth = BASIC

        self.sparql = SPARQLWrapper(self.DEFAULT_ENDPOINT)
        self.sparql.setCredentials(config.GRAPH_DB_USERNAME, config.GRAPH_DB_PASSWORD)

        # Check whether the credentials are configured correctly. Raises an exception if not
        self._configure_query_request('GET', JSON)
        self.sparql.setQuery("ASK WHERE { ?s a ?o }")
        self.sparql.query()

        self.local = False
        logger.info("SPARQL SparqlEngine is running from GraphDB!")
