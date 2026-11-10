import os
from rdflib import ConjunctiveGraph, Namespace, SKOS, DCAT, QB, DCTERMS as DCT
from rdflib.term import URIRef
from time import time
from typing import Dict

import config
from logs import logging
from odata_graph.sparql_controller import SparqlEngine, QUDT, SCOT, XKOS, GECKO

logger = logging.getLogger(__name__)

# Prefixes to register on the graph so (re)serialization emits readable prefixes instead of
# auto-generated ns1/ns2. rdflib does not auto-bind these, and a freshly built graph parses none.
_NS_BINDINGS = {
    'skos': SKOS, 'dct': DCT, 'dcat': DCAT, 'qb': QB, 'sdo': Namespace("https://schema.org/"),
    'scot': SCOT, 'qudt': QUDT, 'xkos': XKOS, 'gecko': GECKO,
}


class LocalEngine(SparqlEngine):
    graph: ConjunctiveGraph

    def __init__(self):
        if not os.path.exists(config.GRAPH_FILE):
            raise FileNotFoundError(f"No local graph file found at location {config.GRAPH_FILE}.")

        t0 = time()
        logger.info(f"Local file found. Loading graph from {config.GRAPH_FILE}. "
                    f"Depending on size this can take a while.")
        self.graph = ConjunctiveGraph(identifier=f"http://{config.GRAPH_DB_REPO}", store="Oxigraph")
        self.graph.parse(config.GRAPH_FILE, format=config.GRAPH_FILE.split('.')[-1])
        for prefix, ns in _NS_BINDINGS.items():
            self.graph.bind(prefix, ns, override=True, replace=True)

        self.local = True
        logger.info(f"SPARQL SparqlEngine is running from local file! ({time() - t0} s)")

    def insert_raw(self, clauses: Dict[str, str], log_uri: str) -> None:
        """
            Fast path for bulk triple inserts: the fragments are N-Triples, so feed them
            straight into the store's parser instead of wrapping them in an INSERT DATA update —
            rdflib parses SPARQL updates with pyparsing at ~24 s per MB of query text, which
            profiling showed to be ~99% of the graph-generation wall time.

            :param clauses: {logical sub-graph name: raw N-Triples string to parse into it}
            :param log_uri: unused for the local graph engine
        """
        for name, triples in clauses.items():
            if triples:
                self.graph.get_context(URIRef(self.graph_for(name))).parse(data=triples, format='nt')
