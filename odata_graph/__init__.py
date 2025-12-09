import config
from odata_graph.sparql_controller import SparqlEngine

engine = SparqlEngine(local=config.LOCAL_GRAPH)
