import config
from odata_graph.graphdb_engine import GraphDBEngine
from odata_graph.local_engine import LocalEngine
from odata_graph.virtuoso_engine import VirtuosoEngine
from utils.custom_types import GraphEngineKind


match config.GRAPH_ENGINE:
    case GraphEngineKind.GRAPHDB:
        engine = GraphDBEngine()
    case GraphEngineKind.VIRTUOSO:
        engine = VirtuosoEngine()
    case _:
        engine = LocalEngine()
