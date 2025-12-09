import pytest

from odata_graph.sparql_controller import SparqlEngine


@pytest.fixture(scope='session')
def sparql_engine():
    """The unittest graph contains tables 80781ned, 84957NED and 85302NED."""
    engine = SparqlEngine(local=True)
    yield engine
