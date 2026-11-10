import pytest

from odata_graph import engine


@pytest.fixture(scope='session')
def sparql_engine():
    """The unittest graph contains tables 80781ned, 84957NED and 85302NED."""
    yield engine
