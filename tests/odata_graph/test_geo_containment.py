"""
Tests for the global geographic containment bootstrap (ensure_geo_containment): xkos partitive
edges between area codes, sourced from 70739ned (historical, defunct codes included) and the
yearly 'Gebieden in Nederland' classification matrices.
"""
import pytest

import odata_graph.odata_graph_generator as gen_module
from odata_graph.odata_graph_generator import TableGraphGenerator
from odata_graph.sparql_controller import XKOS
from s_expression.schema_item import Dimension

BASE = "https://datasets.cbs.nl/odata/v1/CBS/"
DIM = Dimension.rdf_ns

RESPONSES = {
    # catalog: one pre-2013 metadata-only stub + one OData4-era yearly table
    f"{BASE}Datasets?$filter=startswith(Title,'Gebieden in Nederland')": [
        {'Identifier': '37144gin', 'Title': 'Gebieden in Nederland 2000'},
        {'Identifier': '99997GIN', 'Title': 'Gebieden in Nederland 2025'},
    ],
    # historical source: province-name -> PV-code mapping + per-code province rows
    f"{BASE}70739ned/RegioSCodes?$filter=startswith(Identifier,'PV')": [
        {'Identifier': 'PV21', 'Title': 'Fryslân (PV)'},
        {'Identifier': 'PV27', 'Title': 'Noord-Holland (PV)'},
    ],
    f"{BASE}70739ned/Observations?$filter=Measure eq 'PV0000_1'": [
        {'RegioS': 'GM9999', 'StringValue': 'Fryslân                       '},   # defunct municipality
        {'RegioS': 'GM0363', 'StringValue': 'Noord-Holland                 '},
        {'RegioS': 'GM0088', 'StringValue': 'Fryslân    '},
        {'RegioS': 'GM7777', 'StringValue': 'Atlantis   '},                      # unmappable -> skipped
        {'RegioS': 'LD01',   'StringValue': None},                               # no province -> skipped
    ],
    # the pre-2013 stub exposes no Observations and must be skipped without error
    f"{BASE}37144gin": [{'name': 'MeasureCodes'}, {'name': 'GemeentenCodes'}],
    # the yearly matrix table
    f"{BASE}99997GIN": [{'name': 'MeasureCodes'}, {'name': 'Observations'}],
    f"{BASE}99997GIN/MeasureCodes": [
        {'Identifier': 'GM000B',   'Title': 'Code'},   # the municipality's own code -> self-skip
        {'Identifier': 'GM000C_1', 'Title': 'Naam'},   # name column -> ignored
        {'Identifier': 'PV0001',   'Title': 'Code'},
        {'Identifier': 'LD0001',   'Title': 'Code'},
        {'Identifier': 'AM0001',   'Title': 'Code'},
    ],
    f"{BASE}99997GIN/Observations": [
        {'RegioS': 'GM0088', 'Measure': 'GM000B',   'StringValue': 'GM0088    '},
        {'RegioS': 'GM0088', 'Measure': 'GM000C_1', 'StringValue': 'Schiermonnikoog'},
        {'RegioS': 'GM0088', 'Measure': 'PV0001',   'StringValue': 'PV21  '},
        {'RegioS': 'GM0088', 'Measure': 'LD0001',   'StringValue': 'LD01  '},
        {'RegioS': 'GM0088', 'Measure': 'AM0001',   'StringValue': 'AM02  '},
        {'RegioS': 'GM0088', 'Measure': 'LD0001',   'StringValue': '1  '},  # bare numeric -> dropped
    ],
}


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return {'value': self._payload}


@pytest.fixture(scope='module')
def containment() -> TableGraphGenerator:
    """Builds the containment layer from the stubbed HTTP responses into an ISOLATED empty
    engine: the (regenerated) ut graph ships with the real containment layer, which would both
    short-circuit ensure_geo_containment's presence probe and pollute the exact-set assertions
    below with real municipalities."""
    from rdflib import ConjunctiveGraph
    from odata_graph.local_engine import LocalEngine

    engine = LocalEngine.__new__(LocalEngine)  # bypass __init__: no graph file to load
    engine.graph = ConjunctiveGraph(identifier='http://ut-isolated', store='Oxigraph')
    engine.local = True
    # Seed the QUDT marker: its source ttl is not checked into the repo; the gecko TBox (which
    #  IS on disk) loads normally and is all the generator start-up needs on an empty graph.
    engine.insert_raw({'qudt': '<http://qudt.org/schema/qudt/Unit> '
                               '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> '
                               '<http://www.w3.org/2002/07/owl#Class> .\n'}, 'qudt')

    original_get = gen_module._SESSION.get
    gen_module._SESSION.get = lambda url, *a, **kw: _FakeResponse(RESPONSES[url])
    try:
        gen = TableGraphGenerator(engine=engine)
        gen.ensure_geo_containment()
    finally:
        gen_module._SESSION.get = original_get
    return gen


def _is_part_of(engine, subj: str) -> set:
    res = engine.select(f"SELECT ?o WHERE {{ <{DIM.term(subj)}> <{XKOS.isPartOf}> ?o }}")
    return {r['o']['value'].rsplit('/', 1)[-1] for r in res}


def test_municipality_containment_edges(containment):
    """Municipalities link to every parent classification, values stripped of code padding."""
    assert _is_part_of(containment.engine, 'GM0088') == {'PV21', 'LD01', 'AM02'}


def test_chain_derived_up_to_country(containment):
    assert 'LD01' in _is_part_of(containment.engine, 'PV21')
    assert 'NL01' in _is_part_of(containment.engine, 'LD01')


def test_defunct_codes_covered_via_historical_source(containment):
    """GM9999 exists only in the 1830-overview: the province NAME maps onto its PV code."""
    assert 'PV21' in _is_part_of(containment.engine, 'GM9999')
    # unmappable province names are skipped, not guessed
    assert not _is_part_of(containment.engine, 'GM7777')


def test_inverse_has_part_edges(containment):
    res = containment.engine.select(f"SELECT ?m WHERE {{ <{DIM.term('PV21')}> <{XKOS.hasPart}> ?m }}")
    members = {r['m']['value'].rsplit('/', 1)[-1] for r in res}
    assert {'GM0088', 'GM9999'} <= members


def test_no_self_edges(containment):
    """The municipality's own-code measure (GM000B) must not create GM -> GM edges."""
    assert 'GM0088' not in _is_part_of(containment.engine, 'GM0088')


def test_friesland_expansion_query(containment):
    """The motivating case: expand 'municipalities of Friesland' from the graph."""
    res = containment.engine.select(f"""
        PREFIX xkos: <http://rdf-vocabulary.ddialliance.org/xkos#>
        PREFIX gecko: <https://vocabs.cbs.nl/def/gecko#>
        SELECT ?gm WHERE {{ ?gm xkos:isPartOf <{DIM.term('PV21')}> .
                            FILTER (STRSTARTS(STR(?gm), "{DIM}GM")) }}
    """)
    assert {r['gm']['value'].rsplit('/', 1)[-1] for r in res} == {'GM0088', 'GM9999'}


def test_real_ut_graph_ships_containment_layer(sparql_engine):
    """The regenerated ut fixture carries the real containment layer (so the generator's
    presence probe correctly skips a re-bootstrap against the live API)."""
    assert sparql_engine.select(f"""
        SELECT ?s WHERE {{
            ?s <{XKOS.isPartOf}> ?pv .
            FILTER (STRSTARTS(STR(?pv), "{DIM}PV"))
        }} LIMIT 1
    """)


def test_bootstrap_is_idempotent_and_offline_when_loaded(containment):
    """A second run must detect the layer via the probe and perform zero HTTP calls."""
    def _no_http(url, *a, **kw):
        raise AssertionError(f"unexpected HTTP call: {url}")
    original_get = gen_module._SESSION.get
    gen_module._SESSION.get = _no_http
    try:
        containment.ensure_geo_containment()  # must return on the probe alone
    finally:
        gen_module._SESSION.get = original_get
