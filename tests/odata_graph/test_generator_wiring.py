"""
End-to-end tests for insert_dimensions wiring, with the OData HTTP session stubbed out:
  - geo/time codes are typed with their gecko area/period class (resolved from the prefix/infix
    maps loaded from the TBox), dual-emitted with the legacy string-literal types;
  - regular dimensions get the table-scoped group scheme with XKOS partitive code->group edges.
"""
import pandas as pd
import pytest

import odata_graph.odata_graph_generator as gen_module
from odata_graph.odata_graph_generator import TableGraphGenerator
from odata_graph.sparql_controller import GECKO, XKOS
from s_expression.schema_item import Table, Dimension

TABLE = str(Table.rdf_ns.term('99999FAKE'))
DIM = Dimension.rdf_ns

RESPONSES = {
    f"{TABLE}/RegioSCodes": [
        {'Key': 'NL01',   'Title': 'Nederland',     'Description': ''},
        {'Key': 'PV27',   'Title': 'Noord-Holland', 'Description': ''},
        {'Key': 'GM0363', 'Title': 'Amsterdam',     'Description': ''},
        {'Key': 'ZZ99',   'Title': 'Vreemd gebied', 'Description': ''},
    ],
    f"{TABLE}/PeriodenCodes": [
        {'Key': '2022JJ00', 'Title': '2022',                  'Description': ''},
        {'Key': '2024VJ06', 'Title': 'Juli 2023 - juni 2024', 'Description': ''},
        {'Key': '2021W13',  'Title': 'Week 13 2021',          'Description': ''},
        {'Key': '19950101', 'Title': '1 januari 1995',        'Description': ''},
        {'Key': '2020G400', 'Title': '2017 t/m 2020',         'Description': ''},
        {'Key': 'MW2022',   'Title': 'Vreemde periode',       'Description': ''},
    ],
    f"{TABLE}/KenmerkenGroups": [
        {'Id': '0', 'Index': 0, 'Title': 'Kenmerken', 'Description': '', 'ParentId': None},
        {'Id': '1', 'Index': 1, 'Title': 'Totaal',    'Description': '', 'ParentId': '0'},
        {'Id': '2', 'Index': 2, 'Title': 'Categorie', 'Description': '', 'ParentId': '0'},
    ],
    f"{TABLE}/KenmerkenCodes": [
        {'Key': 'T001', 'Title': 'Totaal kenmerken', 'Description': '', 'DimensionGroupId': '1'},
        {'Key': 'A001', 'Title': 'Categorie A',      'Description': '', 'DimensionGroupId': '2'},
        {'Key': 'A002', 'Title': 'Categorie B',      'Description': '', 'DimensionGroupId': '2'},
        {'Key': 'R000', 'Title': 'Los kenmerk',      'Description': '', 'DimensionGroupId': '0'},
    ],
}

DIMENSIONS = pd.DataFrame([
    {'Identifier': 'RegioS',    'Title': "Regio's",  'Description': '', 'Kind': 'GeoDimension'},
    {'Identifier': 'Perioden',  'Title': 'Perioden', 'Description': '', 'Kind': 'TimeDimension'},
    {'Identifier': 'Kenmerken', 'Title': 'Kenmerken', 'Description': '', 'Kind': 'Dimension'},
], dtype=str)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return {'value': self._payload}


@pytest.fixture(scope='module')
def generator(sparql_engine) -> TableGraphGenerator:
    """Run insert_dimensions for a fake table against the in-memory ut graph, with HTTP stubbed."""
    original_get = gen_module._SESSION.get
    gen_module._SESSION.get = lambda url, *a, **kw: _FakeResponse(RESPONSES[url])
    try:
        gen = TableGraphGenerator(engine=sparql_engine)
        # RegioSGroups/PeriodenGroups are listed as available endpoints but have NO stubbed
        #  response: time/geo dims must skip the group fetch entirely (a wrongful fetch raises
        #  KeyError, which lands in failed_tables and trips the assert below).
        gen.insert_dimensions(DIMENSIONS, Table.rdf_ns.term('99999FAKE'),
                              group_endpoints=frozenset({'KenmerkenGroups', 'RegioSGroups',
                                                         'PeriodenGroups'}))
    finally:
        gen_module._SESSION.get = original_get
    assert not gen.failed_tables, "insert_dimensions swallowed an exception into failed_tables"
    return gen


def _types_of(engine, uri) -> set:
    return {r['t']['value'] for r in engine.select(f"SELECT ?t WHERE {{ <{uri}> a ?t }}")}


# == Classifier units ==

@pytest.mark.parametrize('code, cls', [
    ('NL01', 'Country'), ('LD01', 'Region'), ('PV27', 'Province'), ('GM0363', 'Municipality'),
    ('WK036300', 'District'), ('BU03630001', 'Neighbourhood'),
    ('G0400', 'G4'), ('G4000', 'G40'),      # 3-char prefixes must win the longest-match
    ('CR13', 'CoropArea'), ('ZZ99', 'UnknownRegion'),
])
def test_classify_geo_code(generator, code, cls):
    assert generator.classify_geo_code(code) == str(GECKO.term(cls))


@pytest.mark.parametrize('code, cls', [
    ('2022JJ00', 'Year'), ('2022HJ01', 'HalfYear'), ('2022KW01', 'Quarter'),
    ('2022MM01', 'Month'), ('2021W13', 'Week'), ('19950101', 'Date'),
    ('2020G400', 'FourYearAverage'), ('2024VJ06', 'RollingYear'), ('2022X000', 'ReferenceDayTotal'),
])
def test_classify_time_code(generator, code, cls):
    assert generator.classify_time_code(code) == str(GECKO.term(cls))


@pytest.mark.parametrize('code', ['MW2022', '2022', 'JJ2022', ''])
def test_classify_time_code_unrecognized_shape(generator, code):
    assert generator.classify_time_code(code) is None


# == Emission: geo/time typing (dual-emit) ==

def test_geo_dimension_node_dual_typed(generator, sparql_engine):
    types = _types_of(sparql_engine, DIM.term('RegioS'))
    assert 'GeoDimension' in types                    # legacy string literal
    assert str(GECKO.GeoDimension) in types           # new gecko URI class


def test_geo_codes_get_specific_area_class(generator, sparql_engine):
    for code, cls in (('NL01', 'Country'), ('PV27', 'Province'),
                      ('GM0363', 'Municipality'), ('ZZ99', 'UnknownRegion')):
        types = _types_of(sparql_engine, DIM.term(code))
        assert str(GECKO.term(cls)) in types, f"{code} lacks gecko:{cls} ({types})"
        assert 'GeoDimension' in types                # legacy literal kept on codes too


def test_time_codes_get_specific_period_class(generator, sparql_engine):
    for code, cls in (('2022JJ00', 'Year'), ('2024VJ06', 'RollingYear'), ('2021W13', 'Week'),
                      ('19950101', 'Date'), ('2020G400', 'FourYearAverage'),
                      ('MW2022', 'TimeDimension')):   # unrecognized shape -> base class
        types = _types_of(sparql_engine, DIM.term(code))
        assert str(GECKO.term(cls)) in types, f"{code} lacks gecko:{cls} ({types})"


def test_scope_note_still_emitted(generator, sparql_engine):
    """The legacy 2-letter scopeNote must survive the dual-emit (other consumers read it)."""
    res = sparql_engine.select(f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?n WHERE {{ <{DIM.term('GM0363')}> skos:scopeNote ?n }}
    """)
    assert {r['n']['value'] for r in res} == {'GM'}


# == Emission: dimension-group scheme with XKOS partitive edges (code->group path) ==

def test_code_to_group_partitive_edges(generator, sparql_engine):
    scheme, g1, g2 = DIM.term('99999FAKE/Kenmerken'), DIM.term('99999FAKE/g1'), DIM.term('99999FAKE/g2')
    res = sparql_engine.select(f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX xkos: <{XKOS}>
        SELECT ?code WHERE {{ ?code xkos:isPartOf <{g2}> ; skos:broader <{g2}> ; skos:inScheme <{scheme}> }}
    """)
    assert {r['code']['value'] for r in res} == {str(DIM.term('A001')), str(DIM.term('A002'))}
    # the total code sits in the Totaal group, and that group is marked as an aggregate
    assert str(g1) in {r['g']['value'] for r in sparql_engine.select(
        f"SELECT ?g WHERE {{ <{DIM.term('T001')}> <http://rdf-vocabulary.ddialliance.org/xkos#isPartOf> ?g }}")}
    assert str(GECKO.Aggregate) in _types_of(sparql_engine, g1)


def test_time_geo_dimensions_get_no_partition_scheme(generator, sparql_engine):
    """Time/geo dims skip the group-scheme modeling: the coherence validator exempts them and
    their code lists are huge (~15k wijken/buurten codes -> ~75k useless membership triples)."""
    for dim in ('RegioS', 'Perioden'):
        res = sparql_engine.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            SELECT ?scheme WHERE {{ <{DIM.term(dim)}> qb:codeList ?scheme }}
        """)
        assert not [r for r in res if '99999FAKE' in r['scheme']['value']], f"{dim} got a scheme"


def test_code_directly_under_root_becomes_top_concept(generator, sparql_engine):
    scheme = DIM.term('99999FAKE/Kenmerken')
    res = sparql_engine.select(f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?s WHERE {{ <{DIM.term('R000')}> skos:topConceptOf ?s }}
    """)
    assert {r['s']['value'] for r in res} == {str(scheme)}


# == Emission: EN/OData3 CategoryGroups wiring ==

def test_odata3_en_group_wiring(sparql_engine, monkeypatch):
    """EN/OData3 wiring: the table-level CategoryGroups response is fetched once and sliced per
    dimension, the multi-root facets get wrapped under a synthetic scheme root, and codes attach
    to their group through the int-typed CategoryGroupID (whose nulls float-coerce the dtype=str
    column to '4.0')."""
    import config
    monkeypatch.setattr(config, 'LANGUAGE', 'en')

    table_uri = Table.rdf_ns.term('99998FAKE')
    responses = {
        f"{table_uri}/CategoryGroups": [
            {'ID': 4, 'DimensionKey': 'Countries', 'Title': 'Total',                  'Description': '', 'ParentID': None},
            {'ID': 6, 'DimensionKey': 'Countries', 'Title': 'Continents',             'Description': '', 'ParentID': 4},
            {'ID': 7, 'DimensionKey': 'Countries', 'Title': 'Countries by continent', 'Description': '', 'ParentID': None},
        ],
        # OData3 code lists live at /<DimensionKey>, without the OData4 'Codes' suffix
        f"{table_uri}/Countries": [
            {'Key': 'T1', 'Title': 'Total countries', 'Description': '', 'CategoryGroupID': 4},
            {'Key': 'EU', 'Title': 'Europe',          'Description': '', 'CategoryGroupID': 6},
            {'Key': 'XX', 'Title': 'Unattached',      'Description': '', 'CategoryGroupID': None},
        ],
        f"{table_uri}/Periods": [
            {'Key': '2022JJ00', 'Title': '2022', 'Description': '', 'Status': 'Definitief'},
        ],
    }
    dims = pd.DataFrame([
        {'Identifier': 'Countries', 'Title': 'Countries', 'Description': '', 'Kind': 'Dimension'},
        {'Identifier': 'Periods',   'Title': 'Periods',   'Description': '', 'Kind': 'TimeDimension'},
    ], dtype=str)

    original_get = gen_module._SESSION.get
    gen_module._SESSION.get = lambda url, *a, **kw: _FakeResponse(responses[url])
    try:
        gen = TableGraphGenerator(engine=sparql_engine)
        gen.insert_dimensions(dims, table_uri, group_endpoints=frozenset({'CategoryGroups'}))
    finally:
        gen_module._SESSION.get = original_get
    assert not gen.failed_tables, "EN insert_dimensions swallowed an exception into failed_tables"

    scheme = DIM.term('99998FAKE/Countries')
    g4, g6, g7 = DIM.term('99998FAKE/g4'), DIM.term('99998FAKE/g6'), DIM.term('99998FAKE/g7')

    # The facet roots became top concepts of the synthetic scheme.
    res = sparql_engine.select(f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?g WHERE {{ ?g skos:topConceptOf <{scheme}> }}
    """)
    assert {r['g']['value'] for r in res} >= {str(g4), str(g7)}

    # Codes attach to their group via the stringified CategoryGroupID ('4', not '4.0').
    res = sparql_engine.select(f"SELECT ?g WHERE {{ <{DIM.term('T1')}> <{XKOS.isPartOf}> ?g }}")
    assert str(g4) in {r['g']['value'] for r in res}
    res = sparql_engine.select(f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?g WHERE {{ <{DIM.term('EU')}> skos:broader ?g . FILTER(CONTAINS(STR(?g), '99998FAKE')) }}
    """)
    assert {r['g']['value'] for r in res} == {str(g6)}

    # A code with a null CategoryGroupID is simply left out of the scheme (NL behaviour kept).
    res = sparql_engine.select(f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?s WHERE {{ <{DIM.term('XX')}> skos:inScheme ?s }}
    """)
    assert not res

    # Time dims skip the group modelling on EN too (their hierarchy comes from geo/time logic).
    res = sparql_engine.select(f"""
        PREFIX qb: <http://purl.org/linked-data/cube#>
        SELECT ?s WHERE {{ <{DIM.term('Periods')}> qb:codeList ?s }}
    """)
    assert not [r for r in res if '99998FAKE' in r['s']['value']]


# == N-Triples literal escaping (regression: rdflib's .n3() emits multiline strings as
#    triple-quoted Turtle literals, which the insert_raw NT fast path rejects) ==

def test_nt_literal_escapes_hostile_strings():
    from odata_graph.odata_graph_generator import _nt_literal
    assert _nt_literal('regel1\nregel2', 'nl') == '"regel1\\nregel2"@nl'
    assert _nt_literal('zeg "hoi" \\ terug') == '"zeg \\"hoi\\" \\\\ terug"'
    assert _nt_literal('tab\there\r\n') == '"tab\\there\\r\\n"'


def test_hostile_literals_survive_the_nt_fast_path(sparql_engine):
    """CBS descriptions contain bare newlines (only \\r\\n was flattened historically); they
    must round-trip through insert_dimensions -> insert_raw without a ParserError."""
    hostile = pd.DataFrame([{
        'Identifier': 'Hostile', 'Title': 'Titel met "quotes"',
        'Description': '\nEerste regel.\nTweede regel met \\ backslash.', 'Kind': 'Dimension',
    }], dtype=str)
    responses = {f"{TABLE}/HostileCodes": [
        {'Key': 'H001', 'Title': 'Code met\nnieuwe regel', 'Description': 'a "b" c\nd'},
    ]}
    original_get = gen_module._SESSION.get
    gen_module._SESSION.get = lambda url, *a, **kw: _FakeResponse(responses[url])
    try:
        gen = TableGraphGenerator(engine=sparql_engine)
        gen.insert_dimensions(hostile, Table.rdf_ns.term('99999FAKE'))
    finally:
        gen_module._SESSION.get = original_get
    assert not gen.failed_tables, "hostile literals broke the NT fast path"

    # the label survives with its newline intact (concept-linked, like the old SPARQL path stored it)
    res = sparql_engine.select(f"""
        PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?label WHERE {{ <{DIM.term('H001')}> qb:concept/skos:prefLabel ?label }}
    """)
    assert {r['label']['value'] for r in res} == {'Code met\nnieuwe regel'}
