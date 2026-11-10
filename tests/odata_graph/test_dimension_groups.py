"""
Unit tests for the group-scheme (partition) modelling in the graph generator.

Four things are covered:
  1. `_build_group_scheme` emits a table-scoped SKOS ConceptScheme with XKOS partitive
     relations, keeping top-level facets as plain skos:topConcepts (they do not aggregate into
     the scheme) and only nested sub-groups as xkos:isPartOf/hasPart.
  2. `_normalize_odata3_groups` reshapes an EN/OData3 CategoryGroups slice into the OData4
     <Dimension>Groups column shape (int ids stringified), and `_build_group_scheme` wraps the
     (common) multi-root facet layout under a synthetic root, keeping one scheme semantics.
  3. Measure groups reuse the same builder in the onderwerp namespace, anchored on the table
     node, with the totals-only aggregate pattern ('waarde' groups are NOT totals).
  4. The concept cache query stays scoped to the concept namespace, so on a single-graph backend
     (Virtuoso) it does not ingest the dimension-group concept nodes as label concepts.
"""
import pandas as pd
from rdflib import Graph, RDF, SKOS, QB, DCTERMS as DCT

from odata_graph.odata_graph_generator import TableGraphGenerator, _CONCEPT_NS, TOTAL_PATTERN
from odata_graph.sparql_controller import XKOS, GECKO
from s_expression.schema_item import Table, Measure, Dimension

# A dimension with a Totaal facet, two orthogonal facets, and one nested sub-group under a facet.
GROUPS = pd.DataFrame([
    {'Id': '0', 'Title': 'BestemmingEnSeizoen', 'ParentId': None},   # root -> ConceptScheme
    {'Id': '1', 'Title': 'Totaal',              'ParentId': '0'},    # facet, grand total
    {'Id': '2', 'Title': 'Bestemming',          'ParentId': '0'},    # facet (partition A)
    {'Id': '3', 'Title': 'Seizoen',             'ParentId': '0'},    # facet (partition B)
    {'Id': '4', 'Title': 'Binnenland',          'ParentId': '2'},    # nested sub-group under Bestemming
], dtype=str)

TABLE_URI = Table.rdf_ns.term('85302NED')
DIM_URI = Dimension.rdf_ns.term('BestemmingEnSeizoen')
SCHEME = Dimension.rdf_ns.term('85302NED/BestemmingEnSeizoen')
G_TOTAAL = Dimension.rdf_ns.term('85302NED/g1')
G_BESTEMMING = Dimension.rdf_ns.term('85302NED/g2')
G_SEIZOEN = Dimension.rdf_ns.term('85302NED/g3')
G_BINNENLAND = Dimension.rdf_ns.term('85302NED/g4')


def _build_graph() -> Graph:
    triples, scheme_uri, _ = TableGraphGenerator._build_group_scheme(GROUPS, DIM_URI, TABLE_URI)
    assert str(scheme_uri) == str(SCHEME)
    g = Graph()
    g.parse(data=triples, format='nt')
    return g


def test_scheme_is_table_scoped_codelist():
    """The root group becomes a table-scoped ConceptScheme linked from the dimension via qb:codeList."""
    g = _build_graph()
    assert (SCHEME, RDF.type, SKOS.ConceptScheme) in g
    assert (SCHEME, DCT.isPartOf, TABLE_URI) in g
    assert (DIM_URI, QB.codeList, SCHEME) in g


def test_facets_are_top_concepts_not_partitive():
    """Top-level facets partition the whole independently, so they are top concepts of the scheme
    and must NOT be linked to it with a partitive (XKOS) relation."""
    g = _build_graph()
    for facet in (G_TOTAAL, G_BESTEMMING, G_SEIZOEN):
        assert (facet, RDF.type, SKOS.Concept) in g
        assert (facet, SKOS.topConceptOf, SCHEME) in g
        assert (SCHEME, SKOS.hasTopConcept, facet) in g
        assert (facet, SKOS.inScheme, SCHEME) in g
    # The scheme must not aggregate its facets: no xkos partitive edge to/from the scheme.
    assert (SCHEME, XKOS.hasPart, None) not in g
    assert (None, XKOS.isPartOf, SCHEME) not in g


def test_total_facet_is_marked_aggregate():
    """Only the 'Totaal' facet carries gecko:Aggregate; the ordinary facets do not."""
    g = _build_graph()
    assert (G_TOTAAL, RDF.type, GECKO.Aggregate) in g
    assert (G_BESTEMMING, RDF.type, GECKO.Aggregate) not in g
    assert (G_SEIZOEN, RDF.type, GECKO.Aggregate) not in g


def test_nested_subgroup_is_partitive():
    """A nested sub-group aggregates into its parent facet: partitive XKOS edges plus the plain
    SKOS hierarchy (kept so traversal works without a reasoner)."""
    g = _build_graph()
    assert (G_BINNENLAND, XKOS.isPartOf, G_BESTEMMING) in g
    assert (G_BESTEMMING, XKOS.hasPart, G_BINNENLAND) in g
    assert (G_BINNENLAND, SKOS.broader, G_BESTEMMING) in g
    assert (G_BESTEMMING, SKOS.narrower, G_BINNENLAND) in g
    # A nested sub-group is not a top concept of the scheme.
    assert (G_BINNENLAND, SKOS.topConceptOf, SCHEME) not in g


# == OData3 (EN) CategoryGroups normalization ==

# 84407ENG Countries-style: two facet roots (ParentID null) without a wrapper root, int ids.
EN_CATEGORY_GROUPS = [
    {'ID': 4, 'DimensionKey': 'Countries', 'Title': 'Total',                  'Description': '', 'ParentID': None},
    {'ID': 5, 'DimensionKey': 'Countries', 'Title': 'World',                  'Description': '', 'ParentID': 4},
    {'ID': 6, 'DimensionKey': 'Countries', 'Title': 'Continents',             'Description': '', 'ParentID': 4},
    {'ID': 7, 'DimensionKey': 'Countries', 'Title': 'Countries by continent', 'Description': '', 'ParentID': None},
    {'ID': 8, 'DimensionKey': 'Countries', 'Title': 'Europe',                 'Description': '', 'ParentID': 7},
]

EN_TABLE_URI = Table.rdf_ns.term('84407FAKE')
EN_DIM_URI = Dimension.rdf_ns.term('Countries')
EN_SCHEME = Dimension.rdf_ns.term('84407FAKE/Countries')
G_TOTAL = Dimension.rdf_ns.term('84407FAKE/g4')
G_WORLD = Dimension.rdf_ns.term('84407FAKE/g5')
G_CONTINENTS = Dimension.rdf_ns.term('84407FAKE/g6')
G_BY_CONTINENT = Dimension.rdf_ns.term('84407FAKE/g7')
G_EUROPE = Dimension.rdf_ns.term('84407FAKE/g8')


def _build_en_graph():
    groups = TableGraphGenerator._normalize_odata3_groups(EN_CATEGORY_GROUPS)
    triples, scheme_uri, mapping = TableGraphGenerator._build_group_scheme(
        groups, EN_DIM_URI, EN_TABLE_URI, scheme_title='Countries')
    g = Graph()
    g.parse(data=triples, format='nt')
    return g, scheme_uri, mapping


def test_multi_root_facets_become_top_concepts():
    """Without a wrapper root, the facet roots are re-pointed under a synthetic scheme root and
    end up as top concepts, with no partitive (XKOS) edge to/from the scheme — same semantics
    as the OData4 single-root shape."""
    g, scheme_uri, _ = _build_en_graph()
    assert str(scheme_uri) == str(EN_SCHEME)
    assert (EN_SCHEME, RDF.type, SKOS.ConceptScheme) in g
    assert str(g.value(EN_SCHEME, SKOS.prefLabel)) == 'Countries'   # labeled after the dimension
    for facet in (G_TOTAL, G_BY_CONTINENT):
        assert (facet, SKOS.topConceptOf, EN_SCHEME) in g
        assert (EN_SCHEME, SKOS.hasTopConcept, facet) in g
    assert (EN_SCHEME, XKOS.hasPart, None) not in g
    assert (None, XKOS.isPartOf, EN_SCHEME) not in g


def test_multi_root_nested_groups_stay_partitive():
    """Nesting below the facet roots keeps the partitive XKOS edges the coherence validator reads."""
    g, _, _ = _build_en_graph()
    for child, parent in ((G_WORLD, G_TOTAL), (G_CONTINENTS, G_TOTAL), (G_EUROPE, G_BY_CONTINENT)):
        assert (child, XKOS.isPartOf, parent) in g
        assert (parent, XKOS.hasPart, child) in g
        assert (child, SKOS.broader, parent) in g
        assert (child, SKOS.topConceptOf, EN_SCHEME) not in g


def test_multi_root_total_facet_marked_aggregate():
    g, _, _ = _build_en_graph()
    assert (G_TOTAL, RDF.type, GECKO.Aggregate) in g
    assert (G_BY_CONTINENT, RDF.type, GECKO.Aggregate) not in g


def test_normalize_stringifies_int_ids():
    """OData3 ids are ints (nulls would coerce a dtype=str frame to float strings like '4.0');
    the mapping must be keyed by clean stringified ids so code CategoryGroupID lookups resolve."""
    _, _, mapping = _build_en_graph()
    assert {'4', '5', '6', '7', '8'} <= set(mapping)
    assert not any(k.endswith('.0') for k in mapping)


def test_single_root_passes_through_unchanged():
    """A wrapper root in the source (SITC-style) needs no synthesis: it becomes the scheme."""
    single = [
        {'ID': 0, 'DimensionKey': 'SITC', 'Title': 'SITC',     'Description': '', 'ParentID': None},
        {'ID': 1, 'DimensionKey': 'SITC', 'Title': 'Total',    'Description': '', 'ParentID': 0},
        {'ID': 2, 'DimensionKey': 'SITC', 'Title': 'Sections', 'Description': '', 'ParentID': 0},
    ]
    groups = TableGraphGenerator._normalize_odata3_groups(single)
    triples, scheme_uri, mapping = TableGraphGenerator._build_group_scheme(
        groups, Dimension.rdf_ns.term('SITC'), EN_TABLE_URI, scheme_title='ignored: root exists')
    g = Graph()
    g.parse(data=triples, format='nt')
    scheme = Dimension.rdf_ns.term('84407FAKE/SITC')
    assert str(scheme_uri) == str(scheme) and mapping['0'] == scheme
    assert str(g.value(scheme, SKOS.prefLabel)) == 'SITC', "the source root's own title must win"
    for gid in ('1', '2'):
        assert (Dimension.rdf_ns.term(f'84407FAKE/g{gid}'), SKOS.topConceptOf, scheme) in g


def test_no_root_group_returns_empty():
    """A groups response without a root (no null ParentId) yields no scheme and no triples."""
    rootless = pd.DataFrame([{'Id': '1', 'Title': 'x', 'ParentId': '9'}], dtype=str)
    triples, scheme_uri, mapping = TableGraphGenerator._build_group_scheme(rootless, DIM_URI, TABLE_URI)
    assert triples == "" and scheme_uri is None and mapping == {}


# == Measure-group schemes (same builder, onderwerp namespace, anchored on the table) ==

# 85302NED MeasureGroups-style: code-like ids, several facet roots, no wrapper. The 'Waarde in
# constante prijzen' group exercises the totals-only pattern: 'waarde' is NOT a total for measures.
MEASURE_GROUPS = pd.DataFrame([
    {'Id': 'D000084',   'Title': 'Deelname',                    'Description': 'Vakantieparticipatie.', 'ParentId': None},
    {'Id': 'M005005_3', 'Title': 'Totaal vakanties',            'Description': '',   'ParentId': None},
    {'Id': 'M009999_1', 'Title': 'Waarde in constante prijzen', 'Description': None, 'ParentId': None},
    {'Id': 'M005005_9', 'Title': 'Binnenland',                  'Description': '',   'ParentId': 'M005005_3'},
], dtype=str)

MSR_TABLE_URI = Table.rdf_ns.term('85302FAKE')
MSR_SCHEME = Measure.rdf_ns.term('85302FAKE/Measure')
MG_DEELNAME = Measure.rdf_ns.term('85302FAKE/gD000084')
MG_TOTAAL = Measure.rdf_ns.term('85302FAKE/gM005005_3')
MG_WAARDE = Measure.rdf_ns.term('85302FAKE/gM009999_1')
MG_BINNENLAND = Measure.rdf_ns.term('85302FAKE/gM005005_9')


def _build_measure_graph():
    triples, scheme_uri, mapping = TableGraphGenerator._build_group_scheme(
        MEASURE_GROUPS, MSR_TABLE_URI, MSR_TABLE_URI, ns=Measure.rdf_ns, scheme_id='Measure',
        scheme_title='Measure', aggregate_pattern=TOTAL_PATTERN)
    g = Graph()
    g.parse(data=triples, format='nt')
    return g, scheme_uri, mapping


def test_measure_scheme_minted_in_onderwerp_ns_anchored_on_table():
    """The measure scheme lives in the onderwerp namespace (prefix-scoped for Virtuoso) and is
    linked from the TABLE node via qb:codeList — measures have no dimension node to anchor on."""
    g, scheme_uri, _ = _build_measure_graph()
    assert str(scheme_uri) == str(MSR_SCHEME)
    assert (MSR_SCHEME, RDF.type, SKOS.ConceptScheme) in g
    assert (MSR_TABLE_URI, QB.codeList, MSR_SCHEME) in g
    assert (MSR_SCHEME, DCT.isPartOf, MSR_TABLE_URI) in g


def test_measure_facet_roots_wrapped_and_group_description_kept():
    """The rootless facet layout gets the synthetic wrapper (facets = top concepts, no XKOS to
    the scheme) and group descriptions survive as skos:definition on the group node."""
    g, _, _ = _build_measure_graph()
    for facet in (MG_DEELNAME, MG_TOTAAL, MG_WAARDE):
        assert (facet, SKOS.topConceptOf, MSR_SCHEME) in g
    assert (None, XKOS.isPartOf, MSR_SCHEME) not in g
    assert str(g.value(MG_DEELNAME, SKOS.definition)) == 'Vakantieparticipatie.'
    assert (MG_BINNENLAND, XKOS.isPartOf, MG_TOTAAL) in g   # nested sub-group stays partitive


def test_measure_total_pattern_excludes_waarde():
    """Only genuine totals are gecko:Aggregate for measures: 'waarde' marks value-vs-volume
    groupings, and flagging them would false-block legitimate measure aggregations."""
    g, _, _ = _build_measure_graph()
    assert (MG_TOTAAL, RDF.type, GECKO.Aggregate) in g
    assert (MG_WAARDE, RDF.type, GECKO.Aggregate) not in g
    assert (MG_DEELNAME, RDF.type, GECKO.Aggregate) not in g


def test_concept_cache_query_ignores_group_concepts_on_merged_graph():
    """Simulate the Virtuoso single-graph store: a label concept and a dimension-group concept,
    both `a skos:Concept` with the same prefLabel, live in ONE graph. The namespace-scoped cache
    query must return only the label concept, never the group node."""
    g = Graph()
    label_concept = _CONCEPT_NS + "C00000001"
    group_concept = str(G_TOTAAL)  # in the dimensie namespace
    g.update(f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        INSERT DATA {{
            <{label_concept}> a skos:Concept ; skos:prefLabel "Totaal"@nl .
            <{group_concept}> a skos:Concept ; skos:prefLabel "Totaal"@nl .
        }}
    """)

    where = """
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?s WHERE {
            ?s a skos:Concept .
            ?s skos:prefLabel ?label .
            %s
        }
    """
    # Control: without the filter both concepts match (this is the Virtuoso pollution).
    unfiltered = {str(r[0]) for r in g.query(where % "")}
    assert unfiltered == {label_concept, group_concept}

    # Fixed: the namespace filter keeps only the genuine label concept.
    filtered = {str(r[0]) for r in g.query(where % f'FILTER(STRSTARTS(STR(?s), "{_CONCEPT_NS}"))')}
    assert filtered == {label_concept}
