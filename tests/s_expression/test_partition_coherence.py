"""
Tests for the partition-coherence validator: aggregations that collapse a dimension over an
incoherent code-set hard-block with PartitionCoherenceError instead of silently double-counting.

Uses 85302NED's BestemmingEnSeizoen, whose table-scoped code list has three partitions:
    Totaal (T001047, aggregate) | Bestemming (L008691, L999996) | Seizoen (A045878, A045879)
Every partition sums to the same whole (35 933 holidays in 2022), so mixing them double-counts.
"""
import pytest

from s_expression.parser import parse, eval
from s_expression.schema_item import Table, Measure, Dimension
from utils.custom_types import PartitionCoherenceError

TABLE = Table('85302NED')
DIM = Dimension('BestemmingEnSeizoen')


def _codes(*codes) -> frozenset:
    return frozenset(Dimension(c) for c in codes)


# == The verdict table, directly against the engine-level validator ==

@pytest.mark.parametrize('codes', [
    ('L008691', 'L999996'),   # complete Bestemming partition
    ('A045878', 'A045879'),   # complete Seizoen partition
    ('L008691',),             # subset of one partition
    ('T001047',),             # the lone total: plain retrieval of the aggregate is legitimate
])
def test_single_partition_allowed(sparql_engine, codes):
    sparql_engine.validate_partition_coherence(TABLE, DIM, _codes(*codes))


@pytest.mark.parametrize('codes', [
    ('L008691', 'L999996', 'T001047'),  # total alongside the partition it aggregates
    ('T001047', 'A045878'),             # total + a season leaf
    ('L008691', 'A045878'),             # cross-partition, no total involved
])
def test_incoherent_selection_blocked(sparql_engine, codes):
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_partition_coherence(TABLE, DIM, _codes(*codes))


def test_open_selection_blocked(sparql_engine):
    """Open () selects all codes and therefore spans every partition (and the total)."""
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_partition_coherence(TABLE, DIM, frozenset())


def test_unknown_code_blocked(sparql_engine):
    """Codes outside the table's code list have unverifiable membership -> block."""
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_partition_coherence(TABLE, DIM, _codes('L008691', 'ZZZ999'))


def test_time_dimension_open_years_allowed(sparql_engine):
    """Time/geo dims get the same-class rule: this table's Perioden codes are all years
    (single gecko:Year class), so even the open () selection is coherent."""
    sparql_engine.validate_partition_coherence(TABLE, Dimension('Perioden'), frozenset())


def test_dimension_without_code_list_fails_open(sparql_engine):
    """Marges exposes no <Dimension>Groups endpoint -> no partition metadata -> nothing to enforce."""
    sparql_engine.validate_partition_coherence(TABLE, Dimension('Marges'), frozenset())
    sparql_engine.validate_partition_coherence(TABLE, Dimension('Marges'), _codes('MW00000', 'MOG0095'))


# == The same-class rule for time/geo dimensions ==
# A fake table with mixed-level codes: gecko classes encode the level, and an aggregation is
# coherent iff all selected codes share one class (a country total contains its provinces, a
# year its quarters -- mixing levels double-counts).

GEO_TABLE = Table('99998GEO')


@pytest.fixture(scope='module', autouse=True)
def _geo_table_fixture(sparql_engine):
    dim_ns, gecko = Dimension.rdf_ns, 'https://vocabs.cbs.nl/def/gecko#'
    typed_codes = {'NL01': 'Country', 'PV20': 'Province', 'PV27': 'Province', 'GM0363': 'Municipality',
                   '2022JJ00': 'Year', '2022KW01': 'Quarter'}
    parent = {'NL01': 'RegioS', 'PV20': 'RegioS', 'PV27': 'RegioS', 'GM0363': 'RegioS',
              '2022JJ00': 'Perioden', '2022KW01': 'Perioden'}
    triples = (f"<{dim_ns.term('RegioS')}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{gecko}GeoDimension> .\n"
               f"<{dim_ns.term('Perioden')}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{gecko}TimeDimension> .\n")
    for code, cls in typed_codes.items():
        triples += f"<{GEO_TABLE.uri}> <http://purl.org/linked-data/cube#dimension> <{dim_ns.term(code)}> .\n"
        triples += f"<{dim_ns.term(code)}> <http://www.w3.org/2004/02/skos/core#broader> <{dim_ns.term(parent[code])}> .\n"
        triples += f"<{dim_ns.term(code)}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{gecko}{cls}> .\n"
    sparql_engine.insert_raw({'dimensies': triples}, str(GEO_TABLE.uri))


@pytest.mark.parametrize('codes', [
    ('PV20', 'PV27'),      # single level: provinces
    ('NL01',),             # the lone country total passes as a single-class selection
    ('GM0363',),
    ('2022JJ00',),
])
def test_timegeo_single_level_allowed(sparql_engine, codes):
    dim = Dimension('Perioden' if codes[0][0].isdigit() else 'RegioS')
    sparql_engine.validate_partition_coherence(GEO_TABLE, dim, _codes(*codes))


@pytest.mark.parametrize('codes', [
    ('PV20', 'PV27', 'NL01'),   # provinces + the country total: the motivating case
    ('PV27', 'GM0363'),         # containment overlap (Amsterdam inside Noord-Holland)
    ('PV20', 'GM0363'),         # disjoint cross-level mix: blocked by design (same-class strictness)
])
def test_geo_level_mix_blocked(sparql_engine, codes):
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_partition_coherence(GEO_TABLE, Dimension('RegioS'), _codes(*codes))


def test_time_granularity_mix_blocked(sparql_engine):
    """A year plus one of its quarters double-counts the quarter."""
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_partition_coherence(GEO_TABLE, Dimension('Perioden'),
                                                   _codes('2022JJ00', '2022KW01'))


@pytest.mark.parametrize('dim', ['RegioS', 'Perioden'])
def test_timegeo_open_selection_with_mixed_levels_blocked(sparql_engine, dim):
    """Open () selects all of the table's codes, which span multiple levels here."""
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_partition_coherence(GEO_TABLE, Dimension(dim), frozenset())


def test_timegeo_unknown_code_blocked(sparql_engine):
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_partition_coherence(GEO_TABLE, Dimension('RegioS'),
                                                   _codes('PV20', 'XX99FOO'))


# == The aggregate flag belongs to a code's OWN group ==
# A fake code list with two ungrouped codes (directly under the scheme) and an aggregate-marked group.
# Regression: the flag used to be computed with an EXISTS outside the group OPTIONAL, which for an
# ungrouped code (unbound ?group) matched ANY aggregate group and so marked every ungrouped code a total.

AGG_TABLE = Table('99996AGG')
AGG_DIM = Dimension('Soort')

@pytest.fixture(scope='module', autouse=True)
def _aggregate_group_fixture(sparql_engine):
    dim_ns = Dimension.rdf_ns
    skos, xkos = 'http://www.w3.org/2004/02/skos/core#', 'http://rdf-vocabulary.ddialliance.org/xkos#'
    rdf_type, qb = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://purl.org/linked-data/cube#'
    scheme, group = dim_ns.term('99996AGG/Soort'), dim_ns.term('99996AGG/gTotaal')
    triples = (f"<{dim_ns.term('Soort')}> <{qb}codeList> <{scheme}> .\n"
               f"<{scheme}> <http://purl.org/dc/terms/isPartOf> <{AGG_TABLE.uri}> .\n"
               f"<{group}> <{skos}inScheme> <{scheme}> .\n"
               f"<{group}> <{rdf_type}> <https://vocabs.cbs.nl/def/gecko#Aggregate> .\n"
               f"<{dim_ns.term('A900003')}> <{xkos}isPartOf> <{group}> .\n")
    for code in ('A900001', 'A900002', 'A900003'):
        triples += (f"<{AGG_TABLE.uri}> <{qb}dimension> <{dim_ns.term(code)}> .\n"
                    f"<{dim_ns.term(code)}> <{rdf_type}> <{qb}DimensionProperty> .\n"
                    f"<{dim_ns.term(code)}> <{skos}inScheme> <{scheme}> .\n")
    sparql_engine.insert_raw({'dimensies': triples}, str(AGG_TABLE.uri))


def test_ungrouped_codes_not_flagged_by_other_aggregate_group(sparql_engine):
    sparql_engine.validate_partition_coherence(AGG_TABLE, AGG_DIM, _codes('A900001', 'A900002'))


def test_ungrouped_code_with_aggregate_group_member_blocked(sparql_engine):
    """Still a cross-partition selection: (top-level) vs the aggregate group."""
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_partition_coherence(AGG_TABLE, AGG_DIM, _codes('A900001', 'A900003'))

# == End-to-end through the operators (validation runs before execution in every mode) ==
SUM_TEMPLATE = """
    (SUM (BestemmingEnSeizoen)
        (VALUE 85302NED
            (MSR (D004645))
            (DIM Perioden (2022JJ00))
            (DIM BestemmingEnSeizoen ({codes}))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""


def test_sum_over_single_partition_executes():
    _, answer = eval(parse(SUM_TEMPLATE.format(codes='L008691 L999996')), offline=True)
    # Summing the complete destination partition reproduces the published total
    assert 35933.0 in answer.values.flatten().tolist()


@pytest.mark.parametrize('mode', [{'offline': True}, {'sql': True}, {'sql': True, 'pivot': True}])
def test_sum_with_total_mixed_in_blocks_all_modes(mode):
    with pytest.raises(PartitionCoherenceError):
        eval(parse(SUM_TEMPLATE.format(codes='L008691 L999996 T001047')), **mode)


def test_prop_over_open_dimension_blocks():
    """PROP sums the selector dimension as its denominator, so it inherits the same rule."""
    prop_open = """
        (PROP (DIM BestemmingEnSeizoen (L008691))
            (VALUE 85302NED
                (MSR (D004645))
                (DIM Perioden (2022JJ00))
                (DIM BestemmingEnSeizoen ())
                (DIM Vakantiekenmerken (T001460))
                (DIM Marges (MW00000))
            )
        )
    """
    with pytest.raises(PartitionCoherenceError):
        eval(parse(prop_open), offline=True)


def test_max_over_incoherent_set_blocks():
    """Rank-style operators are guarded too: a total in the set would always 'win' the MAX."""
    max_mixed = SUM_TEMPLATE.replace('(SUM', '(MAX').format(codes='L008691 L999996 T001047')
    with pytest.raises(PartitionCoherenceError):
        eval(parse(max_mixed), offline=True)


# == Measure-collapsing aggregations: totals coherence within the table-scoped measure scheme ==
# Measure groups are NOT same-whole partitions (unlike dimension groups): they are usually
# thematic families ('Varkens' | 'Kippen'), and combining across them is a legitimate composite.
# Only totals reliably double-count: a grand total with anything, a group total with its own
# group's members. The fake table mirrors 80233eng: an ungrouped grand-total measure (totals
# label), two families ('Size class' | 'Legal form', the latter with a group total), and an
# aggregate-marked 'Totaal' group with totals variants.

MSR_TABLE = Table('99997MSR')


def _measures(*codes) -> frozenset:
    return frozenset(Measure(c) for c in codes)


@pytest.fixture(scope='module', autouse=True)
def _measure_table_fixture(sparql_engine):
    msr_ns, tab = Measure.rdf_ns, MSR_TABLE.uri
    skos, xkos = 'http://www.w3.org/2004/02/skos/core#', 'http://rdf-vocabulary.ddialliance.org/xkos#'
    rdf_type = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
    qb = 'http://purl.org/linked-data/cube#'
    scheme = msr_ns.term('99997MSR/Measure')
    groups = {'gSize': 'Size class', 'gLegal': 'Legal form', 'gTot': 'Totaal'}
    members = {'M900002_1': ('gSize', '1 werkzaam persoon'),
               'M900003_1': ('gSize', '2 werkzame personen'),
               'M900004_1': ('gLegal', 'Totaal rechtsvormen'),     # group total of gLegal
               'M900005_1': ('gLegal', 'Besloten vennootschappen'),
               'M900006_1': ('gTot', 'Absoluut'),                  # aggregate-group members are
               'M900007_1': ('gTot', 'Relatief')}                  #  grand totals by group flag

    def concept_triples(node, label):
        c = f"https://vocabs.cbs.nl/def/concept/CT{node}"
        return (f"<{node if node.startswith('http') else msr_ns.term(node)}> <{qb}concept> <{c}> .\n"
                f"<{c}> <{skos}prefLabel> \"{label}\"@nl .\n"
                f"<{c}> <http://purl.org/dc/terms/isPartOf> <{tab}> .\n")

    triples = (f"<{tab}> <{qb}codeList> <{scheme}> .\n"
               f"<{scheme}> <{rdf_type}> <{skos}ConceptScheme> .\n"
               f"<{scheme}> <http://purl.org/dc/terms/isPartOf> <{tab}> .\n")
    for gid, label in groups.items():
        g = msr_ns.term(f'99997MSR/{gid}')
        triples += (f"<{g}> <{rdf_type}> <{skos}Concept> .\n"
                    f"<{g}> <{skos}inScheme> <{scheme}> .\n"
                    f"<{g}> <{skos}topConceptOf> <{scheme}> .\n"
                    f"<{g}> <{skos}prefLabel> \"{label}\"@nl .\n")
    triples += f"<{msr_ns.term('99997MSR/gTot')}> <{rdf_type}> <https://vocabs.cbs.nl/def/gecko#Aggregate> .\n"
    # the ungrouped grand total sits directly under the scheme; its table-scoped totals label
    #  marks it grand (top-level + totals title)
    triples += (f"<{tab}> <{qb}measure> <{msr_ns.term('M900001_1')}> .\n"
                f"<{msr_ns.term('M900001_1')}> <{rdf_type}> <{qb}MeasureProperty> .\n"
                f"<{msr_ns.term('M900001_1')}> <{skos}inScheme> <{scheme}> .\n"
                f"<{msr_ns.term('M900001_1')}> <{skos}topConceptOf> <{scheme}> .\n")
    triples += concept_triples('M900001_1', 'Totaal bedrijven')
    # an ungrouped measure WITHOUT a totals label (cf. 84714ENG's 'Production capacity'): not a total
    triples += (f"<{tab}> <{qb}measure> <{msr_ns.term('M900008_1')}> .\n"
                f"<{msr_ns.term('M900008_1')}> <{rdf_type}> <{qb}MeasureProperty> .\n"
                f"<{msr_ns.term('M900008_1')}> <{skos}inScheme> <{scheme}> .\n"
                f"<{msr_ns.term('M900008_1')}> <{skos}topConceptOf> <{scheme}> .\n")
    triples += concept_triples('M900008_1', 'Productiecapaciteit')
    for code, (gid, label) in members.items():
        g = msr_ns.term(f'99997MSR/{gid}')
        triples += (f"<{tab}> <{qb}measure> <{msr_ns.term(code)}> .\n"
                    f"<{msr_ns.term(code)}> <{rdf_type}> <{qb}MeasureProperty> .\n"
                    f"<{msr_ns.term(code)}> <{skos}inScheme> <{scheme}> .\n"
                    f"<{msr_ns.term(code)}> <{xkos}isPartOf> <{g}> .\n"
                    # one shared unit, so aggregations over these measures pass the unit gate of validate()
                    f"<{msr_ns.term(code)}> <http://qudt.org/schema/qudt/unit> <http://qudt.org/vocab/unit/KiloGM> .\n")
        triples += concept_triples(code, label)
    sparql_engine.insert_raw({'onderwerpen': triples}, str(tab))


@pytest.mark.parametrize('measures', [
    ('M900002_1', 'M900003_1'),   # non-total members of one family
    ('M900002_1',),               # single measure
    ('M900001_1',),               # the lone grand total
    ('M900002_1', 'M900005_1'),   # ACROSS families: legitimate composite (pigs + chickens)
    ('M900004_1', 'M900002_1'),   # a group total combined with ANOTHER family's member is fine
    ('M900008_1', 'M900002_1'),   # an ungrouped non-total measure is not flagged by the aggregate gTot group
])
def test_measures_coherent_selections_allowed(sparql_engine, measures):
    sparql_engine.validate_measure_coherence(MSR_TABLE, _measures(*measures))


@pytest.mark.parametrize('measures', [
    ('M900001_1', 'M900002_1'),   # the grand total alongside any other measure
    ('M900006_1', 'M900007_1'),   # two members of the aggregate-marked group (totals variants)
    ('M900006_1', 'M900002_1'),   # an aggregate-group member alongside an ordinary measure
    ('M900004_1', 'M900005_1'),   # a group total with a member of its OWN group
])
def test_measures_total_mixing_blocked(sparql_engine, measures):
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_measure_coherence(MSR_TABLE, _measures(*measures))


def test_measures_open_selection_blocked(sparql_engine):
    """An empty set means all of the table's measures, which includes the grand total."""
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_measure_coherence(MSR_TABLE, frozenset())


def test_measures_unknown_blocked(sparql_engine):
    with pytest.raises(PartitionCoherenceError):
        sparql_engine.validate_measure_coherence(MSR_TABLE, _measures('M900002_1', 'M999999_9'))


def test_real_cross_family_composite_allowed(sparql_engine):
    """Regression for the false positive that surfaced on the regenerated ut fixture: 80781ned
    sums 'Biggen' (group Varkens) with 'Leghennen' (group Kippen) — disjoint families, no
    double-counting, must pass."""
    sparql_engine.validate_measure_coherence(Table('80781ned'), _measures('A028994_1', 'A028997_1'))


def test_table_without_measure_scheme_fails_open(sparql_engine):
    """A table without MeasureGroups/TopicGroups has no measure scheme -> nothing to enforce."""
    sparql_engine.validate_measure_coherence(Table('99999NOSCHEME'), _measures('D004645', 'D006211_2'))
    sparql_engine.validate_measure_coherence(Table('99999NOSCHEME'), frozenset())


# == Static validation: Expression.validate() runs the gates without executing ==

def test_static_validate_blocks_incoherent_measure_sum():
    """The measure gate fires through the static entry point — no OData/DB call needed, so
    stored S-expressions can be audited in bulk. M900004_1 is the total of its own group and
    M900005_1 a member of that same group: summing them double-counts."""
    sexp = eval(parse("""
        (SUM ()
            (VALUE 99997MSR
                (MSR (M900004_1 M900005_1))
                (DIM Perioden (2022JJ00))
            )
        )
    """)[0])  # eval'ing the inner list constructs the tree WITHOUT executing it
    with pytest.raises(PartitionCoherenceError):
        sexp.validate()


def test_static_validate_passes_coherent_measure_sum():
    eval(parse("""
        (SUM ()
            (VALUE 99997MSR
                (MSR (M900002_1 M900003_1))
                (DIM Perioden (2022JJ00))
            )
        )
    """)[0]).validate()


def test_static_validate_reaches_all_join_branches():
    """PROP's gate must descend into every JOIN branch (the old walk assumed a single
    sub_expression chain and missed/crashed on branching nodes)."""
    sexp = eval(parse("""
        (PROP (DIM BestemmingEnSeizoen (L008691))
            (JOIN (D004645)
                (VALUE 85302NED
                    (MSR (D004645))
                    (DIM Perioden (2022JJ00))
                    (DIM BestemmingEnSeizoen (L008691 L999996))
                    (DIM Vakantiekenmerken (T001460))
                    (DIM Marges (MW00000))
                )
                (VALUE 85302NED
                    (MSR (D004645))
                    (DIM Perioden (2022JJ00))
                    (DIM BestemmingEnSeizoen ())
                    (DIM Vakantiekenmerken (T001460))
                    (DIM Marges (MW00000))
                )
            )
        )
    """)[0])
    assert len(sexp.inner_values()) == 2, "traversal must find the VALUE in every JOIN branch"
    with pytest.raises(PartitionCoherenceError):
        sexp.validate()  # the open () in the SECOND branch is only reachable via sub_expressions
