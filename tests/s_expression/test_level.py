"""
    Tests for the intensional `(DIM <dim> (LEVEL <level> [<scope>]))` time/geo selections:
    engine-side resolution against the metadata graph, the hybrid compact SQL patterns, and
    end-to-end execution through the S-expression operators.

    The unittest graph contains for table 80781ned: 1 Land, 4 Landsdelen, 12 Provincies,
    605 Gemeenten (plus COROP/landbouwgebied groupings) under RegioS, and 26 Jaren under
    Perioden. GA08 has containment edges to gemeenten GM0307 and GM0327.
"""
import pytest

from pipeline.odata_executor import OData3QueryBuilder, OData4QueryBuilder
from s_expression import Table, Dimension
from s_expression.parser import parse, eval

TABLE = Table('80781ned')
REGIOS = Dimension('RegioS')
PERIODEN = Dimension('Perioden')

MAX_LEVEL_SEXP = """
    (MAX
        (RegioS)
        (VALUE 80781ned
            (MSR (A028997_1))
            (DIM Perioden (2020JJ00))
            (DIM RegioS (LEVEL GM))
        )
    )
"""


# --- Engine-side resolution ---

def test_resolve_level_codes_counts(sparql_engine):
    for level, expected in [('GM', 605), ('PV', 12), ('LD', 4), ('NL', 1)]:
        assert len(sparql_engine.resolve_level_codes(TABLE, REGIOS, level).codes) == expected
    assert len(sparql_engine.resolve_level_codes(TABLE, PERIODEN, 'JJ').codes) == 26


def test_resolve_level_token_case_insensitive(sparql_engine):
    """The prefix/infix token is the ONLY accepted form, matched case-insensitively."""
    reference = sparql_engine.resolve_level_codes(TABLE, REGIOS, 'GM').codes
    assert sparql_engine.resolve_level_codes(TABLE, REGIOS, 'gm').codes == reference
    for alias in ('Gemeente', 'Municipality'):  # labels/class names are not supported
        with pytest.raises(ValueError, match=f"Unknown level '{alias}'"):
            sparql_engine.resolve_level_codes(TABLE, REGIOS, alias)


def test_resolve_level_sql_patterns(sparql_engine):
    """Prefix/infix patterns are only returned when proven exact against the table's codes."""
    assert sparql_engine.resolve_level_codes(TABLE, REGIOS, 'GM').sql_pattern == ('like', 'GM%')
    assert sparql_engine.resolve_level_codes(TABLE, PERIODEN, 'JJ').sql_pattern == ('regex', r'^\d{4}JJ\d{2}$')
    # A graph-edge-scoped selection cannot be expressed as a code pattern
    assert sparql_engine.resolve_level_codes(TABLE, REGIOS, 'GM', scope='GA08').sql_pattern is None


def test_resolve_level_scoped(sparql_engine):
    """Scopes restrict a level via graph containment edges or code embedding."""
    assert sparql_engine.resolve_level_codes(TABLE, REGIOS, 'GM', scope='GA08').codes == ['GM0307', 'GM0327']

    year = sparql_engine.resolve_level_codes(TABLE, PERIODEN, 'JJ', scope='2005')
    assert year.codes == ['2005JJ00']
    assert year.sql_pattern == ('regex', r'^2005JJ\d{2}$')


def test_resolve_level_errors(sparql_engine):
    with pytest.raises(ValueError, match="Unknown level 'Sterrenstelsel'"):
        sparql_engine.resolve_level_codes(TABLE, REGIOS, 'Sterrenstelsel')
    with pytest.raises(ValueError, match="Available levels"):
        sparql_engine.resolve_level_codes(TABLE, REGIOS, 'BU')  # level absent in this table
    with pytest.raises(ValueError, match="contains no 'GM' codes"):
        sparql_engine.resolve_level_codes(TABLE, REGIOS, 'GM', scope='GA99')


def test_dim_level_summary(sparql_engine):
    summary = sparql_engine.get_dim_level_summary(TABLE)
    assert summary['Perioden'] == [('JJ', 'Jaar', 26, None)]
    # Hierarchical chain first (by gecko:level), non-standardized groupings after
    assert summary['RegioS'][:4] == [('NL', 'Land', 1, None), ('LD', 'Landsdeel', 4, None),
                                     ('PV', 'Provincie', 12, None), ('GM', 'Gemeente', 605, None)]
    assert ('CR', 'COROP-gebied', 40, None) in summary['RegioS']


# --- S-expression execution ---

def test_max_over_level_sql():
    """'Which gemeente had the most laying hens in 2020?' without enumerating 605 codes"""
    sexp, answer = eval(parse(MAX_LEVEL_SEXP), sql=True)
    assert answer['MAX[RegioS]'].item() == 'Venray'
    assert answer['Leghennen'].item() == 3734057.0
    # The WHERE filters the level with the compact pattern instead of an IN-list
    assert "RegioS LIKE 'GM%'" in sexp.odata3_sql
    assert 'GM0064' not in sexp.odata3_sql


def test_max_over_level_pivot_sql():
    sexp, answer = eval(parse(MAX_LEVEL_SEXP), sql=True, pivot=True)
    # The pivoted MAX returns the value and the argmax (the gemeente) per period
    assert sorted(map(str, answer.values.ravel())) == ['3734057.0', 'Venray']
    # The aggregated selector is only filtered on (not pivoted), so the compact pattern applies here too
    assert "RegioS LIKE 'GM%'" in sexp.odata3_pivot_sql
    assert 'GM0064' not in sexp.odata3_pivot_sql


def test_join_over_level_pivot_sql():
    """A LEVEL selector of a JOIN is filtered with the compact pattern in both inner pivot queries."""
    sexp, answer = eval(parse("""
        (JOIN (RegioS)
            (VALUE 80781ned (MSR (A028997_1)) (DIM Perioden (2020JJ00)) (DIM RegioS (LEVEL PV)))
            (VALUE 80781ned (MSR (A028994_1)) (DIM Perioden (2020JJ00)) (DIM RegioS (LEVEL PV)))
        )
    """), sql=True, pivot=True)
    assert sexp.odata3_pivot_sql.count("RegioS LIKE 'PV%'") == 2
    assert 'PV20' not in sexp.odata3_pivot_sql
    assert len(answer) == 12


def test_max_over_scoped_level_sql():
    """Scoped levels (gemeenten within provincie PV30) resolve through the containment edges"""
    _, answer = eval(parse(MAX_LEVEL_SEXP.replace('LEVEL GM', 'LEVEL GM PV30')), sql=True)
    assert answer['MAX[RegioS]'].item() == 'Boxmeer'


def test_time_level_in_value_sql():
    """(LEVEL JJ) selects all yearly periods; the SQL filters on the infix regex"""
    sexp, answer = eval(parse("""
        (VALUE 80781ned
            (MSR (A028997_1))
            (DIM RegioS (NL01))
            (DIM Perioden (LEVEL JJ))
        )
    """), sql=True)
    assert len(answer) == 25  # the parquet data covers one year less than the metadata graph
    assert r"REGEXP_MATCHES(Perioden, '^\d{4}JJ\d{2}$')" in sexp.odata3_sql


def test_avg_over_level_passes_coherence():
    """A LEVEL selection is same-level by construction, so the partition-coherence gate passes"""
    _, answer = eval(parse("""
        (AVG
            (RegioS)
            (VALUE 80781ned
                (MSR (A028997_1))
                (DIM Perioden (2020JJ00))
                (DIM RegioS (LEVEL PV))
            )
        )
    """), sql=True)
    assert len(answer) == 1  # the provinces are averaged into a single value, identical to the pivoted SQL
    assert answer['AVG'].item() == 3597165.5


def test_level_invalid_arity():
    with pytest.raises(SyntaxError):
        eval(parse("""
            (VALUE 80781ned
                (MSR (A028997_1))
                (DIM Perioden (2020JJ00))
                (DIM RegioS (LEVEL))
            )
        """), sql=True)


# --- OData query builders ---

def test_odata3_prefix_filter():
    builder = OData3QueryBuilder(TABLE)
    builder.add_prefix_filter(REGIOS, 'WK')
    assert str(builder).endswith("?$filter=startswith(RegioS,'WK')")


def test_odata4_prefix_filter():
    builder = OData4QueryBuilder(TABLE)
    builder.add_msr_filter({'A028997_1'})
    builder.add_prefix_filter(REGIOS, 'WK')
    assert str(builder).endswith("?$filter=Measure in ('A028997_1') and startswith(RegioS,'WK')")
