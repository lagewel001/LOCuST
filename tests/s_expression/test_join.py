import duckdb
import pandas as pd
import pytest
import sqlglot

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

# == UNIT TESTS FOR JOIN ON DIMENSION GROUP ==
# Let's ignore that joining these two tables doesn't make any sense
JOIN_ON_DIM_SEXP = """
    (JOIN (Perioden)
        (VALUE 84957NED
            (MSR (M004367))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM Vervoerstromen (A045747))
        )
        (VALUE 85302NED
            (MSR (D006211_2))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

JOIN_ON_DIM_ANSWER_DATA = [[11003., 205.2], [9659., 265.4]]
JOIN_ON_DIM_ANSWER_INDEX_SEXP = ['2021', '2022']
JOIN_ON_DIM_ANSWER_COLS_SEXP = [('Ladingtonkilometer', 'Binnenlands', 'mln tonkm'), ('Totaal overnachtingen', 'Totaal vakanties', 'Waarde')]
JOIN_ON_DIM_ANSWER_DF_SEXP = pd.DataFrame(
    data=JOIN_ON_DIM_ANSWER_DATA,
    index=pd.Index(JOIN_ON_DIM_ANSWER_INDEX_SEXP),
    columns=pd.MultiIndex.from_tuples(JOIN_ON_DIM_ANSWER_COLS_SEXP),
)

JOIN_ON_DIM_ANSWER_INDEX_PIVOT_SQL = ['2021', '2022']
JOIN_ON_DIM_ANSWER_COLS_PIVOT_SQL = [('Ladingtonkilometer', 'Binnenlands', '', ''), ('Totaal vakanties', 'Totaal overnachtingen', 'Waarde', 'Totaal vakanties')]
JOIN_ON_DIM_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=JOIN_ON_DIM_ANSWER_DATA,
    index=pd.Index(JOIN_ON_DIM_ANSWER_INDEX_PIVOT_SQL),
    columns=pd.MultiIndex.from_tuples(JOIN_ON_DIM_ANSWER_COLS_PIVOT_SQL),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_join_on_dim_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(JOIN_ON_DIM_SEXP))
    assert_frame_equal_unordered(answer, JOIN_ON_DIM_ANSWER_DF_SEXP)

@pytest.mark.skip(reason="Offline SQL joins are not yet identical to SEXP joins")
def test_join_on_dim_sexp_offline():
    # FIXME: make sure the results of the offline mode is identical
    #  to the online mode and Units are being returned as well
    expected_columns = [('Ladingtonkilometer', 'Binnenlands'), ('Totaal overnachtingen', 'Totaal vakanties')]
    expected_df = pd.DataFrame(
        data=JOIN_ON_DIM_ANSWER_DATA,
        index=pd.Index(JOIN_ON_DIM_ANSWER_INDEX_SEXP),
        columns=pd.MultiIndex.from_tuples(expected_columns, names=['foo', 'bar']),
    )

    _, answer = eval(parse(JOIN_ON_DIM_SEXP), offline=True)
    assert_frame_equal_unordered(answer, expected_df)

def test_join_on_dim_sql():
    expected_answer = pd.DataFrame({'Perioden': ['2021', '2022'],
                                    'Ladingtonkilometer': [11003., 9659.], 'Vervoerstromen': ['Binnenlands'] * 2,
                                    'Totaal overnachtingen': [205.2, 265.4], 'Bestemming en seizoen': ['Totaal vakanties'] * 2,
                                    'Marges': ['Waarde'] * 2, 'Vakantiekenmerken': ['Totaal vakanties'] * 2})
    _, answer = eval(parse(JOIN_ON_DIM_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_join_on_dim_pivot_sql():
    _, answer = eval(parse(JOIN_ON_DIM_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, JOIN_ON_DIM_ANSWER_DF_PIVOT_SQL)


# == UNIT TEST FOR JOIN ON MEASURES ==
# No, it doesn't make any sense to JOIN two of the same tables, but it can test the workings perfectly
JOIN_ON_MSR_SEXP = """
    (JOIN (M004367)
        (VALUE 84957NED
            (MSR (M004367))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM Vervoerstromen (A045747))
        )
        (VALUE 84957NED
            (MSR (M004367))
            (DIM Perioden (2016JJ00 2017JJ00))
            (DIM Vervoerstromen (A045747))
        )
    )
"""

JOIN_ON_MSR_ANSWER_DATA = [[11003., 9659., 19379., 17946.]]
JOIN_ON_MSR_ANSWER_INDEX_SEXP = [('Ladingtonkilometer', 'mln tonkm')]
JOIN_ON_MSR_ANSWER_COLS_SEXP = [('Binnenlands', '2021'), ('Binnenlands', '2022'), ('Binnenlands', '2016'), ( 'Binnenlands', '2017')]
JOIN_ON_MSR_ANSWER_DF_SEXP = pd.DataFrame(
    data=JOIN_ON_MSR_ANSWER_DATA,
    index=pd.Index(JOIN_ON_MSR_ANSWER_INDEX_SEXP, tupleize_cols=False),
    columns=pd.MultiIndex.from_tuples(JOIN_ON_MSR_ANSWER_COLS_SEXP),
)

JOIN_ON_MSR_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=JOIN_ON_MSR_ANSWER_DATA,
    index=pd.MultiIndex.from_tuples(JOIN_ON_MSR_ANSWER_INDEX_SEXP, names=['Measure', 'Unit']),
    columns=pd.MultiIndex.from_tuples(JOIN_ON_MSR_ANSWER_COLS_SEXP),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_join_on_msr_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(JOIN_ON_MSR_SEXP))
    assert_frame_equal_unordered(answer, JOIN_ON_MSR_ANSWER_DF_SEXP)

def test_join_on_msr_sexp_offline():
    _, answer = eval(parse(JOIN_ON_MSR_SEXP), offline=True)
    assert_frame_equal_unordered(answer, JOIN_ON_MSR_ANSWER_DF_SEXP)

def test_join_on_msr_sql():
    # Joining over a shared measure stacks the values of both expressions (UNION ALL BY NAME)
    expected_answer = pd.DataFrame({'Perioden': ['2021', '2022', '2016', '2017'],
                                    'Vervoerstromen': ['Binnenlands'] * 4,
                                    'Ladingtonkilometer': [11003., 9659., 19379., 17946.]})
    sexp, answer = eval(parse(JOIN_ON_MSR_SEXP), sql=True)
    assert 'UNION ALL BY NAME' in sexp.odata3_sql
    assert_frame_equal_unordered(answer, expected_answer)

def test_join_on_msr_pivot_sql():
    _, answer = eval(parse(JOIN_ON_MSR_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, JOIN_ON_MSR_ANSWER_DF_PIVOT_SQL)


# == UNIT TEST FOR JOIN WITH A SHARED, DIFFERENTLY FILTERED DIMENSION ==
# Both sides share BestemmingEnSeizoen but filter different codes. The non-pivot SQL must join on the selector
# (plus the identically filtered shared groups) only; joining on BestemmingEnSeizoen too would never match and
# split the answer over four half-empty rows. TableB's differing group is kept under a '_B' suffix.
JOIN_DIFFERING_DIM_SEXP = """
    (JOIN (Perioden)
        (VALUE 85302NED
            (MSR (D004645))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
        (VALUE 85302NED
            (MSR (D006211_2))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (L008691))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

def test_join_differing_shared_dim_sql():
    sexp, answer = eval(parse(JOIN_DIFFERING_DIM_SEXP), sql=True)
    assert 'TableA.BestemmingEnSeizoen = TableB.BestemmingEnSeizoen' not in sexp.odata3_sql
    assert len(answer) == 2
    assert sorted(zip(answer['Totaal vakanties'], answer['Totaal overnachtingen'])) == [(31685., 98.5), (35933., 73.4)]
    assert 'BestemmingEnSeizoen_B' in answer.columns


def test_join_differing_shared_dim_pivot_sql_row_count():
    _, answer = eval(parse(JOIN_DIFFERING_DIM_SEXP), sql=True, pivot=True)
    assert len(answer) == 2

# == UNIT TEST FOR PIVOTED JOIN COLUMNS THAT START WITH A DIGIT ==
# Joining over BestemmingEnSeizoen pivots Perioden, so the pivoted column names contain period codes and may
# start with a digit (e.g. 2021JJ00_D004645_...), depending on the (set-based) order of the PIVOT fields.
# Unquoted, DuckDB reads such a column reference as a number plus an alias and returns e.g. 2021 as value.
JOIN_PIVOTED_PERIODS_SEXP = """
    (JOIN (BestemmingEnSeizoen)
        (VALUE 85302NED
            (MSR (D004645))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
        (VALUE 85302NED
            (MSR (D006211_2))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

def test_join_pivot_sql_quotes_pivoted_columns():
    sexp = eval(parse(JOIN_PIVOTED_PERIODS_SEXP)[0])
    outer = sqlglot.parse_one(sexp.odata3_pivot_sql)
    pivoted = [e for e in outer.expressions if isinstance(e, sqlglot.exp.Column) and not e.table]
    assert len(pivoted) == 4 and all(col.this.quoted for col in pivoted)

    values = duckdb.sql(sexp.odata3_pivot_sql).df().drop(columns='BestemmingEnSeizoen').iloc[0].tolist()
    assert sorted(values) == [205.2, 265.4, 31685., 35933.]
