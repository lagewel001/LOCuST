import pandas as pd
import pytest

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

AVG_SEXP = """
    (AVG
        (Perioden)
        (VALUE 85302NED
            (MSR (D004645))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

AVG_ANSWER_DATA = [[33809.0]]
AVG_ANSWER_INDEX_SEXP = [('x 1 000', 'Totaal vakanties')]
AVG_ANSWER_COLS_SEXP = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', "AVG['Perioden']")]
AVG_ANSWER_DF_SEXP = pd.DataFrame(
    data=AVG_ANSWER_DATA,
    index=pd.MultiIndex.from_tuples(AVG_ANSWER_INDEX_SEXP, names=['Unit', 'Measure']),
    columns=pd.MultiIndex.from_tuples(AVG_ANSWER_COLS_SEXP, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'Perioden']),
)

AVG_ANSWER_INDEX_PIVOT_SQL = [('Totaal vakanties', 'x 1 000')]
AVG_ANSWER_COLS_PIVOT_SQL = [('Totaal vakanties', 'Waarde', 'Totaal vakanties')]
AVG_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=AVG_ANSWER_DATA,
    index=pd.MultiIndex.from_tuples(AVG_ANSWER_INDEX_PIVOT_SQL, names=['Measure', 'Unit']),
    columns=pd.MultiIndex.from_tuples(AVG_ANSWER_COLS_PIVOT_SQL, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken']),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_avg_sexp():
    """For this test an internet connection is required and the OData4 API must be live"""
    _, answer = eval(parse(AVG_SEXP))
    assert_frame_equal_unordered(answer, AVG_ANSWER_DF_SEXP)

def test_avg_sexp_offline():
    _, answer = eval(parse(AVG_SEXP), offline=True)
    assert_frame_equal_unordered(answer, AVG_ANSWER_DF_SEXP)

def test_avg_sql():
    # Averaging over Perioden collapses the periods into a single value, identical to the pivoted SQL
    expected_answer = pd.DataFrame({'AVG': [33809.],
                                    'Bestemming en seizoen': ['Totaal vakanties'],
                                    'Marges': ['Waarde'],
                                    'Vakantiekenmerken': ['Totaal vakanties']}, index=range(1))
    _, answer = eval(parse(AVG_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_avg_sql_does_not_group_by_selector():
    """The selector is aggregated over, so grouping by it would return one row per selector code"""
    sexp, _ = eval(parse(AVG_SEXP), sql=True)
    group_by = sexp.odata3_sql.split('GROUP BY')[-1]
    assert 'Perioden' not in group_by
    assert "Perioden IN ('2021JJ00', '2022JJ00')" in sexp.odata3_sql

def test_avg_pivot_sql():
    _, answer = eval(parse(AVG_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, AVG_ANSWER_DF_PIVOT_SQL)
