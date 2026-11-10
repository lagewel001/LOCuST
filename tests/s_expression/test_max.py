import pandas as pd
import pytest

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

MAX_ON_DIM_SEXP = """
    (MAX
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

# S-exp returns (argmax, max)
MAX_ON_DIM_ANSWER_DATA_SEXP = [[('2022', 35933.0)]]
MAX_ON_DIM_ANSWER_INDEX_SEXP = [('Totaal vakanties', 'x 1 000')]
MAX_ON_DIM_ANSWER_COLS_SEXP = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MAX[\'Perioden\']')]
MAX_ON_DIM_ANSWER_DF_SEXP = pd.DataFrame(
    data=MAX_ON_DIM_ANSWER_DATA_SEXP,
    index=pd.MultiIndex.from_tuples(MAX_ON_DIM_ANSWER_INDEX_SEXP, names=['Measure', 'Unit']),
    columns=pd.Index(MAX_ON_DIM_ANSWER_COLS_SEXP),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_max_on_dim_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(MAX_ON_DIM_SEXP))
    assert_frame_equal_unordered(answer, MAX_ON_DIM_ANSWER_DF_SEXP)

def test_max_sexp_on_dim_offline():
    _, answer = eval(parse(MAX_ON_DIM_SEXP), offline=True)
    assert_frame_equal_unordered(answer, MAX_ON_DIM_ANSWER_DF_SEXP)

# Pivoted SQL returns (max, argmax) too: MAX(Value) and ARG_MAX(Perioden, Value) inside the PIVOT
MAX_ON_DIM_ANSWER_DATA_PIVOT_SQL = [[35933.0, '2022']]
MAX_ON_DIM_ANSWER_INDEX_PIVOT_SQL = [('Totaal vakanties', 'x 1 000')]
MAX_ON_DIM_ANSWER_COLS_PIVOT_SQL = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MAX'),
                                    ('Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MAX[Perioden]')]
MAX_ON_DIM_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=MAX_ON_DIM_ANSWER_DATA_PIVOT_SQL,
    index=pd.MultiIndex.from_tuples(MAX_ON_DIM_ANSWER_INDEX_PIVOT_SQL, names=['Measure', 'Unit']),
    columns=pd.MultiIndex.from_tuples(MAX_ON_DIM_ANSWER_COLS_PIVOT_SQL, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'MAX']),
)

def test_max_on_dim_sql():
    # Non-pivoted SQL returns (argmax, max)
    expected_answer = pd.DataFrame({'Vakantiekenmerken': 'Totaal vakanties', 'Marges': 'Waarde',
                                    'Bestemming en seizoen': 'Totaal vakanties', 'MAX[Perioden]': '2022',
                                    'Totaal vakanties': 35933.0}, index=range(1))
    _, answer = eval(parse(MAX_ON_DIM_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_max_on_dim_pivot_sql():
    _, answer = eval(parse(MAX_ON_DIM_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, MAX_ON_DIM_ANSWER_DF_PIVOT_SQL)


MAX_ON_MSR_SEXP = """
    (MAX
        ()
        (VALUE 85302NED
            (MSR (D004645 D006211_2))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

# S-exp returns (argmax, max)
MAX_ON_MSR_ANSWER_DATA_SEXP = [[('Totaal overnachtingen', 'aantal', 205200000.0),
                                ('Totaal overnachtingen', 'aantal', 265399999.99999997)]]
MAX_ON_MSR_ANSWER_INDEX_SEXP = ['Totaal overnachtingen\nTotaal vakanties']
MAX_ON_MSR_ANSWER_COLS_SEXP = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2021'),
                               ('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2022')]
MAX_ON_MSR_ANSWER_DF_SEXP = pd.DataFrame(
    data=MAX_ON_MSR_ANSWER_DATA_SEXP,
    index=pd.Index(MAX_ON_MSR_ANSWER_INDEX_SEXP),
    columns=pd.Index(MAX_ON_MSR_ANSWER_COLS_SEXP),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_max_on_msr_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(MAX_ON_MSR_SEXP))
    assert_frame_equal_unordered(answer, MAX_ON_MSR_ANSWER_DF_SEXP)

def test_max_on_msr_sexp_offline():
    _, answer = eval(parse(MAX_ON_MSR_SEXP), offline=True)
    assert_frame_equal_unordered(answer, MAX_ON_MSR_ANSWER_DF_SEXP)

MAX_ON_MSR_SQL = """
    (MAX
        ()
        (VALUE 85302NED
            (MSR (M001339 M001860))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""
# Pivoted SQL returns (max, argmax) per period: ARG_MAX(Measure, Value) names the extreme measure
MAX_ON_MSR_ANSWER_DATA_PIVOT_SQL = [[973., 'Gemiddeld per Nederlander', 1401., 'Gemiddeld per Nederlander']]
MAX_ON_MSR_ANSWER_INDEX_PIVOT_SQL = [('Gemiddeld per persoon per vakantie;Gemiddeld per Nederlander', '')]
MAX_ON_MSR_ANSWER_COLS_PIVOT_SQL = [('2021', 'Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MAX'),
                              ('2021', 'Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MAX[Measure]'),
                              ('2022', 'Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MAX'),
                              ('2022', 'Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MAX[Measure]')]
MAX_ON_MSR_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=MAX_ON_MSR_ANSWER_DATA_PIVOT_SQL,
    index=pd.MultiIndex.from_tuples(MAX_ON_MSR_ANSWER_INDEX_PIVOT_SQL, names=['Measure', 'Unit']),
    columns=pd.MultiIndex.from_tuples(MAX_ON_MSR_ANSWER_COLS_PIVOT_SQL, names=['Perioden', 'Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'MAX']),
)

def test_max_on_msr_sql():
    # Non-pivoted SQL returns (argmax, max) per period, identical to the values of the pivoted SQL
    expected_answer = pd.DataFrame({'Vakantiekenmerken': ['Totaal vakanties'] * 2, 'Marges': ['Waarde'] * 2,
                                    'Bestemming en seizoen': ['Totaal vakanties'] * 2, 'MAX': [973.0, 1401.0],
                                    'MAX[Measure]': ['Gemiddeld per Nederlander'] * 2, 'Perioden': ['2021', '2022']}, index=range(2))
    _, answer = eval(parse(MAX_ON_MSR_SQL), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_max_on_msr_pivot_sql():
    _, answer = eval(parse(MAX_ON_MSR_SQL), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, MAX_ON_MSR_ANSWER_DF_PIVOT_SQL)
