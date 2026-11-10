import pandas as pd
import pytest

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

MIN_ON_DIM_SEXP = """
    (MIN
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

# S-exp returns (argmin, min)
MIN_ON_MSR_ANSWER_DATA_SEXP = [[('2021', 31685.0)]]
MIN_ON_DIM_ANSWER_INDEX_SEXP = [('x 1 000', 'Totaal vakanties')]
MIN_ON_DIM_ANSWER_COLS_SEXP = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MIN[\'Perioden\']')]
MIN_ON_DIM_ANSWER_DF_SEXP = pd.DataFrame(
    data=MIN_ON_MSR_ANSWER_DATA_SEXP,
    index=pd.MultiIndex.from_tuples(MIN_ON_DIM_ANSWER_INDEX_SEXP, names=['Unit', 'Measure']),
    columns=pd.Index(MIN_ON_DIM_ANSWER_COLS_SEXP),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_min_on_dim_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(MIN_ON_DIM_SEXP))
    assert_frame_equal_unordered(answer, MIN_ON_DIM_ANSWER_DF_SEXP)

def test_min_on_dim_sexp_offline():
    _, answer = eval(parse(MIN_ON_DIM_SEXP), offline=True)
    assert_frame_equal_unordered(answer, MIN_ON_DIM_ANSWER_DF_SEXP)

# Pivoted SQL returns (min, argmin) too: MIN(Value) and ARG_MIN(Perioden, Value) inside the PIVOT
MIN_ON_DIM_ANSWER_DATA_PIVOT_SQL = [[31685.0, '2021']]
MIN_ON_DIM_ANSWER_INDEX_PIVOT_SQL = [('Totaal vakanties', 'x 1 000')]
MIN_ON_DIM_ANSWER_COLS_PIVOT_SQL = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MIN'),
                                    ('Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MIN[Perioden]')]
MIN_ON_DIM_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=MIN_ON_DIM_ANSWER_DATA_PIVOT_SQL,
    index=pd.MultiIndex.from_tuples(MIN_ON_DIM_ANSWER_INDEX_PIVOT_SQL, names=['Measure', 'Unit']),
    columns=pd.MultiIndex.from_tuples(MIN_ON_DIM_ANSWER_COLS_PIVOT_SQL, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'MIN']),
)

def test_min_on_dim_sql():
    # Non-pivoted SQL returns (argmin, min)
    expected_answer = pd.DataFrame({'Vakantiekenmerken': 'Totaal vakanties', 'Marges': 'Waarde',
                                    'Bestemming en seizoen': 'Totaal vakanties', 'MIN[Perioden]': '2021',
                                    'Totaal vakanties': 31685.0}, index=range(1))
    _, answer = eval(parse(MIN_ON_DIM_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_min_on_dim_pivot_sql():
    _, answer = eval(parse(MIN_ON_DIM_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, MIN_ON_DIM_ANSWER_DF_PIVOT_SQL)


MIN_ON_MSR_SEXP = """
    (MIN
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

# S-exp returns (argmin, min)
MIN_ON_MSR_ANSWER_DATA_SEXP = [[('Totaal vakanties', 'aantal', 31685000.0),
                                ('Totaal vakanties', 'aantal', 35933000.0)]]
MIN_ON_MSR_ANSWER_INDEX_SEXP = ['Totaal overnachtingen\nTotaal vakanties']
MIN_ON_MSR_ANSWER_COLS_SEXP = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2021'),
                               ('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2022')]
MIN_ON_MSR_ANSWER_DF_SEXP = pd.DataFrame(
    data=MIN_ON_MSR_ANSWER_DATA_SEXP,
    index=pd.Index(MIN_ON_MSR_ANSWER_INDEX_SEXP),
    columns=pd.Index(MIN_ON_MSR_ANSWER_COLS_SEXP),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_min_on_msr_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(MIN_ON_MSR_SEXP))
    assert_frame_equal_unordered(answer, MIN_ON_MSR_ANSWER_DF_SEXP)

def test_min_on_msr_sexp_offline():
    _, answer = eval(parse(MIN_ON_MSR_SEXP), offline=True)
    assert_frame_equal_unordered(answer, MIN_ON_MSR_ANSWER_DF_SEXP)

MIN_ON_MSR_SQL = """
    (MIN
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
# Pivoted SQL returns (min, argmin) per period: ARG_MIN(Measure, Value) names the extreme measure
MIN_ON_MSR_ANSWER_DATA_PIVOT_SQL = [[446., 'Gemiddeld per persoon per vakantie', 571., 'Gemiddeld per persoon per vakantie']]
MIN_ON_MSR_ANSWER_INDEX_PIVOT_SQL = [('Gemiddeld per persoon per vakantie;Gemiddeld per Nederlander', '')]
MIN_ON_MSR_ANSWER_COLS_PIVOT_SQL = [('2021', 'Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MIN'),
                              ('2021', 'Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MIN[Measure]'),
                              ('2022', 'Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MIN'),
                              ('2022', 'Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MIN[Measure]')]
MIN_ON_MSR_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=MIN_ON_MSR_ANSWER_DATA_PIVOT_SQL,
    index=pd.MultiIndex.from_tuples(MIN_ON_MSR_ANSWER_INDEX_PIVOT_SQL, names=['Measure', 'Unit']),
    columns=pd.MultiIndex.from_tuples(MIN_ON_MSR_ANSWER_COLS_PIVOT_SQL, names=['Perioden', 'Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'MIN']),
)

def test_min_on_msr_sql():
    # Non-pivoted SQL returns (argmin, min) per period, identical to the values of the pivoted SQL
    expected_answer = pd.DataFrame({'Vakantiekenmerken': ['Totaal vakanties'] * 2, 'Marges': ['Waarde'] * 2,
                                    'Bestemming en seizoen': ['Totaal vakanties'] * 2, 'MIN': [446.0, 571.0],
                                    'MIN[Measure]': ['Gemiddeld per persoon per vakantie'] * 2, 'Perioden': ['2021', '2022']}, index=range(2))
    _, answer = eval(parse(MIN_ON_MSR_SQL), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_min_on_msr_pivot_sql():
    _, answer = eval(parse(MIN_ON_MSR_SQL), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, MIN_ON_MSR_ANSWER_DF_PIVOT_SQL)
