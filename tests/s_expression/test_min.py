import pandas as pd
import pytest

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

MIN_SEXP = """
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
MIN_ANSWER_DATA_SEXP = [[('2021', 31685.0)]]
MIN_ANSWER_INDEX_SEXP = [('x 1 000', 'Totaal vakanties')]
MIN_ANSWER_COLS_SEXP = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MIN[\'Perioden\']')]
MIN_ANSWER_DF_SEXP = pd.DataFrame(
    data=MIN_ANSWER_DATA_SEXP,
    index=pd.MultiIndex.from_tuples(MIN_ANSWER_INDEX_SEXP, names=['Unit', 'Measure']),
    columns=pd.Index(MIN_ANSWER_COLS_SEXP),
)

# SQL returns just min
MIN_ANSWER_DATA_SQL = [[31685.0]]
MIN_ANSWER_INDEX_SQL = [('Totaal vakanties', 'x 1 000')]
MIN_ANSWER_COLS_SQL = [('Totaal vakanties', 'Waarde', 'Totaal vakanties')]
MIN_ANSWER_DF_SQL = pd.DataFrame(
    data=MIN_ANSWER_DATA_SQL,
    index=pd.MultiIndex.from_tuples(MIN_ANSWER_INDEX_SQL, names=['Measure', 'Unit']),
    columns=pd.MultiIndex.from_tuples(MIN_ANSWER_COLS_SQL, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken']),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_min_sexp():
    """For this test an internet connection is required and the OData4 API must be live"""
    _, answer = eval(parse(MIN_SEXP))
    assert_frame_equal_unordered(answer, MIN_ANSWER_DF_SEXP)


def test_min_sexp_offline():
    _, answer = eval(parse(MIN_SEXP), offline=True)
    assert_frame_equal_unordered(answer, MIN_ANSWER_DF_SEXP)


def test_min_odata3_sql():
    _, answer = eval(parse(MIN_SEXP), sql=True, odata4=False)
    assert_frame_equal_unordered(answer, MIN_ANSWER_DF_SQL)


def test_min_odata4_sql():
    _, answer = eval(parse(MIN_SEXP), sql=True, odata4=True)
    assert_frame_equal_unordered(answer, MIN_ANSWER_DF_SQL)
