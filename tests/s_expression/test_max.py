import pandas as pd
import pytest

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

MAX_SEXP = """
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
MAX_ANSWER_DATA_SEXP = [[('2022', 35933.0)]]
MAX_ANSWER_INDEX_SEXP = [('Totaal vakanties', 'x 1 000')]
MAX_ANSWER_COLS_SEXP = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', 'MAX[\'Perioden\']')]
MAX_ANSWER_DF_SEXP = pd.DataFrame(
    data=MAX_ANSWER_DATA_SEXP,
    index=pd.MultiIndex.from_tuples(MAX_ANSWER_INDEX_SEXP, names=['Measure', 'Unit']),
    columns=pd.Index(MAX_ANSWER_COLS_SEXP),
)

# SQL returns just max
MAX_ANSWER_DATA_SQL = [[35933.0]]
MAX_ANSWER_INDEX_SQL = [('Totaal vakanties', 'x 1 000')]
MAX_ANSWER_COLS_SQL = [('Totaal vakanties', 'Waarde', 'Totaal vakanties')]
MAX_ANSWER_DF_SQL = pd.DataFrame(
    data=MAX_ANSWER_DATA_SQL,
    index=pd.MultiIndex.from_tuples(MAX_ANSWER_INDEX_SQL, names=['Measure', 'Unit']),
    columns=pd.MultiIndex.from_tuples(MAX_ANSWER_COLS_SQL, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken']),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_max_sexp():
    """For this test an internet connection is required and the OData4 API must be live"""
    _, answer = eval(parse(MAX_SEXP))
    assert_frame_equal_unordered(answer, MAX_ANSWER_DF_SEXP)

def test_max_sexp_offline():
    _, answer = eval(parse(MAX_SEXP), offline=True)
    assert_frame_equal_unordered(answer, MAX_ANSWER_DF_SEXP)

def test_max_odata3_sql():
    _, answer = eval(parse(MAX_SEXP), sql=True, odata4=False)
    assert_frame_equal_unordered(answer, MAX_ANSWER_DF_SQL)

def test_max_odata4_sql():
    _, answer = eval(parse(MAX_SEXP), sql=True, odata4=True)
    assert_frame_equal_unordered(answer, MAX_ANSWER_DF_SQL)
