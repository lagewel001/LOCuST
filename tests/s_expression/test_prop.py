import pandas as pd
import pytest

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

PROP_SEXP = """
    (PROP
        (DIM Perioden (2021JJ00))
        (VALUE 85302NED
            (MSR (D004645))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

PROP_ANSWER_DATA = [[67618., 31685., 46.8588]]

PROP_ANSWER_INDEX_SEXP = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', "PROP['Perioden']")]
PROP_ANSWER_COLS_SEXP = [('Totaal vakanties', 'x 1 000'), ('2021', ''), ('%', '')]
PROP_ANSWER_DF_SEXP = pd.DataFrame(
    data=PROP_ANSWER_DATA,
    index=pd.Index(PROP_ANSWER_INDEX_SEXP).to_flat_index(),
    columns=pd.Index(PROP_ANSWER_COLS_SEXP),
)

PROP_ANSWER_INDEX_PIVOT_SQL = [('Totaal vakanties, Waarde, Totaal vakanties, Totaal vakanties', 'x 1 000')]
PROP_ANSWER_COLS_PIVOT_SQL = ["SUM[Perioden]", '2021', '%']
PROP_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=PROP_ANSWER_DATA,
    index=pd.MultiIndex.from_tuples(PROP_ANSWER_INDEX_PIVOT_SQL, names=['Measure', 'Unit']),
    columns=pd.Index(PROP_ANSWER_COLS_PIVOT_SQL)
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_prop_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(PROP_SEXP))
    assert_frame_equal_unordered(answer, PROP_ANSWER_DF_SEXP)


def test_prop_sexp_offline():
    _, answer = eval(parse(PROP_SEXP), offline=True)
    assert_frame_equal_unordered(answer, PROP_ANSWER_DF_SEXP)


def test_prop_sql():
    # PROP has no separate non-pivot SQL; the non-pivoted execution returns its flat result table
    expected_answer = pd.DataFrame({'Measure': ['Totaal vakanties, Waarde, Totaal vakanties, Totaal vakanties'],
                                    'SUM[Perioden]': [67618.], '2021': [31685.], '%': [46.8588]})
    _, answer = eval(parse(PROP_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer, rtol=1e-4)


def test_prop_pivot_sql():
    _, answer = eval(parse(PROP_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, PROP_ANSWER_DF_PIVOT_SQL, rtol=1e-4)


# == UNIT TESTS FOR PROP WITH MEASURE FILTERS ==
# 85302NED holidays (T001047/T001460/MW00000): D004645 is 31685 (2021) and 35933 (2022), D006211_2 is 205.2 and
# 265.4. Filters apply before the proportion is taken (as in native mode), so a period failing the filter drops
# out of the total, and out of the numerator when it is the selected code.
PROP_FILTER_TEMPLATE = """
    (PROP
        (DIM Perioden ({target}))
        (VALUE 85302NED
            {msr}
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

@pytest.mark.parametrize('pivot', [False, True])
@pytest.mark.parametrize('target, expected', [
    ('2022JJ00', {'SUM[Perioden]': [35933.], '%': [100.]}),   # 2021 (31685) fails the filter
    ('2021JJ00', {'SUM[Perioden]': [35933.], '%': [0.]}),     # the selected code itself is filtered out
])
def test_prop_single_measure_filter_sql(pivot, target, expected):
    sexp = PROP_FILTER_TEMPLATE.format(target=target, msr='(MSR (D004645 > 32000))')
    _, answer = eval(parse(sexp), sql=True, pivot=pivot)
    assert {c: answer[c].tolist() for c in expected} == expected


@pytest.mark.parametrize('pivot', [False, True])
def test_prop_combined_measure_filter_sql(pivot):
    """An AND-filter over two measures keeps only the rows where both hold (here: 2022), for both measures."""
    sexp = PROP_FILTER_TEMPLATE.format(target='2022JJ00', msr='(MSR (D004645 > 0) AND (D006211_2 > 250))')
    _, answer = eval(parse(sexp), sql=True, pivot=pivot)
    assert sorted(answer['SUM[Perioden]'].tolist()) == [265.4, 35933.]
    assert answer['%'].tolist() == [100., 100.]
