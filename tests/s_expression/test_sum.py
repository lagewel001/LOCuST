import pandas as pd
import pytest

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

# == UNIT TESTS FOR JOIN OVER MEASURES ==
SUM_ON_MSR_SEXP = """
    (SUM ()
        (VALUE 80781ned
            (MSR (A028997_1 A028994_1))
            (DIM Perioden (2020JJ00 2021JJ00 2022JJ00))
            (DIM RegioS (NL01))
        )
    )
"""

SUM_ON_MSR_ANSWER_INDEX_SEXP = ['Biggen\nLeghennen']
SUM_ON_MSR_ANSWER_INDEX_PIVOT_SQL = [('Biggen;Leghennen', '')]
SUM_ON_MSR_ANSWER_COLS = [('Nederland', '2020'), ('Nederland','2021'), ('Nederland','2022')]
SUM_ON_MSR_ANSWER_DATA = [[48579668., 48329610., 47359417.]]
SUM_ON_MSR_ANSWER_DF_SEXP = pd.DataFrame(
    data=SUM_ON_MSR_ANSWER_DATA,
    index=pd.Index(SUM_ON_MSR_ANSWER_INDEX_SEXP),
    columns=pd.MultiIndex.from_tuples(SUM_ON_MSR_ANSWER_COLS, names=['Regio', 'Perioden']),
)
SUM_ON_MSR_ANSWER_DF_PIVOT_SQL = pd.DataFrame(
    data=SUM_ON_MSR_ANSWER_DATA,
    index=pd.Index(SUM_ON_MSR_ANSWER_INDEX_PIVOT_SQL),
    columns=pd.MultiIndex.from_tuples(SUM_ON_MSR_ANSWER_COLS, names=['Regio', 'Perioden']),
)
SUM_ON_MSR_ANSWER_DF_PIVOT_SQL.index.set_names('Measure', level=0, inplace=True)
SUM_ON_MSR_ANSWER_DF_PIVOT_SQL.index.set_names('Unit', level=1, inplace=True)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_sum_on_msr_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(SUM_ON_MSR_SEXP))
    assert_frame_equal_unordered(answer, SUM_ON_MSR_ANSWER_DF_SEXP)

def test_sum_on_msr_sexp_offline():
    _, answer = eval(parse(SUM_ON_MSR_SEXP), offline=True)
    assert_frame_equal_unordered(answer, SUM_ON_MSR_ANSWER_DF_SEXP)

def test_sum_on_msr_sql():
    expected_answer = pd.DataFrame({'Perioden': ['2020', '2021', '2022'],
                                    "Regio's": ['Nederland'] * 3,
                                    'SUM': [48579668., 48329610., 47359417.]}, index=range(3))
    _, answer = eval(parse(SUM_ON_MSR_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_sum_on_msr_pivot_sql():
    _, answer = eval(parse(SUM_ON_MSR_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, SUM_ON_MSR_ANSWER_DF_PIVOT_SQL)


# == UNIT TESTS FOR SUM OVER DIMENSION GROUP ==
SUM_ON_DIM_SEXP = """
    (SUM
        (Perioden)
        (VALUE 85302NED
            (MSR (D004645 M005005_1))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (L008691 L999996))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

SUM_ON_DIM_ANSWER_INDEX = [('Totaal Nederlanders', 'x 1 000'), ('Totaal vakanties', 'x 1 000')]
SUM_ON_DIM_ANSWER_COLS = [('Vakantiebestemming: Nederland', 'Waarde', 'Totaal vakanties'),
                          ('Vakantiebestemming: buitenland', 'Waarde', 'Totaal vakanties')]
SUM_ON_DIM_ANSWER_DATA = [[16749.0, 15670.0],
                          [37938.0, 29681.0]]
SUM_ON_DIM_ANSWER_DF = pd.DataFrame(
    data=SUM_ON_DIM_ANSWER_DATA,
    index=pd.Index(SUM_ON_DIM_ANSWER_INDEX).set_names(['Measure', 'Unit']),
    columns=SUM_ON_DIM_ANSWER_COLS,
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_sum_on_dim_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    sum_cols = [('Vakantiebestemming: Nederland', 'Waarde', 'Totaal vakanties', 'SUM[\'Perioden\']'),
                ('Vakantiebestemming: buitenland', 'Waarde', 'Totaal vakanties', 'SUM[\'Perioden\']')]
    df = SUM_ON_DIM_ANSWER_DF.copy()
    df.columns = sum_cols
    _, answer = eval(parse(SUM_ON_DIM_SEXP))
    assert_frame_equal_unordered(answer, df)

def test_sum_on_dim_sexp_offline():
    sum_cols = [('Vakantiebestemming: Nederland', 'Waarde', 'Totaal vakanties', 'SUM[\'Perioden\']'),
                ('Vakantiebestemming: buitenland', 'Waarde', 'Totaal vakanties', 'SUM[\'Perioden\']')]
    df = SUM_ON_DIM_ANSWER_DF.copy()
    df.columns = sum_cols
    _, answer = eval(parse(SUM_ON_DIM_SEXP), offline=True)
    assert_frame_equal_unordered(answer, df)

def test_sum_on_dim_sql():
    # Summing over Perioden collapses the periods, each measure is summed separately (identical to the pivoted SQL)
    expected_answer = pd.DataFrame({'Bestemming en seizoen': ['Vakantiebestemming: Nederland', 'Vakantiebestemming: buitenland'],
                                    'Marges': ['Waarde'] * 2,
                                    'Vakantiekenmerken': ['Totaal vakanties'] * 2,
                                    'SUM[D004645]': [37938., 29681.],
                                    'SUM[M005005_1]': [16749., 15670.]}, index=range(2))
    _, answer = eval(parse(SUM_ON_DIM_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_sum_on_dim_pivot_sql():
    _, answer = eval(parse(SUM_ON_DIM_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, SUM_ON_DIM_ANSWER_DF)


# == UNIT TESTS FOR SUM OVER COMPLEX INNER EXPRESSION ==
SUM_ON_JOIN_SEXP = """
    (SUM ()
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
    )
"""

SUM_ON_JOIN_ANSWER_INDEX = [('Ladingtonkilometer', 'mln tonkm')]
SUM_ON_JOIN_ANSWER_COLS = ['SUM']
SUM_ON_JOIN_ANSWER_DATA = [[57987.0]]
SUM_ON_JOIN_ANSWER_DF = pd.DataFrame(
    data=SUM_ON_JOIN_ANSWER_DATA,
    index=pd.Index(SUM_ON_JOIN_ANSWER_INDEX).set_names(['Measure', 'Unit']),
    columns=pd.Index(SUM_ON_JOIN_ANSWER_COLS),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_sum_on_join_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    _, answer = eval(parse(SUM_ON_JOIN_SEXP))
    df = SUM_ON_JOIN_ANSWER_DF.copy()
    df.index = pd.Index(["('Ladingtonkilometer', 'mln tonkm')"], name='Measure')
    df.columns = pd.Index([('Ladingtonkilometer', 'mln tonkm')])
    assert_frame_equal_unordered(answer, df)

def test_sum_on_join_sexp_offline():
    _, answer = eval(parse(SUM_ON_JOIN_SEXP), offline=True)
    df = SUM_ON_JOIN_ANSWER_DF.copy()
    df.index = pd.Index(["('Ladingtonkilometer', 'mln tonkm')"], name='Measure')
    df.columns = pd.Index([('Ladingtonkilometer', 'mln tonkm')])
    assert_frame_equal_unordered(answer, df)

def test_sum_on_join_sql():
    expected_answer = pd.DataFrame({'Measure': ['Ladingtonkilometer'], 'SUM': [57987.]}, index=range(1))
    _, answer = eval(parse(SUM_ON_JOIN_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_sum_on_join_sql_keeps_equal_values():
    """Both operands contain the (identical) 2021 value; joining over the measure must keep them both"""
    sexp = SUM_ON_JOIN_SEXP.replace('(DIM Perioden (2016JJ00 2017JJ00))', '(DIM Perioden (2021JJ00))')
    _, answer = eval(parse(sexp), sql=True)
    assert answer['SUM'].item() == 11003. + 9659. + 11003.

def test_sum_on_join_pivot_sql():
    _, answer = eval(parse(SUM_ON_JOIN_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, SUM_ON_JOIN_ANSWER_DF)
