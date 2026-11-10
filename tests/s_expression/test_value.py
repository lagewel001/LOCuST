import pandas as pd
import pytest

import config
from s_expression.parser import parse, eval
from utils.answer_comparator import assert_frame_equal_unordered

SEXP = """
    (VALUE 85302NED
        (MSR (D004645))
        (DIM Perioden (2021JJ00 2022JJ00))
        (DIM BestemmingEnSeizoen (T001047))
        (DIM Vakantiekenmerken (T001460))
        (DIM Marges (MW00000))
    )
"""

ANSWER_INDEX = [('x 1 000', 'Totaal vakanties')]
ANSWER_COLS = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2021'),
               ('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2022')]
ANSWER_DATA = [[31685.0, 35933.0]]
ANSWER_DF = pd.DataFrame(
    data=ANSWER_DATA,
    index=pd.MultiIndex.from_tuples(ANSWER_INDEX, names=['Unit', 'Measure']),
    columns=pd.MultiIndex.from_tuples(ANSWER_COLS, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'Perioden']),
)

MAPPER_RESULT_ODATA = {'Vakantiekenmerken': 'Vakantiekenmerken', 'T001460': 'Totaal vakanties', 'T001047': 'Totaal vakanties', 'Perioden': 'Perioden', 'Marges': 'Marges', 'MW00000': 'Waarde', 'MOG0095': 'Ondergrens 95%-interval', 'MBG0095': 'Bovengrens 95%-interval', 'L999996': 'Vakantiebestemming: buitenland', 'L008691': 'Vakantiebestemming: Nederland', 'BestemmingEnSeizoen': 'Bestemming en seizoen', 'A048563': 'Soort vakantie: ontspanning', 'A047372': 'Soort vakantie: eigen recreatieve accom.', 'A047152': 'Vervoerwijze: overige', 'A047151': 'Vervoerwijze: vliegtuig', 'A047150': 'Vervoerwijze: auto', 'A046406': 'Organisatie: eigen recreatieve accom.', 'A046404': 'Organisatie: op de bonnefooi', 'A046403': 'Organisatie: alleen vervoer', 'A046402': 'Organisatie: alleen accommodatie', 'A046401': 'Organisatie: samengestelde reis', 'A046400': 'Organisatie: pakketreis', 'A046399': 'Organisatie: accommodatie en vervoer', 'A046397': 'Vakantieduur: 16 of meer dagen', 'A046396': 'Vakantieduur: 9 tot 16 dagen', 'A046395': 'Vakantieduur: 5 tot 9 dagen', 'A046394': 'Vakantieduur: 5 of meer dagen (lang)', 'A046393': 'Vakantieduur: 2 tot 5 dagen (kort)', 'A046391': 'Soort vakantie: overig, onbekend', 'A046390': 'Soort vakantie: familie, vrienden, e.d.', 'A046389': 'Soort vakantie: rondreis', 'A046388': 'Soort vakantie: recreatie', 'A046387': 'Soort vakantie: cultuur', 'A046386': 'Soort vakantie: natuur', 'A046385': 'Soort vakantie: steden', 'A046384': 'Soort vakantie: strand', 'A046378': 'Soort vakantie: actief', 'A046372': 'Reisgezelschap: vrienden, bekenden, e.a.', 'A046371': 'Reisgezelschap: familie, anderen', 'A046370': 'Reisgezelschap: volwassene en kind(eren)', 'A046369': 'Reisgezelschap: paar zonder kinderen', 'A046368': 'Reisgezelschap: alleenreizende', 'A046366': 'Accommodatie: overig, onbekend', 'A046363': 'Accommodatie: particulier (alle)', 'A046362': 'Accommodatie: kampeerverblijf', 'A046361': 'Accommodatie: appartement', 'A046360': 'Accommodatie: vakantiehuis, stacaravan', 'A046359': 'Accommodatie: hotel, pension, B&B', 'A045879': 'Winterseizoen', 'A045878': 'Zomerseizoen', 'A018981': 'Vervoerwijze: trein', '2022JJ00': '2022', '2021JJ00': '2021', 'M006089_2': 'Totaal vakantie-uitgaven', 'M005005_2': 'Percentage Nederlanders', 'M005005_1': 'Totaal Nederlanders', 'M004995': 'Gemiddeld per Nederlander', 'M004993': 'Gemiddeld per deelnemer', 'M004957': 'Gemiddeld per Nederlander', 'M004417': 'Gemiddeld per deelnemer', 'M003924': 'Gemiddeld per deelnemer', 'M003533_2': 'Totaal vakantiedagen', 'M002336': 'Gemiddeld per vakantie', 'M002334': 'Gemiddeld per Nederlander', 'M001963': 'Gemiddeld per persoon per vakantiedag', 'M001957': 'Gemiddeld per vakantie', 'M001860': 'Gemiddeld per Nederlander', 'M001844': 'Gemiddeld per deelnemer', 'M001339': 'Gemiddeld per persoon per vakantie', 'D006211_2': 'Totaal overnachtingen', 'D004645': 'Totaal vakanties', '85302NED': 'Vakanties van Nederlanders; kerncijfers'}
MAPPER_RESULT_SQL = {'Vakantiekenmerken': 'Vakantiekenmerken', 'T001460': 'Totaal vakanties', 'T001047': 'Totaal vakanties', 'Perioden': 'Perioden', 'Marges': 'Marges', 'MW00000': 'Waarde', 'MOG0095': 'Ondergrens 95%-interval', 'MBG0095': 'Bovengrens 95%-interval', 'L999996': 'Vakantiebestemming: buitenland', 'L008691': 'Vakantiebestemming: Nederland', 'BestemmingEnSeizoen': 'Bestemming en seizoen', 'A048563': 'Soort vakantie: ontspanning', 'A047372': 'Soort vakantie: eigen recreatieve accom.', 'A047152': 'Vervoerwijze: overige', 'A047151': 'Vervoerwijze: vliegtuig', 'A047150': 'Vervoerwijze: auto', 'A046406': 'Organisatie: eigen recreatieve accom.', 'A046404': 'Organisatie: op de bonnefooi', 'A046403': 'Organisatie: alleen vervoer', 'A046402': 'Organisatie: alleen accommodatie', 'A046401': 'Organisatie: samengestelde reis', 'A046400': 'Organisatie: pakketreis', 'A046399': 'Organisatie: accommodatie en vervoer', 'A046397': 'Vakantieduur: 16 of meer dagen', 'A046396': 'Vakantieduur: 9 tot 16 dagen', 'A046395': 'Vakantieduur: 5 tot 9 dagen', 'A046394': 'Vakantieduur: 5 of meer dagen (lang)', 'A046393': 'Vakantieduur: 2 tot 5 dagen (kort)', 'A046391': 'Soort vakantie: overig, onbekend', 'A046390': 'Soort vakantie: familie, vrienden, e.d.', 'A046389': 'Soort vakantie: rondreis', 'A046388': 'Soort vakantie: recreatie', 'A046387': 'Soort vakantie: cultuur', 'A046386': 'Soort vakantie: natuur', 'A046385': 'Soort vakantie: steden', 'A046384': 'Soort vakantie: strand', 'A046378': 'Soort vakantie: actief', 'A046372': 'Reisgezelschap: vrienden, bekenden, e.a.', 'A046371': 'Reisgezelschap: familie, anderen', 'A046370': 'Reisgezelschap: volwassene en kind(eren)', 'A046369': 'Reisgezelschap: paar zonder kinderen', 'A046368': 'Reisgezelschap: alleenreizende', 'A046366': 'Accommodatie: overig, onbekend', 'A046363': 'Accommodatie: particulier (alle)', 'A046362': 'Accommodatie: kampeerverblijf', 'A046361': 'Accommodatie: appartement', 'A046360': 'Accommodatie: vakantiehuis, stacaravan', 'A046359': 'Accommodatie: hotel, pension, B&B', 'A045879': 'Winterseizoen', 'A045878': 'Zomerseizoen', 'A018981': 'Vervoerwijze: trein', '2025JJ00': '2025', '2024JJ00': '2024', '2023JJ00': '2023', '2022JJ00': '2022', '2021JJ00': '2021', 'M006089_2': 'Totaal vakantie-uitgaven', 'M005005_2': 'Percentage Nederlanders', 'M005005_1': 'Totaal Nederlanders', 'M004995': 'Gemiddeld per Nederlander', 'M004993': 'Gemiddeld per deelnemer', 'M004957': 'Gemiddeld per Nederlander', 'M004417': 'Gemiddeld per deelnemer', 'M003924': 'Gemiddeld per deelnemer', 'M003533_2': 'Totaal vakantiedagen', 'M002336': 'Gemiddeld per vakantie', 'M002334': 'Gemiddeld per Nederlander', 'M001963': 'Gemiddeld per persoon per vakantiedag', 'M001957': 'Gemiddeld per vakantie', 'M001860': 'Gemiddeld per Nederlander', 'M001844': 'Gemiddeld per deelnemer', 'M001339': 'Gemiddeld per persoon per vakantie', 'D006211_2': 'Totaal overnachtingen', 'D004645': 'Totaal vakanties', '85302NED': 'Vakanties van Nederlanders; kerncijfers'}

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_value_sexp():
    """For this test an internet connection is required and the OData API must be live"""
    sexp, answer = eval(parse(SEXP))
    assert_frame_equal_unordered(answer, ANSWER_DF)
    assert sexp.mapper == MAPPER_RESULT_ODATA

def test_value_sexp_offline():
    sexp, answer = eval(parse(SEXP), offline=True)
    assert_frame_equal_unordered(answer, ANSWER_DF)
    assert sexp.mapper == MAPPER_RESULT_SQL

def test_value_pivot_sql():
    sexp, answer = eval(parse(SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, ANSWER_DF)
    assert sexp.mapper == MAPPER_RESULT_SQL

def test_value_sql():
    expected_answer = pd.DataFrame({'Bestemming en seizoen': ['Totaal vakanties'] * 2, 'Perioden': ['2021', '2022'],
                                    'Marges': ['Waarde'] * 2, 'Vakantiekenmerken': ['Totaal vakanties'] * 2,
                                    'Totaal vakanties': [31685.0, 35933.0]})

    # Check for identical tables with different ordering
    expected_answer_changed_order = pd.DataFrame({'Bestemming en seizoen': ['Totaal vakanties'] * 2, 'Perioden': ['2022', '2021'],
                                    'Marges': ['Waarde'] * 2, 'Vakantiekenmerken': ['Totaal vakanties'] * 2,
                                    'Totaal vakanties': [35933.0, 31685.0]})

    sexp, answer = eval(parse(SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)
    assert_frame_equal_unordered(answer, expected_answer_changed_order)
    assert sexp.mapper == MAPPER_RESULT_SQL


COND_SEXP = """
    (VALUE 85302NED
        (MSR (D004645 < 35000))
        (DIM Perioden (2021JJ00 2022JJ00 2023JJ00 2024JJ00))
        (DIM BestemmingEnSeizoen (T001047))
        (DIM Vakantiekenmerken (T001460))
        (DIM Marges (MW00000))
    )
"""

COND_ANSWER_INDEX = [('x 1 000', 'Totaal vakanties')]
COND_ANSWER_COLS = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2021')]
COND_ANSWER_DATA = [[31685.0]]
COND_ANSWER_DF = pd.DataFrame(
    data=COND_ANSWER_DATA,
    index=pd.MultiIndex.from_tuples(COND_ANSWER_INDEX, names=['Unit', 'Measure']),
    columns=pd.MultiIndex.from_tuples(COND_ANSWER_COLS, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'Perioden']),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_value_sexp_conditional():
    _, answer = eval(parse(COND_SEXP))
    assert_frame_equal_unordered(answer, COND_ANSWER_DF)

def test_value_sql_conditional():
    expected_answer = pd.DataFrame({'Bestemming en seizoen': ['Totaal vakanties'], 'Perioden': ['2021'],
                                    'Marges': ['Waarde'], 'Vakantiekenmerken': ['Totaal vakanties'],
                                    'Totaal vakanties': [31685.0]})
    _, answer = eval(parse(COND_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_value_pivot_sql_conditional():
    _, answer = eval(parse(COND_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, COND_ANSWER_DF)

def test_value_sql_conditional_without_dimension_filters():
    """A measure filter must start the WHERE clause when there are no dimension filters"""
    sexp, answer = eval(parse("""
        (VALUE 85302NED
            (MSR (D004645 < 35000))
            (DIM Perioden ())
        )
    """), sql=True)
    assert 'WHERE\n  "D004645" < 35000' in sexp.odata3_sql
    assert len(answer) > 0 and (answer['Totaal vakanties'] < 35000).all()


# == UNIT TESTS FOR BETWEEN-RANGE MEASURE FILTERS ==
BETWEEN_SEXP = """
    (VALUE 85302NED
        (MSR (31000 < D004645 <= 35933))
        (DIM Perioden (2021JJ00 2022JJ00 2023JJ00))
        (DIM BestemmingEnSeizoen (T001047))
        (DIM Vakantiekenmerken (T001460))
        (DIM Marges (MW00000))
    )
"""

BETWEEN_ANSWER_COLS = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2021'),
                       ('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2022')]
BETWEEN_ANSWER_DF = pd.DataFrame(
    data=[[31685.0, 35933.0]],  # 2023 (37554) exceeds the inclusive upper bound
    index=pd.MultiIndex.from_tuples([('x 1 000', 'Totaal vakanties')], names=['Unit', 'Measure']),
    columns=pd.MultiIndex.from_tuples(BETWEEN_ANSWER_COLS, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'Perioden']),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_value_between_sexp():
    _, answer = eval(parse(BETWEEN_SEXP))
    assert_frame_equal_unordered(answer, BETWEEN_ANSWER_DF)

def test_value_between_pivot_sql():
    _, answer = eval(parse(BETWEEN_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, BETWEEN_ANSWER_DF)

def test_value_between_sql():
    expected_answer = pd.DataFrame({'Bestemming en seizoen': ['Totaal vakanties'] * 2, 'Perioden': ['2021', '2022'],
                                    'Marges': ['Waarde'] * 2, 'Vakantiekenmerken': ['Totaal vakanties'] * 2,
                                    'Totaal vakanties': [31685.0, 35933.0]})
    _, answer = eval(parse(BETWEEN_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_value_between_invalid_bounds():
    with pytest.raises((ValueError, SyntaxError)):
        eval(parse(BETWEEN_SEXP.replace('31000 < D004645 <= 35933', '35933 < D004645 <= 31000')))

def test_value_between_invalid_operator():
    with pytest.raises((ValueError, SyntaxError)):
        eval(parse(BETWEEN_SEXP.replace('31000 < D004645 <= 35933', '31000 > D004645 <= 35933')))


# == UNIT TESTS FOR COMBINED (AND/OR) MEASURE FILTERS ==
AND_SEXP = """
    (VALUE 85302NED
        (MSR (D004645 > 32000) AND (M005005_1 < 11900))
        (DIM Perioden (2021JJ00 2022JJ00 2023JJ00))
        (DIM BestemmingEnSeizoen (T001047))
        (DIM Vakantiekenmerken (T001460))
        (DIM Marges (MW00000))
    )
"""

AND_ANSWER_DF = pd.DataFrame(
    data=[[11683.0], [35933.0]],  # only 2022 satisfies both conditions; both measures returned
    index=pd.MultiIndex.from_tuples([('x 1 000', 'Totaal Nederlanders'), ('x 1 000', 'Totaal vakanties')],
                                    names=['Unit', 'Measure']),
    columns=pd.MultiIndex.from_tuples([('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2022')],
                                      names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'Perioden']),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_value_and_filter_sexp():
    _, answer = eval(parse(AND_SEXP))
    assert_frame_equal_unordered(answer, AND_ANSWER_DF)

def test_value_and_filter_sql():
    expected_answer = pd.DataFrame({'Bestemming en seizoen': ['Totaal vakanties'], 'Perioden': ['2022'],
                                    'Marges': ['Waarde'], 'Vakantiekenmerken': ['Totaal vakanties'],
                                    'Totaal Nederlanders': [11683.0], 'Totaal vakanties': [35933.0]})
    _, answer = eval(parse(AND_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_value_and_filter_pivot_sql():
    _, answer = eval(parse(AND_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, AND_ANSWER_DF)


OR_SEXP = """
    (VALUE 85302NED
        (MSR (D004645 < 32000) OR (M005005_1 > 11900))
        (DIM Perioden (2021JJ00 2022JJ00 2023JJ00))
        (DIM BestemmingEnSeizoen (T001047))
        (DIM Vakantiekenmerken (T001460))
        (DIM Marges (MW00000))
    )
"""

OR_ANSWER_COLS = [('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2021'),
                  ('Totaal vakanties', 'Waarde', 'Totaal vakanties', '2023')]
OR_ANSWER_DF = pd.DataFrame(
    data=[[11144.0, 11991.0], [31685.0, 37554.0]],  # 2021 satisfies the first, 2023 the second condition
    index=pd.MultiIndex.from_tuples([('x 1 000', 'Totaal Nederlanders'), ('x 1 000', 'Totaal vakanties')],
                                    names=['Unit', 'Measure']),
    columns=pd.MultiIndex.from_tuples(OR_ANSWER_COLS, names=['Bestemming en seizoen', 'Marges', 'Vakantiekenmerken', 'Perioden']),
)

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_value_or_filter_sexp():
    _, answer = eval(parse(OR_SEXP))
    assert_frame_equal_unordered(answer, OR_ANSWER_DF)

def test_value_or_filter_sql():
    expected_answer = pd.DataFrame({'Bestemming en seizoen': ['Totaal vakanties'] * 2, 'Perioden': ['2021', '2023'],
                                    'Marges': ['Waarde'] * 2, 'Vakantiekenmerken': ['Totaal vakanties'] * 2,
                                    'Totaal Nederlanders': [11144.0, 11991.0], 'Totaal vakanties': [31685.0, 37554.0]})
    _, answer = eval(parse(OR_SEXP), sql=True)
    assert_frame_equal_unordered(answer, expected_answer)

def test_value_or_filter_pivot_sql():
    _, answer = eval(parse(OR_SEXP), sql=True, pivot=True)
    assert_frame_equal_unordered(answer, OR_ANSWER_DF)

def test_value_mixed_connectives_raises():
    with pytest.raises((ValueError, SyntaxError)):
        eval(parse("""
            (VALUE 85302NED
                (MSR (D004645 > 32000) AND (M005005_1 < 11900) OR (M005005_1 > 11000))
                (DIM Perioden (2021JJ00))
            )
        """))


def test_native_filter_observations():
    """The native (OData API) measure filters are applied client-side; test the filter logic in isolation."""
    from pipeline.odata_executor import ODataExecutor

    observations = [
        {'ID': i, 'Perioden': p, 'Measure': m, 'Value': v}
        for i, (p, m, v) in enumerate([('2021JJ00', 'D004645', 31685.0), ('2021JJ00', 'M005005_1', 11144.0),
                                       ('2022JJ00', 'D004645', 35933.0), ('2022JJ00', 'M005005_1', 11683.0),
                                       ('2023JJ00', 'D004645', 37554.0), ('2023JJ00', 'M005005_1', 11991.0)])
    ]

    executor = ODataExecutor.__new__(ODataExecutor)  # bypass __init__ to avoid graph loading

    # Single condition: only observations of the filtered measure that satisfy it survive
    executor.measure_filters = [('D004645', '<', 35000.0)]
    executor.filter_connective = 'AND'
    assert [obs['ID'] for obs in executor._filter_observations(observations)] == [0]

    # Between-range: two AND conditions on the same measure
    executor.measure_filters = [('D004645', '>', 31000.0), ('D004645', '<=', 35933.0)]
    assert [obs['ID'] for obs in executor._filter_observations(observations)] == [0, 1, 2, 3]

    # AND over different measures: only contexts satisfying both conditions survive (all their measures kept)
    executor.measure_filters = [('D004645', '>', 32000.0), ('M005005_1', '<', 11900.0)]
    assert [obs['ID'] for obs in executor._filter_observations(observations)] == [2, 3]

    # OR over different measures
    executor.measure_filters = [('D004645', '<', 32000.0), ('M005005_1', '>', 11900.0)]
    executor.filter_connective = 'OR'
    assert [obs['ID'] for obs in executor._filter_observations(observations)] == [0, 1, 4, 5]


def test_value_rejects_measure_group():
    """A MeasureGroup id looks like a measure code but would silently yield an empty answer."""
    with pytest.raises(ValueError, match=r"'M003533_1' is a MeasureGroup of table '85302NED'") as e:
        eval(parse("(VALUE 85302NED (MSR (M003533_1)) (DIM Perioden (2021JJ00)))"), sql=True)
    # the group's member measures are suggested, so the LLM can correct its selection
    assert all(m in str(e.value) for m in ('M003533_2', 'M002336', 'M002334', 'M001844'))


def test_value_rejects_nested_measure_group(sparql_engine):
    """A parent group that only contains sub-groups still resolves to the measures below them."""
    from s_expression import Table, Measure
    with pytest.raises(ValueError, match=r"'A042220_1' is a MeasureGroup .* \['A042220_2'"):
        sparql_engine.validate_table_measures(Table('80781ned'), {Measure('A042220_1')})


def test_value_rejects_unknown_measure():
    with pytest.raises(ValueError, match=r"'M999999' is not a measure of table '85302NED'"):
        eval(parse("(VALUE 85302NED (MSR (D004645 M999999)) (DIM Perioden (2021JJ00)))"), sql=True)
