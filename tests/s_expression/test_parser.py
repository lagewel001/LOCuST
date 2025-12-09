import pytest

from s_expression.parser import parse


def test_parser(sparql_engine):
    sexp = """(VALUE 85302NED
        (MSR (D004645))
        (DIM Perioden (2021JJ00 2022JJ00))
        (DIM BestemmingEnSeizoen (T001047))
        (DIM Vakantiekenmerken (T001460))
    )
    """
    parsed_sexp = parse(sexp)
    assert parsed_sexp == [['VALUE', '85302NED', ['MSR', ['D004645']], ['DIM', 'Perioden', ['2021JJ00', '2022JJ00']],
                            ['DIM', 'BestemmingEnSeizoen', ['T001047']], ['DIM', 'Vakantiekenmerken', ['T001460']]]]


def test_bracket_nesting(sparql_engine):
    with pytest.raises(AssertionError):
        parse("""(VALUE 85302NED
            (MSR (D004645))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (T001047)
            (DIM Vakantiekenmerken (T001460))
        )
        """)
