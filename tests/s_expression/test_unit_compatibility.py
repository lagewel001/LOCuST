import pytest

import config
from s_expression.parser import parse, eval
from utils.custom_types import UnitCompatibilityError

# == UNIT TESTS FOR JOIN OVER DIFFERENT MEASURES UNITS  ==
# M004367 has unit qudt:KiloGM-PER-KiloM
# M004369 has unit qudt:TONNE
FAIL_ON_UNITS_SEXP = """
    (SUM ()
        (VALUE 84957NED
            (MSR (M004367 M004369))
            (DIM Perioden (2015JJ00 2016JJ00 2017JJ00))
            (DIM Vervoerstromen (T001448))
        )
    )
"""

@pytest.mark.skipif(config.ENV == 'devops', reason="Test for local use only")
def test_unit_compatibility_sexp():
    with pytest.raises(UnitCompatibilityError,
                       match="Trying to aggregate over measures with a different unit"):
        eval(parse(FAIL_ON_UNITS_SEXP))

def test_unit_compatibility_sexp_offline():
    with pytest.raises(UnitCompatibilityError,
                       match="Trying to aggregate over measures with a different unit"):
        eval(parse(FAIL_ON_UNITS_SEXP), offline=True)

@pytest.mark.parametrize('pivot', [False, True])
def test_unit_compatibility_sql(pivot):
    with pytest.raises(UnitCompatibilityError,
                       match="Trying to aggregate over measures with a different unit"):
        eval(parse(FAIL_ON_UNITS_SEXP), sql=True, pivot=pivot)


# == UNIT TESTS FOR AVG OVER DIMENSIONLESS UNITS  ==
FAIL_ON_DIMENSIONLESS_SEXP = """
    (AVG ()
        (VALUE 85302NED
            (MSR (D004645 M005005_2))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (L008691 L999996))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

def test_unit_dimensionless_sexp():
    with pytest.raises(UnitCompatibilityError,
                       match="Trying to aggregate over two or more measures with a dimensionless unit"):
        eval(parse(FAIL_ON_DIMENSIONLESS_SEXP))

def test_unit_dimensionless_sexp_offline():
    with pytest.raises(UnitCompatibilityError,
                       match="Trying to aggregate over two or more measures with a dimensionless unit"):
        eval(parse(FAIL_ON_DIMENSIONLESS_SEXP), offline=True)

@pytest.mark.parametrize('pivot', [False, True])
def test_unit_dimensionless_sql(pivot):
    with pytest.raises(UnitCompatibilityError,
                       match="Trying to aggregate over two or more measures with a dimensionless unit"):
        eval(parse(FAIL_ON_DIMENSIONLESS_SEXP), sql=True, pivot=pivot)


# == UNIT TESTS FOR SUM OVER DIFFERENT CONVERTION RATIOS  ==
FAIL_ON_CONVERSION_SEXP = """
    (SUM ()
        (VALUE 85302NED
            (MSR (D004645 D006211_2))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (L008691 L999996))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
"""

@pytest.mark.parametrize('pivot', [False, True])
def test_conversion_compatibility_sql(pivot):
    with pytest.raises(UnitCompatibilityError,
                       match="Trying to aggregate over multiple measures with different conversion multipliers"):
        eval(parse(FAIL_ON_CONVERSION_SEXP), sql=True, pivot=pivot)


# == AGGREGATING OVER A DIMENSION KEEPS THE MEASURES APART ==
# Same measures as FAIL_ON_UNITS_SEXP, but summed over Perioden: every measure is aggregated on its own,
# so their different units never mix and the unit gate must not fire (as in native mode).
PASS_ON_DIM_AGGREGATION_SEXP = """
    (SUM (Perioden)
        (VALUE 84957NED
            (MSR (M004367 M004369))
            (DIM Perioden (2015JJ00 2016JJ00 2017JJ00))
            (DIM Vervoerstromen (T001448))
        )
    )
"""

@pytest.mark.parametrize('pivot', [False, True])
def test_unit_gate_skips_dimension_aggregation_sql(pivot):
    _, answer = eval(parse(PASS_ON_DIM_AGGREGATION_SEXP), sql=True, pivot=pivot)
    assert not answer.empty


# == THE STATIC VALIDATE() WALK INCLUDES THE UNIT GATE ==
# eval'ing the inner parsed list constructs the tree WITHOUT executing it (no DB/OData query)

@pytest.mark.parametrize('sexp, message', [
    (FAIL_ON_UNITS_SEXP, "Trying to aggregate over measures with a different unit"),
    (FAIL_ON_DIMENSIONLESS_SEXP, "Trying to aggregate over two or more measures with a dimensionless unit"),
    (FAIL_ON_CONVERSION_SEXP, "Trying to aggregate over multiple measures with different conversion multipliers"),
])
def test_static_validate_blocks_incompatible_units(sexp, message):
    with pytest.raises(UnitCompatibilityError, match=message):
        eval(parse(sexp)[0]).validate()


def test_static_validate_skips_units_for_dimension_aggregation():
    eval(parse(PASS_ON_DIM_AGGREGATION_SEXP)[0]).validate()
