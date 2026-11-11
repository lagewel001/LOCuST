import pandas as pd
from evaluation.metrics import numeric_recall, numeric_f1


def test_exact_match():
    gt = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    pred = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    assert numeric_recall(gt, pred) == 1.0


def test_no_match():
    gt = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    pred = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
    assert numeric_recall(gt, pred) == 0.0


def test_partial_match():
    gt = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    pred = pd.DataFrame({'A': [1, 3], 'B': [9, 9]})
    assert numeric_recall(gt, pred) == 0.5


def test_ignores_row_structure():
    """Values swapped across rows still count as matched."""
    gt = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    pred = pd.DataFrame({'A': [2, 1], 'B': [4, 3]})
    assert numeric_recall(gt, pred) == 1.0


def test_ignores_strings():
    gt = pd.DataFrame({'A': ['foo', 1], 'B': ['bar', 2]})
    pred = pd.DataFrame({'X': ['baz', 1], 'Y': ['qux', 2]})
    assert numeric_recall(gt, pred) == 1.0


def test_transposed_format():
    """Wide vs long format produces the same numeric bag."""
    gt = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    pred = pd.DataFrame({'X': [1, 4], 'Y': [2, 5], 'Z': [3, 6]})
    assert numeric_recall(gt, pred) == 1.0


def test_nan_ignored():
    gt = pd.DataFrame({'A': [1, float('nan')], 'B': [3, 4]})
    pred = pd.DataFrame({'A': [1, 3, 4]})
    assert numeric_recall(gt, pred) == 1.0


def test_duplicate_values():
    gt = pd.DataFrame({'A': [1, 1, 2]})
    pred = pd.DataFrame({'A': [1, 2, 3]})
    assert numeric_recall(gt, pred) == 2 / 3


def test_empty_ground_truth():
    assert numeric_recall(pd.DataFrame(), pd.DataFrame({'A': [1]})) == 0.0
    assert numeric_recall(pd.DataFrame(), pd.DataFrame()) == 1.0


def test_empty_predicted():
    gt = pd.DataFrame({'A': [1, 2]})
    assert numeric_recall(gt, pd.DataFrame()) == 0.0


def test_only_strings_in_gt():
    gt = pd.DataFrame({'A': ['foo', 'bar']})
    pred = pd.DataFrame({'A': ['foo', 'bar']})
    assert numeric_recall(gt, pred) == 1.0


def test_only_strings_in_gt_mismatch():
    gt = pd.DataFrame({'A': ['foo', 'bar']})
    pred = pd.DataFrame({'A': [1, 2]})
    assert numeric_recall(gt, pred) == 0.0


def test_f1_exact_match():
    gt = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    pred = pd.DataFrame({'A': [2, 1], 'B': [4, 3]})
    assert numeric_f1(gt, pred) == 1.0


def test_f1_penalizes_extra_values():
    """Recall stays 1 when the prediction returns too much, F1 does not (precision 2/4)."""
    gt = pd.DataFrame({'A': [1, 2]})
    pred = pd.DataFrame({'A': [1, 2, 3, 4]})
    assert numeric_recall(gt, pred) == 1.0
    assert numeric_f1(gt, pred) == 2 * 0.5 * 1.0 / 1.5


def test_f1_duplicate_values():
    """Precision 2/4 (one extra 1, one 3), recall 2/3."""
    gt = pd.DataFrame({'A': [1, 1, 2]})
    pred = pd.DataFrame({'A': [1, 2, 2, 3]})
    precision, recall = 2 / 4, 2 / 3
    assert numeric_f1(gt, pred) == 2 * precision * recall / (precision + recall)


def test_f1_no_match():
    gt = pd.DataFrame({'A': [1, 2]})
    pred = pd.DataFrame({'A': [5, 6]})
    assert numeric_f1(gt, pred) == 0.0


def test_f1_empty_frames():
    gt = pd.DataFrame({'A': [1, 2]})
    assert numeric_f1(gt, pd.DataFrame()) == 0.0
    assert numeric_f1(pd.DataFrame(), pd.DataFrame({'A': [1]})) == 0.0
    assert numeric_f1(pd.DataFrame(), pd.DataFrame()) == 1.0


def test_f1_no_numeric_values_predicted():
    gt = pd.DataFrame({'A': [1, 2]})
    pred = pd.DataFrame({'A': ['foo', 'bar']})
    assert numeric_f1(gt, pred) == 0.0


def test_nullable_integer_columns():
    """Nullable Int64 columns (with NA) yield NumPy integers, which must be counted as well."""
    gt = pd.DataFrame({'A': pd.array([11991, None], dtype='Int64'), 'B': pd.array([None, 7944], dtype='Int64')})
    pred = pd.DataFrame({'A': [11991.0, 7944.0]})
    assert numeric_recall(gt, pred) == 1.0
    assert numeric_f1(gt, pred) == 1.0


def test_ignore_numeric_dimension_codes():
    """A numeric dimension code (e.g. an SBI code) is no observation: omitting it costs nothing."""
    gt = pd.DataFrame({'SectorBranchSIC2008': [383100], 'Periods': ['2019JJ00'], 'NaturalGasDelivered_1': [365]})
    pred = pd.DataFrame({'Periods': ['2019JJ00'], 'NaturalGasDelivered_1': [365]})
    assert numeric_recall(gt, pred) == 0.5
    assert numeric_recall(gt, pred, ignore_columns={'SectorBranchSIC2008'}) == 1.0
    assert numeric_f1(gt, pred, ignore_columns={'SectorBranchSIC2008'}) == 1.0


def test_ignore_columns_no_credit_for_codes():
    """Selecting the right dimension code, but the wrong observation, scores 0."""
    gt = pd.DataFrame({'Id': [17], 'Leeftijd': [52050], 'M001': [12.5]})
    pred = pd.DataFrame({'Id': [17], 'Leeftijd': [52050], 'M001': [99.0]})
    assert numeric_f1(gt, pred, ignore_columns={'Id', 'Leeftijd'}) == 0.0


def test_ignore_columns_multiindex():
    """Columns the DBExecutor split into a MultiIndex are matched on their joined name."""
    gt = pd.DataFrame([[300007, 1.5]], columns=pd.MultiIndex.from_tuples([('Sector', 'Branches'), ('M001', '')]))
    pred = pd.DataFrame([[1.5]], columns=pd.MultiIndex.from_tuples([('M001', '')]))
    assert numeric_recall(gt, pred, ignore_columns={'Sector_Branches'}) == 1.0


def test_ignore_columns_multiindex_level():
    """A pivoted ARG_MIN column is ignored on its level, e.g. ('2019JJ00', 'MIN[Regions]')."""
    gt = pd.DataFrame([[363, 4.2]], columns=pd.MultiIndex.from_tuples([('2019JJ00', 'MIN[Regions]'), ('2019JJ00', 'MIN')]))
    pred = pd.DataFrame([[4.2]], columns=pd.MultiIndex.from_tuples([('2019JJ00', 'MIN')]))
    assert numeric_f1(gt, pred, ignore_columns={'MIN[Regions]'}) == 1.0


def test_f1_only_strings():
    gt = pd.DataFrame({'A': ['foo', 'bar']})
    assert numeric_f1(gt, pd.DataFrame({'A': ['foo', 'bar']})) == 1.0
    assert numeric_f1(gt, pd.DataFrame({'A': [1, 2]})) == 0.0
