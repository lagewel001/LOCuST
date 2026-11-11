import numbers
import pandas as pd
from collections import Counter
from typing import Collection


def _is_ignored(column, ignore_columns: Collection[str]) -> bool:
    """
        Whether a column is ignored. A MultiIndex column (split on underscores by the DBExecutor) is ignored when its
        joined name or one of its levels is, e.g. ('2019JJ00', 'MIN[Regions]') for a pivoted ARG_MIN over the regions.
    """
    if isinstance(column, tuple):
        levels = [str(level) for level in column if not pd.isna(level) and level != '']
        return '_'.join(levels) in ignore_columns or any(level in ignore_columns for level in levels)
    return str(column) in ignore_columns


def _numeric_multiset(df: pd.DataFrame, numeric_precision: int, ignore_columns: Collection[str] = ()) -> Counter:
    """Extracts all numeric values from a DataFrame into a flat multiset (Counter),
    ignoring string values, column names, row structure, NaN and the values in ignore_columns."""
    values = []
    for column, series in df.items():
        if _is_ignored(column, ignore_columns):
            continue
        for v in series:
            # numbers.Real includes the NumPy scalars, which nullable (Int64) columns yield
            if isinstance(v, numbers.Real) and not pd.isna(v):
                values.append(round(float(v), numeric_precision))
    return Counter(values)


def numeric_recall(ground_truth: pd.DataFrame, predicted: pd.DataFrame, numeric_precision: int = 5,
                   ignore_columns: Collection[str] = ()) -> float:
    """
        Format-independent numeric recall.

        Compares the multiset of all numeric values across both DataFrames,
        ignoring column names, row structure, and string values. This is a
        lenient metric that complements the stricter row-aware record_accuracy.

        :param ground_truth: ground truth dataframe
        :param predicted: predicted dataframe
        :param numeric_precision: rounding tolerance for float comparisons
        :param ignore_columns: columns whose values are no observations, e.g. dimensions with numeric codes
        :returns: fraction of GT numeric values found in the prediction (0 to 1)
    """
    if len(ground_truth) == 0:
        return 1.0 if len(predicted) == 0 else 0.0

    if len(predicted) == 0:
        return 0.0

    gt_values = _numeric_multiset(ground_truth, numeric_precision, ignore_columns)
    pred_values = _numeric_multiset(predicted, numeric_precision, ignore_columns)

    if not gt_values:
        return 1.0 if not pred_values else 0.0

    correct = sum(min(gt_values[v], pred_values[v]) for v in gt_values)
    return correct / sum(gt_values.values())


def _numeric_precision(ground_truth: pd.DataFrame, predicted: pd.DataFrame, numeric_precision: int,
                       ignore_columns: Collection[str] = ()) -> float:
    """Fraction of predicted numeric values found in the GT; the counterpart of numeric_recall."""
    if len(ground_truth) == 0:
        return 1.0 if len(predicted) == 0 else 0.0

    if len(predicted) == 0:
        return 0.0

    gt_values = _numeric_multiset(ground_truth, numeric_precision, ignore_columns)
    pred_values = _numeric_multiset(predicted, numeric_precision, ignore_columns)

    if not gt_values:
        return 1.0 if not pred_values else 0.0

    if not pred_values:
        return 0.0

    correct = sum(min(gt_values[v], pred_values[v]) for v in pred_values)
    return correct / sum(pred_values.values())


def numeric_f1(ground_truth: pd.DataFrame, predicted: pd.DataFrame, numeric_precision: int = 5,
               ignore_columns: Collection[str] = ()) -> float:
    """
        Format-independent numeric F1: the harmonic mean of the numeric precision (fraction of predicted numeric
        values found in the GT) and numeric_recall. Unlike recall, it also penalizes returning too many values.

        :param ground_truth: ground truth dataframe
        :param predicted: predicted dataframe
        :param numeric_precision: rounding tolerance for float comparisons
        :param ignore_columns: columns whose values are no observations, e.g. dimensions with numeric codes
        :returns: numeric F1 (0 to 1)
    """
    precision = _numeric_precision(ground_truth, predicted, numeric_precision, ignore_columns)
    recall = numeric_recall(ground_truth, predicted, numeric_precision, ignore_columns)
    return 2 * precision * recall / (precision + recall) if precision + recall > 0 else 0.0
