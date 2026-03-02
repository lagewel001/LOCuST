"""
This module contains helper functions for comparing two answers from S-expressions or SQL with each other.
"""
import numpy as np
import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.Index:
    """
        Sort components within each MultiIndex or Index tuple while preserving level names
    """
    if isinstance(df.columns, pd.MultiIndex):
        # For MultiIndex, sort components within each tuple
        sorted_tuples = [tuple(sorted(map(str, item))) for item in df.columns]
        return pd.MultiIndex.from_tuples(sorted_tuples, names=df.columns.names)
    else:
        # For regular Index, check if it contains tuples
        if len(df.columns) > 0 and any(isinstance(col, tuple) for col in df.columns):
            # Sort components within each tuple
            sorted_columns = [tuple(sorted(map(str, col))) if isinstance(col, tuple) else col for col in df.columns]
            return pd.Index(sorted_columns)
        else:
            return df.columns


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
        This helper function normalizes the index and columns (Index or MultiIndex) for
        S-expression/SQL answers in order to test equality between two answers regardless
        of ordering of column/index names.
    """
    df = df.copy()
    df.columns = normalize_columns(df)

    if isinstance(df.index, pd.RangeIndex):
        # RangeIndex applies for 'simplified' SQLs
        # Ensure column order is irrelevant
        sorted_cols = sorted(df.columns.to_list())
        df = df[sorted_cols]

        # Convert to records array, sort the records, then convert back to DataFrame
        # This ensures an irrelevant row order, even if there are duplicate rows.
        records = df.to_records(index=False)
        records = np.array(sorted(records, key=str))
        df = pd.DataFrame.from_records(records)
    elif isinstance(df.index, pd.Index):
        # Sort rows alphabetically (relevant when resetting MultiIndex)
        df = df.loc[sorted(df.index)]

        # Reset index such that ordering doesn't matter anymore and sort aggregated indices
        if isinstance(df.index, pd.MultiIndex):
            col_names = df.index.names
            df = df.reset_index()
            # Make sure order or aggregated index doesn't matter after resetting it (they have been converted to columns)
            for c in col_names:
                df[c] = df[c].map(lambda x: sorted(x))
        elif df.index.dtype == object:
            df.index = df.index.map(lambda x: sorted(x))  # makes sure order of aggregated index doesn't matter

        # Sort column levels alphabetically
        df = df.sort_index(axis=1)

    # Sort tuple values alphabetically
    for c in df.columns:
        df[c] = df[c].apply(lambda v: tuple(sorted(v, key=str)) if isinstance(v, tuple) else v)

    # Cast values to float for numeric columns, leave others
    for col in df.columns:
        try:
            df[col] = df[col].astype(float)
        except (ValueError, TypeError):
            continue
    return df


def assert_frame_equal_unordered(actual: pd.DataFrame, expected: pd.DataFrame, **kwargs):
    """Compare DataFrames ignoring MultiIndex order and names"""
    actual_norm = normalize_dataframe(actual)
    expected_norm = normalize_dataframe(expected)

    pd.testing.assert_frame_equal(actual_norm, expected_norm, check_dtype=True, check_names=False, **kwargs)


def is_equal_frame(actual: pd.DataFrame, expected: pd.DataFrame, **kwargs) -> bool:
    """
        Compare two DataFrames for equality, ignoring order of columns/indices.
        This function wraps the assertion version to provide a boolean result.
    """
    try:
        assert_frame_equal_unordered(actual, expected, **kwargs)
        return True
    except AssertionError:
        return False
