"""
This module contains helper functions for comparing two answers from S-expressions or SQL with each other.
"""
import pandas as pd

def normalize_columns(df: pd.DataFrame) -> pd.Index:
    """
        Sort components within each MultiIndex or Index tuple while preserving level names
    """
    if isinstance(df.columns, pd.MultiIndex):
        # For MultiIndex, sort components within each tuple
        sorted_tuples = [tuple(sorted(item)) for item in df.columns]
        return pd.MultiIndex.from_tuples(sorted_tuples, names=df.columns.names)
    else:
        # For regular Index, check if it contains tuples
        if len(df.columns) > 0 and any(isinstance(col, tuple) for col in df.columns):
            # Sort components within each tuple
            sorted_columns = [tuple(sorted(col)) if isinstance(col, tuple) else col for col in df.columns]
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

    # Sort rows alphabetically (relevant when resetting MultiIndex)
    df = df.loc[sorted(df.index)]

    # Reset index such that ordering doesn't matter anymore and sort aggregated indices
    if isinstance(df.index, pd.Index):
        if isinstance(df.index, pd.MultiIndex):
            col_names = df.index.names
            df = df.reset_index()
            # Make sure order or aggregated index doesn't matter after resetting it (they have been converted to columns)
            for c in col_names:
                df[c] = df[c].map(lambda x: sorted(x))
        else:
            df.index = df.index.map(lambda x: sorted(x))  # makes sure order of aggregated index doesn't matter

    # Sort column levels alphabetically
    df = df.sort_index(axis=1)

    return df


def assert_frame_equal_unordered(actual: pd.DataFrame, expected: pd.DataFrame, **kwargs):
    """Compare DataFrames ignoring MultiIndex order and names"""
    actual_norm = normalize_dataframe(actual)
    expected_norm = normalize_dataframe(expected)

    pd.testing.assert_frame_equal(actual_norm, expected_norm, check_dtype=True, check_names=False, **kwargs)


def is_equal_frame(actual: pd.DataFrame, expected: pd.DataFrame, **kwargs) -> bool:
    """                                                                                                                                                                        │
        Compare two DataFrames for equality, ignoring order of columns/indices.                                                                              │
        This function wraps the assertion version to provide a boolean result.                                                                                                  │
    """
    try:
        assert_frame_equal_unordered(actual, expected, **kwargs)
        return True
    except AssertionError:
        return False
