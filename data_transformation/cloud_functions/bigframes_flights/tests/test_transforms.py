import bigframes.pandas as bpd
import pytest

from src.transforms import map_columns, map_bools, map_floats


def test_map_floats():
    test_data: dict = {
        "measurements": ["35.6", "40.2", "32.4", "10.15", "55"],
        "unrelated_strings": ["high", "medium", "low", "medium", "high"],
    }

    want: dict = {
        "measurements": [35.6, 40.2, 32.4, 10.15, 55],
        "unrelated_strings": ["high", "medium", "low", "medium", "high"],
    }

    df_test: bpd.DataFrame = bpd.DataFrame(test_data)

    df_got: bpd.DataFrame = df_test.copy()
    map_floats(df_target=df_got, columns=["measurements"])

    # orient = "list" will convert to { column: value }
    # the rest generally add indexes
    got: dict = df_got.to_dict(orient="list")
    assert want == got
