"""Тесты вспомогательных функций Arrow Flight клиента."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "clients"))


class TestArrowHelpers:
    def test_arrow_ipc_size(self):
        import pyarrow as pa
        import arrow_client

        table = pa.table({"x": [1, 2, 3], "y": [4.0, 5.0, 6.0]})
        size = arrow_client.arrow_ipc_size(table)
        assert size > 0

    def test_to_polars_zero_copy(self):
        import pyarrow as pa
        import polars as pl
        import arrow_client

        table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df = arrow_client.to_polars_zero_copy(table)
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (3, 2)

    def test_benchmark_json(self):
        import pyarrow as pa
        import arrow_client

        table = pa.table({"product_id": ["A"], "window_start": [1700000000], 
                          "avg_rating": [4.5], "total_likes": [100], "review_count": [10]})
        result = arrow_client.benchmark_json(table)
        assert "time_sec" in result
        assert "size_bytes" in result
        assert "records" in result
        assert result["records"] == 1
