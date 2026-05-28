"""Тесты генерации синтетических данных."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "clients"))


class TestSyntheticData:
    def test_generate_reviews_structure(self):
        """Проверяет структуру данных из clean_reviews.generate_reviews."""
        from clean_reviews import generate_reviews

        df = generate_reviews(100)
        assert len(df) == 100
        expected_cols = {"review_id", "product_id", "rating", "text", "likes"}
        assert expected_cols.issubset(set(df.columns))

    def test_generate_reviews_rating_range(self):
        from clean_reviews import generate_reviews

        df = generate_reviews(1000)
        ratings = df["rating"].to_list()
        assert all(-1.0 <= r <= 5.0 for r in ratings)

    def test_generate_reviews_likes_non_negative_mostly(self):
        from clean_reviews import generate_reviews

        df = generate_reviews(1000)
        likes = df["likes"].to_list()
        assert any(l >= 0 for l in likes)

    def test_arrow_synthetic_data_structure(self):
        """Проверяет структуру данных из arrow_client.generate_synthetic_data."""
        import arrow_client

        table = arrow_client.generate_synthetic_data(50)
        assert table.num_rows == 50
        assert table.num_columns == 5
        expected = {"product_id", "window_start", "avg_rating", "total_likes", "review_count"}
        assert expected == set(table.column_names)
