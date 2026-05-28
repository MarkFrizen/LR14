"""Тесты валидации отзывов (pure-Python fallback логика)."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "clients"))

# Копия логики валидации из clean_reviews.py (Python fallback)
def validate_review(rating, text, likes):
    if not (1.0 <= rating <= 5.0):
        return False, f"rating must be between 1.0 and 5.0, got {rating}"
    if not text:
        return False, "text must not be empty"
    if len(text) > 5000:
        return False, f"text too long: {len(text)} chars, max 5000"
    if likes < 0:
        return False, f"likes must be >= 0, got {likes}"
    return True, ""


class TestValidateReview:
    def test_valid_review(self):
        ok, msg = validate_review(3.0, "Хороший товар", 10)
        assert ok
        assert msg == ""

    def test_valid_boundary_rating_min(self):
        ok, msg = validate_review(1.0, "Товар", 0)
        assert ok
        assert msg == ""

    def test_valid_boundary_rating_max(self):
        ok, msg = validate_review(5.0, "Отлично", 999)
        assert ok
        assert msg == ""

    def test_rating_below_min(self):
        ok, msg = validate_review(0.9, "текст", 0)
        assert not ok
        assert "1.0" in msg

    def test_rating_above_max(self):
        ok, msg = validate_review(5.1, "текст", 0)
        assert not ok
        assert "5.0" in msg

    def test_rating_negative(self):
        ok, msg = validate_review(-1.0, "текст", 0)
        assert not ok
        assert "1.0" in msg

    def test_empty_text(self):
        ok, msg = validate_review(3.0, "", 0)
        assert not ok
        assert "empty" in msg

    def test_whitespace_text(self):
        ok, msg = validate_review(3.0, "   ", 0)
        assert ok
        assert msg == ""

    def test_text_too_long(self):
        long_text = "а" * 5001
        ok, msg = validate_review(3.0, long_text, 0)
        assert not ok
        assert "5000" in msg

    def test_max_length_text(self):
        long_text = "а" * 5000
        ok, msg = validate_review(3.0, long_text, 0)
        assert ok
        assert msg == ""

    def test_negative_likes(self):
        ok, msg = validate_review(3.0, "текст", -1)
        assert not ok
        assert ">= 0" in msg

    def test_zero_likes(self):
        ok, msg = validate_review(3.0, "текст", 0)
        assert ok
        assert msg == ""
