#!/usr/bin/env python3
"""
Python-конвейер очистки отзывов с валидацией через Rust PyO3.

Использование:
  # 1. Собрать Rust-расширение (требуется сеть для загрузки pyo3):
  #    cd py-validator && maturin develop --release
  #
  # 2. Запустить конвейер:
  #    .venv/bin/python clients/clean_reviews.py

Если расширение не собрано — используется pure-Python запасной валидатор
с той же логикой.
"""

import random
import time

import polars as pl

# ====================================================================
# 1. Загрузка Rust-валидатора (PyO3) или fallback
# ====================================================================

try:
    import py_review_validator as rv

    def validate_review(rating, text, likes):
        """Обёртка Rust-функции: возвращает (is_valid, error_msg)."""
        return rv.validate_review_py(rating, text, likes)

    print("✅ Using Rust PyO3 validator: py_review_validator")
except ImportError:
    # Pure-Python fallback — та же логика
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

    print("⚠️  Rust extension not found — using pure-Python fallback validator")
    print("   To enable Rust: cd py-validator && maturin develop --release")

# ====================================================================
# 2. Генерация синтетических отзывов
# ====================================================================

def generate_reviews(n: int) -> pl.DataFrame:
    """Генерирует датасет отзывов с долей невалидных записей."""
    products = [f"WB-{i:03d}" for i in range(1, 21)]

    ratings = []
    texts = []
    likes_list = []
    product_ids = []

    for i in range(n):
        product_ids.append(random.choice(products))
        # ~15% невалидных
        coin = random.random()

        if coin < 0.05:
            ratings.append(random.uniform(-1, 0.9))  # bad rating
        else:
            ratings.append(round(random.uniform(1.0, 5.0), 1))

        if coin < 0.08:
            texts.append("")  # empty text
        elif coin < 0.10:
            texts.append("x" * 5001)  # too long
        else:
            texts.append(random.choice([
                "Отличный товар! Всё соответствует описанию.",
                "Доставка быстрая, упаковка надёжная.",
                "Нормально, но могло быть и лучше за такие деньги.",
                "Товар пришёл с дефектом, разочарован.",
                "Супер! Буду заказывать ещё.",
                "Качество на высоте, рекомендую.",
                "Средне, но свои функции выполняет.",
                "Бракованный экземпляр, оформил возврат.",
            ]))

        if coin < 0.13:
            likes_list.append(random.randint(-10, -1))  # negative likes
        else:
            likes_list.append(random.randint(0, 500))

    return pl.DataFrame({
        "review_id": [f"R-{i:05d}" for i in range(n)],
        "product_id": product_ids,
        "rating": ratings,
        "text": texts,
        "likes": likes_list,
    })


# ====================================================================
# 3. Применение валидации к каждой строке
# ====================================================================

def clean_reviews(df: pl.DataFrame) -> pl.DataFrame:
    """Добавляет колонки is_valid и error_msg, используя Rust-валидатор."""

    # Проходим по строкам, применяем валидатор.
    is_valid = []
    errors = []
    for row in df.iter_rows():
        ok, msg = validate_review(row[2], row[3], row[4])
        is_valid.append(ok)
        errors.append(msg)

    df = df.with_columns([
        pl.Series("is_valid", is_valid),
        pl.Series("error_msg", errors),
    ])
    return df


# ====================================================================
# 4. Main
# ====================================================================

def main():
    n = 10_000
    print("=" * 65)
    print("  Конвейер очистки отзывов — Rust/PyO3 + Polars")
    print("=" * 65)

    # Генерация
    print(f"\n[1] Generating {n} reviews (≈15% invalid)...")
    t0 = time.perf_counter()
    df = generate_reviews(n)
    print(f"    {len(df)} rows in {time.perf_counter()-t0:.3f}s")

    # Валидация
    print(f"\n[2] Validating reviews...")
    t0 = time.perf_counter()
    df = clean_reviews(df)
    t_val = time.perf_counter() - t0
    print(f"    {t_val:.3f}s ({n/t_val:.0f} rows/sec)")

    # Статистика
    n_valid = df["is_valid"].sum()
    n_invalid = (~df["is_valid"]).sum()
    print(f"\n[3] Validation results:")
    print(f"    ✅ Valid:   {n_valid:>6} ({100*n_valid/n:.1f}%)")
    print(f"    ❌ Invalid: {n_invalid:>6} ({100*n_invalid/n:.1f}%)")

    # Типы ошибок
    if n_invalid > 0:
        print(f"\n[4] Error breakdown:")
        error_counts = (
            df.filter(~pl.col("is_valid"))
            .group_by("error_msg")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )
        for row in error_counts.rows()[:10]:
            msg = row[0][:70] + "..." if len(row[0]) > 70 else row[0]
            print(f"    • {msg:72s} {row[1]:>5}")

    # Невалидные строки
    print(f"\n[5] Invalid rows preview:")
    invalid = df.filter(~pl.col("is_valid")).select([
        "review_id", "product_id", "rating", "likes", "is_valid", "error_msg"
    ])
    print(invalid.head(5))

    # Очищенный датасет
    print(f"\n[6] Cleaning: keeping only valid reviews...")
    clean = df.filter(pl.col("is_valid")).drop(["is_valid", "error_msg"])
    print(f"    Clean dataset: {len(clean)}/{len(df)} rows kept")

    print("\nDone.")


if __name__ == "__main__":
    main()
