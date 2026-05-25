#!/usr/bin/env python3
"""Бенчмарк валидации 100 000 отзывов: чистая Python (pandas apply) vs PyO3 (Rust)."""

import timeit
import random
import sys
import json
import math
import pandas as pd

# Константы
N = 100_000
SEED = 42

def generate_reviews(n: int, seed: int = SEED) -> pd.DataFrame:
    """Генерирует n отзывов (тот же алгоритм, что и в Go-бенчмарке)."""
    rng = random.Random(seed)
    records = []
    for _ in range(n):
        coin = rng.random()

        # rating
        rating = 1.0 + rng.random() * 4.0
        if coin < 0.05:
            rating = rng.random() * 0.9 - 0.1

        # text
        if coin < 0.08:
            text = ""
        elif coin < 0.10:
            text = "x" * 5001
        else:
            text = "Normal review text. This is a valid review for the benchmark."

        # likes
        likes = rng.randint(0, 499)
        if coin < 0.13:
            likes = -rng.randint(1, 10)

        records.append({"rating": rating, "text": text, "likes": likes})

    return pd.DataFrame(records)


def validate_python(rating: float, text: str, likes: int) -> bool:
    """Чистая Python-валидация (аналог Go-версии)."""
    if rating < 1.0 or rating > 5.0:
        return False
    if not text or len(text) > 5000:
        return False
    if likes < 0:
        return False
    return True


def benchmark_single(df: pd.DataFrame, name: str, fn, setup: str = "", number: int = 3) -> dict:
    """Замер времени выполнения fn(df) с помощью timeit.repeat."""
    stmt = f"fn(df)"
    setup_code = f"""
import pandas as pd
{setup}
df = __df__
fn = __fn__
"""
    # timeit: repeat 3 раза, каждый по 1 запуску
    times = timeit.repeat(
        stmt="fn(df)",
        setup=setup_code,
        globals={"__df__": df.copy(), "__fn__": fn},
        repeat=3,
        number=1,
    )
    best = min(times)
    rps = N / best
    return {
        "method": name,
        "time_sec": round(best, 4),
        "rows_per_sec": round(rps, 0),
        "times": [round(t, 4) for t in times],
    }


def main():
    print("=" * 65)
    print("  BENCHMARK: Сравнение валидации 100 000 отзывов")
    print("=" * 65)
    print()

    # 1. Генерация данных
    print(f"Генерация {N} отзывов...")
    df = generate_reviews(N)
    print(f"  DataFrame shape: {df.shape}")
    print(f"  RAM: ~{df.memory_usage(deep=True).sum() / 1024:.0f} KB")
    print()

    results = []

    # === 2. Чистый Python через pandas apply ===
    print("--- 1/2: Pure Python (pandas apply) ---")
    def apply_python(df):
        return df.apply(lambda row: validate_python(row["rating"], row["text"], row["likes"]), axis=1)

    r_py = benchmark_single(df, "Python (pandas apply)", apply_python)
    results.append(r_py)
    print(f"  Time: {r_py['time_sec']:.4f}s  |  {r_py['rows_per_sec']:,.0f} rows/sec")
    print()

    # === 3. Python + PyO3 (Rust) ===
    print("--- 2/2: Python + Rust (PyO3) ---")
    try:
        from py_review_validator import validate_review_py

        def apply_pyo3(df):
            return df.apply(
                lambda row: validate_review_py(row["rating"], row["text"], row["likes"]),
                axis=1,
            )

        r_pyo3 = benchmark_single(df, "Python + Rust (PyO3)", apply_pyo3)
        results.append(r_pyo3)
        print(f"  Time: {r_pyo3['time_sec']:.4f}s  |  {r_pyo3['rows_per_sec']:,.0f} rows/sec")
    except ImportError as e:
        print(f"  SKIP — {e}")
    print()

    # === 4. Итоговая таблица ===
    print("=" * 65)
    print("  РЕЗУЛЬТАТЫ")
    print("=" * 65)
    print(f"  {'Method':<30s} {'Time (s)':>10s} {'Rows/sec':>14s}")
    print(f"  {'─'*30} {'─'*10} {'─'*14}")
    for r in results:
        print(f"  {r['method']:<30s} {r['time_sec']:>10.4f} {r['rows_per_sec']:>14,.0f}")
    if len(results) >= 2:
        speedup = results[0]["time_sec"] / results[1]["time_sec"]
        print()
        print(f"  Ускорение PyO3 vs Python: {speedup:.2f}×")

    # Сохраняем результаты
    with open("benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\n  Результаты сохранены в benchmark_results.json")


if __name__ == "__main__":
    main()
