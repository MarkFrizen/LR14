#!/usr/bin/env python3
"""
Скрипт сравнения производительности Go vs Python сборщиков отзывов.

Запускает оба бенчмарка последовательно, парсит SUMMARY_JSON из вывода,
выводит итоговую таблицу сравнения.

Использование:
    .venv/bin/python benchmark/comparison.py --python-only   # только Python
    .venv/bin/python benchmark/comparison.py --go-only       # только Go
    .venv/bin/python benchmark/comparison.py                 # оба
"""

import argparse
import json
import os
import re
import subprocess
import sys
import textwrap


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_python_bench(products: int = 1000, reviews: int = 50,
                     concurrency: int = 50) -> dict:
    """Запускает Python-бенчмарк и возвращает словарь с результатами."""
    collector_py = os.path.join(PROJECT_DIR, "clients", "collector.py")
    venv_python = os.path.join(PROJECT_DIR, ".venv", "bin", "python")

    cmd = [
        venv_python, collector_py,
        "--bench",
        "--bench-products", str(products),
        "--bench-reviews", str(reviews),
        "--bench-concurrency", str(concurrency),
        "--source", "wildberries",
        "--metrics-csv", "/tmp/metrics_python.csv",
    ]

    print(f"\n{'#'*70}")
    print(f"# RUNNING PYTHON BENCHMARK: {products} products × {reviews} reviews")
    print(f"# Concurrency: {concurrency}")
    print(f"{'#'*70}\n")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600,  # 10 minutes max
    )

    # Print stdout (the main output)
    print(result.stdout)

    if result.stderr:
        print("STDERR:", result.stderr[:500], file=sys.stderr)

    # Extract SUMMARY_JSON
    return _extract_summary(result.stdout, "python")


def run_go_bench(products: int = 1000, reviews: int = 50,
                 concurrency: int = 50) -> dict:
    """Запускает Go-бенчмарк и возвращает словарь с результатами."""
    go_binary = "/tmp/go-bench-collector"

    # Build if not exists
    if not os.path.exists(go_binary):
        print("Building Go benchmark...")
        subprocess.run(
            ["go", "build", "-o", go_binary,
             os.path.join(PROJECT_DIR, "cmd", "benchmark-collector")],
            cwd=PROJECT_DIR,
            check=True,
        )

    cmd = [
        go_binary,
        "-products", str(products),
        "-reviews", str(reviews),
        "-concurrency", str(concurrency),
        "-source", "wildberries",
    ]

    print(f"\n{'#'*70}")
    print(f"# RUNNING GO BENCHMARK: {products} products × {reviews} reviews")
    print(f"# Concurrency: {concurrency}")
    print(f"{'#'*70}\n")

    # Use /usr/bin/time -v for Go to get max RSS
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600,
    )

    print(result.stdout)

    if result.stderr:
        print("STDERR:", result.stderr[:500], file=sys.stderr)

    return _extract_summary(result.stdout, "go")


def _extract_summary(text: str, language: str) -> dict:
    """Извлекает JSON-строку после SUMMARY_JSON: из вывода."""
    m = re.search(r"SUMMARY_JSON:\s*(\{.*\})", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError as e:
            print(f"WARNING: Failed to parse {language} JSON summary: {e}",
                  file=sys.stderr)
            print(f"  Raw: {m.group(1)[:200]}", file=sys.stderr)
    else:
        print(f"WARNING: No SUMMARY_JSON found in {language} output",
              file=sys.stderr)

    return {"language": language, "error": "no summary"}


def print_comparison(py_result: dict, go_result: dict):
    """Выводит итоговую таблицу сравнения Go vs Python."""

    has_py = "error" not in py_result
    has_go = "error" not in go_result

    sep = "=" * 72

    print(f"\n{sep}")
    print(f"  СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ: Go vs Python")
    print(f"  Сборщик отзывов с маркетплейса (имитация Wildberries/Ozon)")
    print(f"  Нагрузка: {py_result.get('products', go_result.get('products', '?'))} "
          f"товаров, {py_result.get('expected_total', '?')} отзывов (ожидалось)")
    print(f"{sep}")

    # Таблица
    header = f"  {'Метрика':<25} {'Go':>15} {'Python':>15} {'Разница':>15}"
    print(header)
    print("  " + "-" * 70)

    rows = [
        ("Длительность (с)", "wall_clock_s", "%.2f", False),
        ("Всего отзывов", "total_reviews", "%d", False),
        ("Пропускная способность (rev/s)", "rps", "%.1f", True),
        ("Пиковая пропускная (rev/s)", "peak_rps", "%.1f", True),
        ("Среднее RSS (MB)", "avg_rss_mb", "%.1f", True),
        ("Пиковое RSS (MB)", "peak_rss_mb", "%.1f", True),
        ("Средний CPU (%)", "avg_cpu_pct", "%.1f", True),
        ("Пиковый CPU (%)", "peak_cpu_pct", "%.1f", True),
    ]

    for label, key, fmt, higher_is_better in rows:
        go_val = go_result.get(key, "N/A") if has_go else "N/A"
        py_val = py_result.get(key, "N/A") if has_py else "N/A"

        go_str = fmt % go_val if isinstance(go_val, (int, float)) else str(go_val)
        py_str = fmt % py_val if isinstance(py_val, (int, float)) else str(py_val)

        if isinstance(go_val, (int, float)) and isinstance(py_val, (int, float)):
            if go_val != 0:
                diff = (py_val - go_val) / go_val * 100
                diff_str = f"{diff:+.1f}%"
            else:
                diff_str = "N/A"
        else:
            diff_str = "N/A"

        print(f"  {label:<25} {go_str:>15} {py_str:>15} {diff_str:>15}")

    print(f"{sep}")

    # Дополнительные метрики Go (память по runtime.ReadMemStats)
    if has_go:
        print(f"\n  Go runtime.ReadMemStats:")
        print(f"    Alloc (final):    {go_result.get('alloc_final_mb', 'N/A'):>8} MB")
        print(f"    Total alloc:      {go_result.get('total_alloc_mb', 'N/A'):>8} MB")
        print(f"    GC cycles:        {go_result.get('gc_cycles', 'N/A'):>8}")

    print(f"\n  CSV-файлы метрик:")
    print(f"    Python: /tmp/metrics_python.csv")
    print(f"{sep}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Сравнение Go vs Python сборщиков отзывов")
    parser.add_argument("--products", type=int, default=1000)
    parser.add_argument("--reviews", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--python-only", action="store_true")
    parser.add_argument("--go-only", action="store_true")
    args = parser.parse_args()

    py_result = {}
    go_result = {}

    if not args.go_only:
        py_result = run_python_bench(
            products=args.products,
            reviews=args.reviews,
            concurrency=args.concurrency,
        )

    if not args.python_only:
        go_result = run_go_bench(
            products=args.products,
            reviews=args.reviews,
            concurrency=args.concurrency,
        )

    print_comparison(py_result, go_result)


if __name__ == "__main__":
    main()
