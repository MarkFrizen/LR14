#!/usr/bin/env python3
"""
Генерация графиков сравнения Go vs Python сборщиков отзывов.

Запускает оба бенчмарка (1000×50, concurrency=50), строит три графика:
  1. Throughput (RPS)
  2. Memory usage (RSS / Heap)
  3. CPU usage

Сохраняет PNG и выводит текстовое сравнение.

Запуск:
    .venv/bin/python benchmark/plot_comparison.py
"""

import json
import os
import re
import subprocess
import sys
import textwrap

import matplotlib

matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import numpy as np


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_bench(language: str, products: int = 1000, reviews: int = 50,
              concurrency: int = 50) -> dict:
    """Запускает бенчмарк и возвращает словарь с метриками."""
    if language == "python":
        collector_py = os.path.join(PROJECT_DIR, "clients", "collector.py")
        venv_python = os.path.join(PROJECT_DIR, ".venv", "bin", "python")
        cmd = [
            venv_python, collector_py,
            "--bench",
            "--bench-products", str(products),
            "--bench-reviews", str(reviews),
            "--bench-concurrency", str(concurrency),
            "--source", "wildberries",
            "--metrics-csv", "/tmp/metrics_python_plot.csv",
            "--metrics-interval", "2",
        ]
    else:
        go_bin = "/tmp/go-bench-collector"
        if not os.path.exists(go_bin):
            subprocess.run(
                ["go", "build", "-o", go_bin,
                 os.path.join(PROJECT_DIR, "cmd", "benchmark-collector")],
                cwd=PROJECT_DIR, check=True,
            )
        cmd = [
            go_bin,
            "-products", str(products),
            "-reviews", str(reviews),
            "-concurrency", str(concurrency),
            "-source", "wildberries",
        ]

    label = "GO" if language == "go" else "PYTHON"
    print(f"\n{'#'*60}")
    print(f"# RUNNING {label} BENCHMARK …")
    print(f"{'#'*60}\n", flush=True)

    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=300,
    )

    print(result.stdout)
    if result.stderr:
        print(f"[{label} STDERR] {result.stderr[:300]}", file=sys.stderr)

    # Парсим SUMMARY_JSON
    m = re.search(r"SUMMARY_JSON:\s*(\{.*\})", result.stdout, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError as e:
            print(f"  WARNING: JSON parse error: {e}", file=sys.stderr)
            return {"language": language, "error": str(e)}
    print(f"  WARNING: no SUMMARY_JSON in output", file=sys.stderr)
    return {"language": language, "error": "no summary"}


def plot_comparison(py: dict, go: dict, output_path: str):
    """Строит три bar-диаграммы и сохраняет PNG."""

    has_py = "error" not in py
    has_go = "error" not in go

    if not has_py or not has_go:
        print("ERROR: missing benchmark data", file=sys.stderr)
        return

    # ── данные ──────────────────────────────────────────────────────
    rps = [go.get("rps", 0), py.get("rps", 0)]
    rps_peak = [go.get("rps", 0), py.get("peak_rps", 0)]

    # Memory: Go использует max RSS из getrusage, Python — psutil RSS
    go_mem = go.get("max_rss_mb", go.get("alloc_final_mb", 0))
    py_mem = py.get("peak_rss_mb", py.get("avg_rss_mb", 0))
    mem = [go_mem, py_mem]

    cpu = [go.get("cpu_pct", 0), py.get("avg_cpu_pct", 0)]

    labels = ["Go", "Python"]
    colors = ["#1f77b4", "#ff7f0e"]
    x = np.arange(len(labels))
    bar_width = 0.5

    # ── создаём 3 subplot ───────────────────────────────────────
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.suptitle(
        "Сравнение производительности: Go vs Python\n"
        f"(1000 товаров × 50 отзывов = 50 000, concurrency=50)",
        fontsize=13, fontweight="bold", y=1.02,
    )

    # 1. Throughput
    ax = axes[0]
    bars = ax.bar(x, rps, bar_width, color=colors, edgecolor="white",
                  linewidth=0.8)
    ax.set_ylabel("reviews / sec")
    ax.set_title("Пропускная способность (RPS)", fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.grid(axis="y", alpha=0.3)

    # Подписи значений на столбцах
    for bar, val in zip(bars, rps):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 200,
                f"{val:.0f}", ha="center", va="bottom", fontsize=9,
                fontweight="bold")

    # 2. Memory
    ax = axes[1]
    bars = ax.bar(x, mem, bar_width, color=colors, edgecolor="white",
                  linewidth=0.8)
    ax.set_ylabel("MB")
    ax.set_title("Потребление памяти (Max RSS)", fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.grid(axis="y", alpha=0.3)

    for bar, val in zip(bars, mem):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{val:.1f} MB", ha="center", va="bottom", fontsize=9,
                fontweight="bold")

    # 3. CPU
    ax = axes[2]
    bars = ax.bar(x, cpu, bar_width, color=colors, edgecolor="white",
                  linewidth=0.8)
    ax.set_ylabel("% (одно ядро)")
    ax.set_title("Загрузка CPU", fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.grid(axis="y", alpha=0.3)

    for bar, val in zip(bars, cpu):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f"{val:.1f}%", ha="center", va="bottom", fontsize=9,
                fontweight="bold")

    # Легенда
    for ax in axes:
        ax.legend(
            [plt.Rectangle((0, 0), 1, 1, fc=c) for c in colors],
            ["Go (goroutines + runtime)", "Python (asyncio + psutil)"],
            fontsize=7, loc="upper right",
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\n  [OK] Chart saved to: {output_path}")


def comparison_text(py: dict, go: dict) -> str:
    """Формирует текстовый блок со сравнением.

    Формат (как запрошено пользователем):
      Go быстрее Python в X раз по пропускной способности,
      потребляет в Y раз меньше / больше памяти.
    """
    has_py = "error" not in py
    has_go = "error" not in go
    if not has_py or not has_go:
        return "  ERROR: missing benchmark data"

    go_rps = go.get("rps", 1)
    py_rps = py.get("rps", 1)
    go_mem = go.get("max_rss_mb", go.get("alloc_final_mb", 1))
    py_mem = py.get("peak_rss_mb", py.get("avg_rss_mb", 1))
    go_cpu = go.get("cpu_pct", 0)
    py_cpu = py.get("avg_cpu_pct", 0)

    def _ratio(a: float, b: float) -> float:
        return a / b if a >= b else b / a

    # ── Пропускная способность (Go vs Python) ──────────────────────
    if go_rps >= py_rps:
        rps_part = f"Go быстрее Python в {_ratio(go_rps, py_rps):.2f} раза"
    else:
        rps_part = f"Python быстрее Go в {_ratio(py_rps, go_rps):.2f} раза"

    # ── Память ─────────────────────────────────────────────────────
    if go_mem <= py_mem:
        # Go потребляет МЕНЬШЕ памяти
        mem_part = f"Go потребляет в {_ratio(py_mem, go_mem):.1f} раза меньше памяти (RSS)"
    else:
        # Go потребляет БОЛЬШЕ памяти
        mem_part = f"Go потребляет в {_ratio(go_mem, py_mem):.1f} раза больше памяти (RSS)"

    # ── CPU ────────────────────────────────────────────────────────
    if go_cpu <= py_cpu:
        cpu_part = f"Go загружает CPU в {_ratio(py_cpu, go_cpu):.1f} раза меньше"
    else:
        cpu_part = f"Go загружает CPU в {_ratio(go_cpu, py_cpu):.1f} раза больше"

    # ── Raw data ───────────────────────────────────────────────────
    raw = (
        f"Raw: Go RPS={go_rps:.0f}, RSS={go_mem:.1f}MB, CPU={go_cpu:.1f}%; "
        f"Python RPS={py_rps:.0f}, RSS={py_mem:.1f}MB, CPU={py_cpu:.1f}%"
    )

    return f"  {rps_part} по пропускной способности,\n  {mem_part},\n  {cpu_part}.\n\n  {raw}"


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Сравнение Go vs Python с графиками")
    parser.add_argument("--products", type=int, default=1000)
    parser.add_argument("--reviews", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--output", default="benchmark/comparison.png",
                        help="Path to output PNG")
    parser.add_argument("--go-only", action="store_true")
    parser.add_argument("--python-only", action="store_true")
    args = parser.parse_args()

    output_path = os.path.join(PROJECT_DIR, args.output)

    py_result = {}
    go_result = {}

    if not args.go_only:
        py_result = run_bench("python",
                              products=args.products,
                              reviews=args.reviews,
                              concurrency=args.concurrency)

    if not args.python_only:
        go_result = run_bench("go",
                              products=args.products,
                              reviews=args.reviews,
                              concurrency=args.concurrency)

    # ── графики ──────────────────────────────────────────────────
    plot_comparison(py_result, go_result, output_path)

    # ── текстовый вывод ──────────────────────────────────────────
    sep = "=" * 72
    print(f"\n{sep}")
    print("  ВЫВОД")
    print(f"{sep}")
    print(comparison_text(py_result, go_result))
    print(f"{sep}\n")


if __name__ == "__main__":
    main()
