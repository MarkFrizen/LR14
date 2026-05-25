#!/usr/bin/env python3
"""Сравнительный график бенчмарков валидации."""

import json
import os
import matplotlib
matplotlib.use("Agg")  # no display needed
import matplotlib.pyplot as plt
import numpy as np

BASE = "/home/ubuntu/Yandex.Disk/Универ (1)/Методы и технологии программирования/Projects/LR14"
BENCH_DIR = os.path.join(BASE, "cmd", "benchmark-validate")

# Читаем результаты Go
go_path = os.path.join(BASE, "benchmark_results_go.json")
with open(go_path) as f:
    go_results = json.load(f)

# Читаем результаты Python
py_path = os.path.join(BENCH_DIR, "benchmark_results.json")
with open(py_path) as f:
    py_results = json.load(f)

# Собираем всё вместе
all_results = go_results + py_results

# Цвета для методов
colors_map = {
    "Pure Go (inline)": "#2ecc71",
    "Go cgo → Rust (.so)": "#e74c3c",
    "Python (pandas apply)": "#3498db",
    "Python + Rust (PyO3)": "#f39c12",
}

methods = [r["method"] for r in all_results]
times = [r["time_sec"] for r in all_results]
rps = [r["rows_per_sec"] for r in all_results]
colors = [colors_map.get(m, "#95a5a6") for m in methods]

# =============================================
# График 1: Время выполнения (сек) — log scale
# =============================================
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

ax1 = axes[0]
bars1 = ax1.barh(methods, times, color=colors, edgecolor="white", height=0.6)
ax1.set_xscale("log")
ax1.set_xlabel("Время (сек, log scale)", fontsize=12)
ax1.set_title("Время валидации 100 000 отзывов\n(меньше = лучше)", fontsize=13, fontweight="bold")
for bar, t in zip(bars1, times):
    if t < 1:
        ax1.text(t * 1.1, bar.get_y() + bar.get_height() / 2,
                 f"{t*1000:.1f} ms", va="center", fontsize=10)
    else:
        ax1.text(t * 1.1, bar.get_y() + bar.get_height() / 2,
                 f"{t:.2f} s", va="center", fontsize=10)

# =============================================
# График 2: Пропускная способность (rows/sec)
# =============================================
ax2 = axes[1]
bars2 = ax2.barh(methods, rps, color=colors, edgecolor="white", height=0.6)
ax2.set_xscale("log")
ax2.set_xlabel("Строк/сек (log scale)", fontsize=12)
ax2.set_title("Пропускная способность\n(больше = лучше)", fontsize=13, fontweight="bold")
for bar, r in zip(bars2, rps):
    if r > 1_000_000:
        label = f"{r/1_000_000:.1f}M"
    elif r > 1_000:
        label = f"{r/1_000:.0f}K"
    else:
        label = f"{r:.0f}"
    ax2.text(r * 1.1, bar.get_y() + bar.get_height() / 2,
             label, va="center", fontsize=10)

plt.tight_layout(pad=2)
out_path = os.path.join(BENCH_DIR, "benchmark_comparison.png")
plt.savefig(out_path, dpi=150, bbox_inches="tight")
print(f"График сохранён: {out_path}")

# =============================================
# ASCII-таблица для терминала
# =============================================
print()
print("=" * 75)
print("  ИТОГОВАЯ ТАБЛИЦА: Валидация 100 000 отзывов")
print("=" * 75)
print(f"  {'Метод':<30s} {'Время':>12s} {'Rows/sec':>14s} {'Относ. Pure Go':>16s}")
print(f"  {'─'*30} {'─'*12} {'─'*14} {'─'*16}")
# Находим Pure Go для базового сравнения
go_inline_time = next(r["time_sec"] for r in all_results if r["method"] == "Pure Go (inline)")
for r in all_results:
    m = r["method"]
    t = r["time_sec"]
    rps_val = r["rows_per_sec"]
    ratio = t / go_inline_time
    # Форматирование времени
    if t < 1:
        t_str = f"{t*1000:.2f} ms"
    else:
        t_str = f"{t:.2f} s "
    # Форматирование rows/sec
    if rps_val > 1_000_000:
        rps_str = f"{rps_val/1_000_000:.2f}M"
    elif rps_val > 1_000:
        rps_str = f"{rps_val/1_000:.1f}K"
    else:
        rps_str = f"{rps_val:.0f}"
    print(f"  {m:<30s} {t_str:>12s} {rps_str:>14s} {ratio:>15.1f}×")

print()
print("  Выводы:")
print(f"    • Pure Go — самый быстрый ({go_inline_time*1000:.2f} ms)")
py_times = {r["method"]: r["time_sec"] for r in all_results if "Python" in r["method"]}
cgo_time = next(r["time_sec"] for r in all_results if "cgo" in r["method"])
print(f"    • Python (pandas apply) — в {py_times['Python (pandas apply)']/go_inline_time:.0f}× медленнее Go")
print(f"    • PyO3 — на ~{((py_times['Python + Rust (PyO3)']/py_times['Python (pandas apply)'] - 1)*100):.0f}% медленнее чистого Python из-за FFI-оверхеда")
print(f"      для простой логики")
print(f"    • Go cgo → Rust — в {cgo_time/go_inline_time:.0f}× медленнее Pure Go")
print(f"      (dlopen/dlsym на каждый вызов — основной оверхед)")
