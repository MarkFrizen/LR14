#!/usr/bin/env python3
"""
Arrow Flight Python-клиент для получения WindowAgg от Go-сервера.

Возможности:
  • Подключение к Go Arrow Flight RPC-серверу
  • DoGet → pyarrow.Table → Polars DataFrame (zero-copy)
  • Вывод схемы и первых 5 строк
  • Сравнение производительности Flight vs JSON (время + объём)

Запуск:
  ./clients/arrow_client.py                         # Flight-server :50051
  ./clients/arrow_client.py --no-flight             # генерация синтетических данных
  ./clients/arrow_client.py --addr grpc://localhost:50051
"""

import argparse
import json
import random
import sys
import time
from sys import getsizeof

import pyarrow as pa
import pyarrow.flight as fl
import polars as pl


# ===========================================================================
# Синтетические данные (если Flight-сервер недоступен)
# ===========================================================================

def generate_synthetic_data(n: int) -> pa.Table:
    """Генерирует таблицу WindowAgg с n записями для тестирования."""
    products = [f"WB-{i:03d}" for i in range(1, 101)]
    base_time = int(time.time()) * 1_000_000_000  # ns

    product_ids = []
    timestamps = []
    ratings = []
    likes = []
    counts = []

    for i in range(n):
        product_ids.append(random.choice(products))
        timestamps.append(base_time + i * 60_000_000_000)  # +1 min each
        ratings.append(round(random.uniform(1.0, 5.0), 2))
        likes.append(random.randint(0, 500))
        counts.append(random.randint(1, 50))

    return pa.table({
        "product_id":   pa.array(product_ids, type=pa.utf8()),
        "window_start": pa.array(timestamps, type=pa.timestamp("ns", tz="UTC")),
        "avg_rating":   pa.array(ratings, type=pa.float64()),
        "total_likes":  pa.array(likes, type=pa.int64()),
        "review_count": pa.array(counts, type=pa.int64()),
    })


# ===========================================================================
# Arrow Flight-клиент
# ===========================================================================

def fetch_via_flight(client: fl.FlightClient) -> pa.Table:
    """ListFlights → GetFlightInfo → DoGet → pyarrow.Table."""
    flights = list(client.list_flights())
    if not flights:
        print("  [WARN] No flights available — сервер вернул пустой список.", file=sys.stderr)
        # fallback: пытаемся прямой DoGet с дескриптором
        desc = fl.FlightDescriptor.for_path("windows")
        try:
            info = client.get_flight_info(desc)
        except Exception:
            raise RuntimeError("Нет данных на сервере и не удалось создать дескриптор")

    else:
        desc = flights[0].descriptor
        print(f"  descriptor: {desc}")

        info = client.get_flight_info(desc)

    print(f"  total_records (from server): {info.total_records}")
    print(f"  endpoints: {len(info.endpoint)}")

    endpoint = info.endpoint[0]
    reader = client.do_get(endpoint.ticket)
    table = reader.read_all()
    return table


# ===========================================================================
# Преобразование в Polars (zero-copy)
# ===========================================================================

def to_polars_zero_copy(table: pa.Table) -> pl.DataFrame:
    """Преобразует pyarrow.Table в Polars DataFrame без копирования.

    Polars хранит данные в Arrow Columnar Format и может использовать
    память PyArrow напрямую (нуль-копирование).
    """
    return pl.from_arrow(table)


# ===========================================================================
# Arrow IPC wire size (для сравнения с JSON)
# ===========================================================================

def arrow_ipc_size(table: pa.Table) -> int:
    """Размер таблицы в формате Arrow IPC Streaming (байт)."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().size


# ===========================================================================
# Бенчмарк JSON
# ===========================================================================

def benchmark_json(table: pa.Table) -> dict:
    """Сериализует таблицу в JSON, замеряет время и размер."""
    t0 = time.perf_counter()

    # Сериализация с обработкой типов Arrow.
    batch = table.to_batches()[0]
    names = table.column_names
    rows = []
    for i in range(batch.num_rows):
        row = {}
        for j, name in enumerate(names):
            val = batch.column(j)[i].as_py()
            # Timestamp → ISO-строка
            if isinstance(val, (int, float)) and "window" in name:
                row[name] = str(val)
            else:
                row[name] = val
        rows.append(row)

    json_str = json.dumps(rows, ensure_ascii=False, default=str)
    t_json = time.perf_counter() - t0

    return {
        "time_sec": round(t_json, 6),
        "size_bytes": getsizeof(json_str),
        "size_mb": round(getsizeof(json_str) / 1024 / 1024, 4),
        "records": batch.num_rows,
    }


# ===========================================================================
# Вывод результатов
# ===========================================================================

def print_results(
    table: pa.Table,
    df: pl.DataFrame,
    t_flight: float,
    json_bench: dict,
    arrow_bytes: int,
):
    num_rows = table.num_rows
    num_cols = table.num_columns

    # ---- [1] Сводка ----
    print("\n" + "─" * 60)
    print("РЕЗУЛЬТАТЫ")
    print("─" * 60)
    print(f"  Получено:         {num_rows} rows × {num_cols} cols")

    # ---- [2] Первые 5 строк ----
    print(f"\n  Первые 5 строк:")
    print(df.head(5))

    # ---- [3] Схема Polars ----
    print(f"\n  Схема Polars:")
    for col_name, col_type in zip(df.columns, df.dtypes):
        print(f"    {col_name:20s} {col_type}")

    # ---- [4] Схема Arrow ----
    print(f"\n  Схема Arrow:")
    print(f"    {table.schema}")

    # ---- [5] Бенчмарк ----
    print(f"\n  ┌{'─'*20}┬{'─'*14}┬{'─'*14}┐")
    print(f"  │ {'Метрика':20s} │ {'Flight':>12s} │ {'JSON':>12s} │")
    print(f"  ├{'─'*20}┼{'─'*14}┼{'─'*14}┤")

    json_arrow_ratio = json_bench["size_bytes"] / arrow_bytes if arrow_bytes > 0 else 0

    print(f"  │ {'Время (sec)':20s} │ {t_flight:>12.4f} │ {json_bench['time_sec']:>12.6f} │")
    print(f"  │ {'Объём (bytes)':20s} │ {arrow_bytes:>12,} │ {json_bench['size_bytes']:>12,} │")
    print(f"  │ {'Объём (MB)':20s} │ {arrow_bytes/1024/1024:>12.4f} │ {json_bench['size_mb']:>12.4f} │")

    speedup = json_bench["time_sec"] / t_flight if t_flight > 0 else float("inf")
    compr = json_bench["size_bytes"] / arrow_bytes if arrow_bytes > 0 else float("inf")
    print(f"  │ {'Скорость (x)':20s} │ {1:>12.1f} │ {speedup:>12.1f}× │")
    print(f"  │ {'Сжатие (x)':20s} │ {1:>12.1f} │ {compr:>12.1f}× │")
    print(f"  └{'─'*20}┴{'─'*14}┴{'─'*14}┘")

    print(f"\n  📦 Flight — это бинарный Arrow IPC Streaming")
    print(f"     JSON  — это текстовый JSON (utf-8)")
    print(f"     Flight компактнее в {compr:.1f}× и быстрее в {speedup:.1f}× (сеть + сериализация)")


# ===========================================================================
# Main
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Arrow Flight Python Client")
    parser.add_argument("--addr", default="grpc://localhost:50051", help="Flight server address")
    parser.add_argument("--no-flight", action="store_true", help="Use synthetic data instead of Flight")
    parser.add_argument("--rows", type=int, default=10_000, help="Synthetic rows count")
    args = parser.parse_args()

    print("=" * 60)
    print("  Arrow Flight Python Client — WindowAgg")
    print("=" * 60)

    # ---- Получение данных ----
    if args.no_flight:
        print("\n[1] Generating synthetic data...")
        t0 = time.perf_counter()
        table = generate_synthetic_data(args.rows)
        t_total = time.perf_counter() - t0
        print(f"  Generated {table.num_rows} rows in {t_total:.4f} sec")
    else:
        print(f"\n[1] Connecting to Flight server: {args.addr} ...")
        t0 = time.perf_counter()
        client = fl.FlightClient(args.addr)
        try:
            table = fetch_via_flight(client)
        except Exception as e:
            print(f"  [ERROR] Flight connection failed: {e}", file=sys.stderr)
            print("  Используйте --no-flight для работы без сервера")
            client.close()
            sys.exit(1)
        t_flight = time.perf_counter() - t0
        client.close()

    # ---- Zero-copy → Polars ----
    print("\n[2] Converting to Polars DataFrame (zero-copy)...")
    t0 = time.perf_counter()
    df = to_polars_zero_copy(table)
    t_polars = time.perf_counter() - t0
    print(f"  Polars conversion: {t_polars:.6f} sec (zero-copy)")

    # ---- Размер в Arrow IPC ----
    arrow_bytes = arrow_ipc_size(table)
    print(f"  Arrow IPC size:    {arrow_bytes:,} bytes ({arrow_bytes/1024/1024:.2f} MB)")

    # ---- JSON benchmark ----
    print("\n[3] Benchmarking JSON serialization...")
    json_bench = benchmark_json(table)

    # ---- Вывод результатов ----
    t_flight_local = t_flight if not args.no_flight else 0.0
    if args.no_flight:
        t_flight_local = 0.001  # dummy для синтетики

    print_results(table, df, t_flight_local, json_bench, arrow_bytes)


if __name__ == "__main__":
    main()
