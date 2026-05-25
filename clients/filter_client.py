#!/usr/bin/env python3
"""
Python-клиент для фильтрации WindowAgg через Arrow Flight Ticket.

Демонстрирует два способа фильтрации:
  1. GetFlightInfo + path-дескриптор   (стандартный Flight-подход)
  2. DoGet напрямую с JSON-билетом     (быстрый путь)

Использование:
  python clients/filter_client.py                        # через Flight-сервер
  python clients/filter_client.py --no-flight --rows 5000 # синтетика
"""

import argparse
import json
import random
import sys
import time

import polars as pl
import pyarrow as pa
import pyarrow.flight as fl

FLIGHT_ADDR = "grpc://localhost:50051"


# ====================================================================
# 0. Синтетические данные
# ====================================================================

def generate_data(n: int) -> pl.DataFrame:
    """Генерирует Polars DataFrame с колонками WindowAgg."""
    products = [f"WB-{i:03d}" for i in range(1, 51)]  # 50 товаров
    rows = []
    for i in range(n):
        pid = random.choice(products)
        rows.append({
            "product_id": pid,
            "window_start": random.randint(1_700_000_000, 1_800_000_000),
            "avg_rating": round(random.uniform(1.0, 5.0), 2),
            "total_likes": random.randint(0, 500),
            "review_count": random.randint(1, 50),
        })
    return pl.DataFrame(rows).sort("window_start")


# ====================================================================
# 1. Получение через GetFlightInfo дескриптор
# ====================================================================

def fetch_all_from_server(client: fl.FlightClient) -> pl.DataFrame:
    desc = fl.FlightDescriptor.for_path("windows")
    info = client.get_flight_info(desc)
    reader = client.do_get(info.endpoint[0].ticket)
    return pl.from_arrow(reader.read_all())


def fetch_by_product_descriptor(client: fl.FlightClient, product_id: str) -> pl.DataFrame:
    desc = fl.FlightDescriptor.for_path("windows", product_id)
    info = client.get_flight_info(desc)
    reader = client.do_get(info.endpoint[0].ticket)
    return pl.from_arrow(reader.read_all())


# ====================================================================
# 2. Получение через прямой DoGet с JSON-билетом
# ====================================================================

def fetch_by_ticket_json(client: fl.FlightClient, product_id: str = "") -> pl.DataFrame:
    payload = {"cmd": "filter", "product_id": product_id} if product_id else {"cmd": "all"}
    ticket = fl.Ticket(json.dumps(payload).encode("utf-8"))
    return pl.from_arrow(client.do_get(ticket).read_all())


# ====================================================================
# 3. Локальная фильтрация (для --no-flight)
# ====================================================================

def filter_local(df: pl.DataFrame, product_id: str) -> pl.DataFrame:
    return df.filter(pl.col("product_id") == product_id)


# ====================================================================
# Main
# ====================================================================

def main():
    parser = argparse.ArgumentParser(description="Filter WindowAgg via Flight Ticket")
    parser.add_argument("--addr", default=FLIGHT_ADDR, help="Flight server address")
    parser.add_argument("--no-flight", action="store_true", help="use synthetic data")
    parser.add_argument("--rows", type=int, default=10_000, help="synthetic rows")
    args = parser.parse_args()

    print("=" * 65)
    print("  Arrow Flight — фильтрация WindowAgg через Ticket")
    print("=" * 65)

    if args.no_flight:
        # ---- Локальный тест фильтрации ----
        print(f"\n[0] Generating {args.rows} synthetic rows...")
        df_all = generate_data(args.rows)
        products = df_all["product_id"].unique().sort().to_list()
        n_products = len(products)
        print(f"    {len(df_all)} rows, {n_products} unique products")

        target = products[0]
        print(f"\n[1] Testing local filter: product_id = {target}")

        t0 = time.perf_counter()
        df_filtered = filter_local(df_all, target)
        t_filt = time.perf_counter() - t0

        print(f"\n    Result: {len(df_filtered)} rows in {t_filt:.6f}s")
        print(f"\n    Filtered preview:")
        print(df_filtered)

        # Имитация: JSON-билет (просто показываем формат)
        ticket_all = json.dumps({"cmd": "all"})
        ticket_filt = json.dumps({"cmd": "filter", "product_id": target})
        print(f"\n    Ticket (all):  {ticket_all}")
        print(f"    Ticket (filt): {ticket_filt}")
        return

    # ---- Через Flight-сервер ----
    client = fl.FlightClient(args.addr)

    print("\n[1] Listing available flights...")
    for f in client.list_flights():
        print(f"    {f.descriptor}")

    print("\n[2] Fetching ALL records...")
    t0 = time.perf_counter()
    df_all = fetch_all_from_server(client)
    t_all = time.perf_counter() - t0
    print(f"    {len(df_all):>6} rows in {t_all:.4f}s")

    products = df_all["product_id"].unique().sort().to_list()
    print(f"\n[3] Available product IDs ({len(products)} unique):")
    print(f"    {products}")

    if not products:
        print("    [WARN] Сервер пуст.")
        client.close()
        return

    target = products[0]
    print(f"\n    → Target: {target}")

    print(f"\n[4a] Filter via descriptor path (product={target})...")
    t0 = time.perf_counter()
    df_desc = fetch_by_product_descriptor(client, target)
    t_desc = time.perf_counter() - t0
    print(f"    {len(df_desc):>6} rows in {t_desc:.4f}s")

    print(f"\n[4b] Filter via JSON ticket (product={target})...")
    t0 = time.perf_counter()
    df_tkt = fetch_by_ticket_json(client, target)
    t_tkt = time.perf_counter() - t0
    print(f"    {len(df_tkt):>6} rows in {t_tkt:.4f}s")

    print(f"\n[5] Verification:")
    print(f"    All:            {len(df_all):>6}")
    print(f"    Filter desc:    {len(df_desc):>6}")
    print(f"    Filter ticket:  {len(df_tkt):>6}")

    assert len(df_desc) == len(df_tkt), "Mismatch!"
    if not df_desc.is_empty():
        assert df_desc.sort("window_start").to_dict() == df_tkt.sort("window_start").to_dict()
        print("    ✅ Both methods match")

    print(f"\n[6] Filtered preview (product={target}):")
    if not df_tkt.is_empty():
        print(df_tkt)
    else:
        print("    (no data)")

    print(f"\n[7] Speed:")
    print(f"    {'Method':30s} {'Rows':>8s} {'Time':>10s}")
    print(f"    {'-'*30} {'-'*8} {'-'*10}")
    print(f"    {'All (descriptor)':30s} {len(df_all):>8d} {t_all:>10.4f}s")
    print(f"    {'Filter (descriptor)':30s} {len(df_desc):>8d} {t_desc:>10.4f}s")
    print(f"    {'Filter (JSON ticket)':30s} {len(df_tkt):>8d} {t_tkt:>10.4f}s")

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
