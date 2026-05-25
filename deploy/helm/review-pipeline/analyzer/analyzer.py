"""
Python-анализатор: читает WindowAgg из NATS JetStream, агрегирует и сохраняет.

Запуск:
  python analyzer.py --nats nats://localhost:4222
"""
import argparse
import asyncio
import json
import signal
import sys
import time

import polars as pl
from nats import connect
from nats.js import JetStreamContext


STREAM_NAME = "reviews"
WINDOWED_SUBJECT = "reviews.windowed"
CONSUMER_NAME = "analyzer-consumer"

# ── Глобальный буфер для окон ─────────────────────────────────────────
window_buffer: list[dict] = []
BATCH_SIZE = 100
FLUSH_INTERVAL = 30  # секунд


async def on_message(msg, df: pl.DataFrame):
    """Обработчик входящих WindowAgg."""
    data = json.loads(msg.data)
    window_buffer.append(data)
    await msg.ack()

    if len(window_buffer) >= BATCH_SIZE:
        flush_buffer()


def flush_buffer():
    """Сбрасывает буфер в Polars DataFrame с агрегацией."""
    global window_buffer
    if not window_buffer:
        return

    batch = pl.DataFrame(window_buffer)
    window_buffer = []

    # Сохраняем или обновляем Parquet
    import os
    parquet_path = "/data/aggregated_windows.parquet"

    try:
        if os.path.exists(parquet_path):
            existing = pl.read_parquet(parquet_path)
            combined = pl.concat([existing, batch]).unique(
                subset=["product_id", "window_start"],
                keep="last",
            )
        else:
            combined = batch

        combined.write_parquet(parquet_path)
        print(f"  [ANALYZER] Flushed {len(batch)} windows to {parquet_path} "
              f"(total: {len(combined)} rows)")
    except Exception as e:
        print(f"  [ANALYZER] ERROR flushing buffer: {e}", file=sys.stderr)


async def periodic_flush():
    """Фоновая задача: сбрасывает буфер по таймеру."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        flush_buffer()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nats", default="nats://localhost:4222")
    parser.add_argument("--flush-interval", type=int, default=FLUSH_INTERVAL)
    args = parser.parse_args()

    globals()['FLUSH_INTERVAL'] = args.flush_interval

    print(f"[ANALYZER] Connecting to NATS: {args.nats}")
    nc = await connect(args.nats)
    js: JetStreamContext = nc.jetstream()

    # Создаём/находим durable consumer
    try:
        await js.add_consumer(
            STREAM_NAME,
            config={
                "durable_name": CONSUMER_NAME,
                "ack_policy": "explicit",
                "filter_subject": WINDOWED_SUBJECT,
                "max_deliver": 3,
                "replay_policy": "instant",
            },
        )
    except Exception:
        pass  # уже существует

    print(f"[ANALYZER] Subscribing to {WINDOWED_SUBJECT}...")
    sub = await js.pull_subscribe(WINDOWED_SUBJECT, durable=CONSUMER_NAME)

    # Запускаем периодический flush
    asyncio.create_task(periodic_flush())

    print(f"[ANALYZER] Ready. Waiting for messages...")

    # Основной цикл pull
    shutdown = False

    def handle_sig(sig, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    while not shutdown:
        try:
            msgs = await sub.fetch(10, timeout=10)
            for msg in msgs:
                await on_message(msg, None)
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"[ANALYZER] Error: {e}", file=sys.stderr)

    # Финальный flush
    flush_buffer()
    await nc.drain()
    print("[ANALYZER] Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
