#!/usr/bin/env python3
"""
Скрипт генерации нагрузки для проверки HPA.

Генерирует и публикует большое количество отзывов в NATS JetStream,
имитируя работу нескольких сборщиков. Используется для наблюдения
автоскалирования Go-сборщика.

Запуск:
  python load-test.py --nats nats://localhost:4222 --reviews 50000

Для запуска внутри minikube:
  kubectl run load-test --rm -it --image=python:3.12-slim \
    -- bash -c "pip install nats-py && python -c \"$(cat load-test.py)\"" \
    -- --nats nats://nats:4222 --reviews 100000
"""
import argparse
import asyncio
import json
import random
import sys
import time

from nats import connect


WINDOWED_SUBJECT = "reviews.windowed"


async def generate_reviews(n: int) -> list[dict]:
    """Генерирует n WindowAgg-записей с ~15% невалидных."""
    products = [f"WB-{i:03d}" for i in range(1, 101)] + \
               [f"OZ-{i:03d}" for i in range(1, 51)]
    rng = random.Random(42)
    reviews = []
    base_ts = int(time.time()) * 1_000_000_000

    for i in range(n):
        product = rng.choice(products)
        ts = base_ts + i * 60_000_000_000  # +1 min each
        coin = rng.random()

        rating = round(1.0 + rng.random() * 4.0, 2)
        if coin < 0.05:
            rating = round(rng.random() * 0.9 - 0.1, 2)

        likes = rng.randint(0, 500)
        if coin < 0.13:
            likes = -rng.randint(1, 10)

        reviews.append({
            "product_id": product,
            "window_start": ts,
            "avg_rating": rating,
            "total_likes": likes,
            "review_count": rng.randint(1, 50),
        })

    return reviews


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nats", default="nats://localhost:4222")
    parser.add_argument("--reviews", type=int, default=50000,
                        help="количество отзывов для публикации")
    parser.add_argument("--batch", type=int, default=500,
                        help="размер батча для публикации")
    args = parser.parse_args()

    print("=" * 60)
    print("  LOAD TEST — Генератор нагрузки для HPA")
    print("=" * 60)
    print(f"  NATS:       {args.nats}")
    print(f"  Reviews:    {args.reviews:,}")
    print(f"  Batch size: {args.batch}")

    # Генерация
    print(f"\n[1] Generating {args.reviews:,} reviews...")
    t0 = time.perf_counter()
    reviews = await generate_reviews(args.reviews)
    print(f"    Done in {time.perf_counter()-t0:.2f}s")

    # Подключение к NATS
    print(f"\n[2] Connecting to NATS...")
    t0 = time.perf_counter()
    nc = await connect(args.nats)
    js = nc.jetstream()
    print(f"    Connected in {time.perf_counter()-t0:.2f}s")

    # Публикация батчами
    print(f"\n[3] Publishing {args.reviews:,} reviews to '{WINDOWED_SUBJECT}'...")
    t0 = time.perf_counter()
    published = 0
    errors = 0

    for i in range(0, len(reviews), args.batch):
        batch = reviews[i:i + args.batch]
        tasks = []
        for review in batch:
            data = json.dumps(review).encode()
            dedup_key = f"{review['product_id']}-{review['window_start']}"
            tasks.append(
                js.publish(WINDOWED_SUBJECT, data, msg_id=dedup_key)
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                errors += 1
            else:
                published += 1

        elapsed = time.perf_counter() - t0
        rate = published / elapsed if elapsed > 0 else 0
        progress = 100 * published / args.reviews

        print(f"    [{progress:5.1f}%] {published:>8,} / {args.reviews:,} "
              f"published  ({rate:>8,.0f} msgs/sec, {errors} errors)",
              end="\r")

    elapsed = time.perf_counter() - t0
    print(f"\n    Done in {elapsed:.2f}s — "
          f"{published:,} published, {errors:,} errors "
          f"({published/elapsed:,.0f} msgs/sec)")

    # Ждём обработки
    print(f"\n[4] Waiting {5} seconds for consumer to process...")
    await asyncio.sleep(5)

    # Проверяем, что NATS consumer получил сообщения
    try:
        consumer = await js.consumer_info("reviews", "review-collector-worker")
        print(f"    Consumer 'review-collector-worker':")
        print(f"      Delivered:  {consumer.delivered.consumer_seq}")
        print(f"      Pending:    {consumer.num_pending}")
        print(f"      AckPending: {consumer.num_ack_pending}")
    except Exception as e:
        print(f"    Consumer info unavailable: {e}")

    await nc.drain()
    print(f"\n[5] Load test complete. Check HPA scaling:")
    print(f"    kubectl get hpa -n review-pipeline -w")
    print(f"    kubectl get pods -n review-pipeline -w")
    print(f"    kubectl top pods -n review-pipeline")


if __name__ == "__main__":
    asyncio.run(main())
