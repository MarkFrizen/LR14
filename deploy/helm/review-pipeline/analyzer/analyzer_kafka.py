#!/usr/bin/env python3
"""
Python-консьюмер Kafka: читает сырые отзывы из топика 'reviews.raw',
выводит статистику в реальном времени и публикует агрегаты
по скользящему окну (5 мин, сдвиг 1 мин) в топик 'reviews.aggregated'.

Замена NATS-анализатору (analyzer.py) на Kafka + aiokafka.

Запуск:
  .venv/bin/pip install aiokafka
  .venv/bin/python deploy/helm/review-pipeline/analyzer/analyzer_kafka.py \
      --bootstrap-servers localhost:9092 \
      --group reviews-analyzer

Особенности:
  - Consumer group для масштабирования
  - Ручной commit после каждой пачки (at-least-once)
  - Sliding window 5 мин / сдвиг 1 мин: средний рейтинг, кол-во, доля негативных
  - Публикация агрегатов в 'reviews.aggregated'
  - Graceful shutdown
"""

import argparse
import asyncio
import json
import signal
import sys
import time
from collections import Counter, deque
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition


# ══════════════════════════════════════════════════════════════════════
#  Sliding Window Aggregator (deque, 5 мин / сдвиг 1 мин)
# ══════════════════════════════════════════════════════════════════════

class SlidingWindowAggregator:
    """Скользящее окно для per-product агрегации отзывов.

    Хранит все отзывы за последние `window_size` секунд в deque.
    Каждые `slide_interval` секунд:
      1. Удаляет записи старше window_size
      2. Считает агрегаты по каждому product_id
      3. Выводит в консоль
      4. Публикует в Kafka (если producer передан)

    Потокобезопасность: все операции в одной корутине (asyncio).
    """

    def __init__(
        self,
        window_size: float = 300.0,     # 5 минут
        slide_interval: float = 60.0,    # 1 минута
        producer: Optional[AIOKafkaProducer] = None,
        agg_topic: str = "reviews.aggregated",
    ):
        self.window_size = window_size
        self.slide_interval = slide_interval
        self.producer = producer
        self.agg_topic = agg_topic

        # Кольцевой буфер: deque из (arrival_ts, product_id, rating, review_date_str)
        self._buffer: deque[tuple[float, str, int, str]] = deque()
        self._total_added: int = 0

    # ── публичный API ─────────────────────────────────────────────

    def add(self, product_id: str, rating: int,
            review_date: Optional[str] = None):
        """Добавляет отзыв в окно (вызывается из consumer).

        Args:
            product_id: ID товара
            rating: оценка (1-5)
            review_date: ISO-строка даты сбора отзыва (для latency)
        """
        now = time.time()
        self._buffer.append((now, product_id, rating,
                             review_date or datetime.now(timezone.utc).isoformat()))
        self._total_added += 1

    def prune(self):
        """Удаляет записи старше window_size секунд."""
        cutoff = time.time() - self.window_size
        while self._buffer and self._buffer[0][0] < cutoff:
            self._buffer.popleft()

    def compute(self) -> list[dict]:
        """Вычисляет агрегаты по каждому product_id в текущем окне.

        Возвращает список словарей:
          {
            "window_start":   "ISO",
            "window_end":     "ISO",
            "computed_at":    "ISO",
            "product_id":     "WB-001",
            "review_count":   42,
            "avg_rating":     4.2,
            "negative_share": 0.05,          # rating < 3
            "max_review_date": "2026-05-28T12:04:00",  # самый свежий отзыв в окне
            "latency_sec":    90.0,          # computed_at - max_review_date
          }
        """
        now = time.time()
        window_end_dt = datetime.fromtimestamp(now, tz=timezone.utc)
        window_start_dt = datetime.fromtimestamp(
            now - self.window_size, tz=timezone.utc)

        # Группируем по product_id
        per_product: dict[str, tuple[list[int], list[str]]] = {}
        for _ts, pid, rating, rdate in self._buffer:
            if pid not in per_product:
                per_product[pid] = ([], [])
            per_product[pid][0].append(rating)
            if rdate:
                per_product[pid][1].append(rdate)

        results: list[dict] = []
        for pid, (ratings, dates) in sorted(per_product.items()):
            n = len(ratings)
            avg_r = sum(ratings) / n if n > 0 else 0.0
            neg = sum(1 for r in ratings if r < 3) / n if n > 0 else 0.0

            # latency: самый свежий review_date → now
            max_review_date = ""
            latency_sec = 0.0
            if dates:
                max_review_date = max(dates)
                try:
                    dt = datetime.fromisoformat(max_review_date)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    latency_sec = max(0.0, (window_end_dt - dt).total_seconds())
                except (ValueError, TypeError):
                    pass

            results.append({
                "window_start":   window_start_dt.isoformat(),
                "window_end":     window_end_dt.isoformat(),
                "computed_at":    window_end_dt.isoformat(),
                "product_id":     pid,
                "review_count":   n,
                "avg_rating":     round(avg_r, 2),
                "negative_share": round(neg, 4),
                "max_review_date": max_review_date,
                "latency_sec":    round(latency_sec, 2),
            })

        return results

    def display(self, results: list[dict]):
        """Выводит агрегаты в консоль."""
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"\n─── [{now}] Sliding Window Aggregates ───")
        print(f"  Window: {self.window_size / 60:.0f} min, "
              f"slide: {self.slide_interval / 60:.0f} min, "
              f"buffer: {len(self._buffer)} reviews")
        if not results:
            print("  (no data in window)")
        else:
            # Заголовок
            print(f"  {'Product':<12} {'Count':>6} {'AvgRat':>7} {'NegShare':>9}")
            print(f"  {'─'*12} {'─'*6} {'─'*7} {'─'*9}")
            for r in results:
                print(f"  {r['product_id']:<12} {r['review_count']:>6} "
                      f"{r['avg_rating']:>7.2f} {r['negative_share']:>9.1%}")
        print(f"{'─'*50}")

    async def publish(self, results: list[dict]):
        """Публикует агрегаты в Kafka (топик 'reviews.aggregated')."""
        if not self.producer or not results:
            return

        for agg in results:
            key = agg["product_id"].encode()
            value = json.dumps(agg).encode()
            try:
                await self.producer.send(
                    self.agg_topic,
                    key=key,
                    value=value,
                )
            except Exception as e:
                print(f"[WINDOW] Publish error: {e}", file=sys.stderr)

        # Ждём подтверждения (flush)
        try:
            await self.producer.flush()
            print(f"[WINDOW] Published {len(results)} aggregates "
                  f"to '{self.agg_topic}'")
        except Exception as e:
            print(f"[WINDOW] Flush error: {e}", file=sys.stderr)

    def stats(self) -> dict:
        """Текущее состояние окна (для отладки)."""
        return {
            "buffer_size": len(self._buffer),
            "total_added": self._total_added,
            "window_sec": self.window_size,
        }


# ══════════════════════════════════════════════════════════════════════
#  Глобальная статистика (cumulative — не окно)
# ══════════════════════════════════════════════════════════════════════

class ReviewStats:
    """Собирает кумулятивную статистику по consumed отзывам."""

    def __init__(self):
        self.total: int = 0
        self.valid: int = 0
        self.invalid: int = 0
        self.rating_dist: Counter[int] = Counter()  # rating → count
        self.product_counts: Counter[str] = Counter()
        self.rating_sum: float = 0.0

        self._rps_window: list[tuple[float, int]] = []
        self.last_messages: list[dict] = []

    def record(self, review: dict):
        self.total += 1
        rating = review.get("rating", 0)
        product_id = review.get("product_id", "unknown")

        if 1 <= rating <= 5:
            self.valid += 1
            self.rating_dist[rating] += 1
            self.rating_sum += rating
        else:
            self.invalid += 1

        self.product_counts[product_id] += 1

        self.last_messages.append(review)
        if len(self.last_messages) > 5:
            self.last_messages.pop(0)

    def rps(self) -> float:
        now = time.time()
        self._rps_window.append((now, self.total))
        cutoff = now - 5.0
        while self._rps_window and self._rps_window[0][0] < cutoff:
            self._rps_window.pop(0)
        if len(self._rps_window) >= 2:
            dt = self._rps_window[-1][0] - self._rps_window[0][0]
            dc = self._rps_window[-1][1] - self._rps_window[0][1]
            return dc / dt if dt > 0 else 0.0
        return 0.0

    def avg_rating(self) -> float:
        return self.rating_sum / self.valid if self.valid > 0 else 0.0

    def top_products(self, n: int = 5) -> list[tuple[str, int]]:
        return self.product_counts.most_common(n)

    def display(self):
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"\n─── [{now}] Kafka Consumer Statistics ───")
        print(f"  Total consumed:    {self.total:>8}")
        print(f"  Valid / Invalid:   {self.valid:>8} / {self.invalid:<8}")
        print(f"  Throughput:        {self.rps():>8.1f} msg/s")
        print(f"  Avg rating:        {self.avg_rating():>8.2f} / 5.0")
        print(f"  Rating distr:      ", end="")
        for r in range(1, 6):
            pct = (self.rating_dist[r] / self.valid * 100) if self.valid > 0 else 0
            print(f"{r}★ {pct:.0f}%  ", end="")
        print()
        print(f"  Top products:")
        for pid, cnt in self.top_products(5):
            print(f"    {pid:<12} {cnt:>5} reviews")
        print(f"  Last 5 reviews:")
        for m in self.last_messages:
            rid = m.get("id", "?")[:20]
            pid = m.get("product_id", "?")
            rat = m.get("rating", "?")
            print(f"    {rid:<20} product={pid:<8} rating={rat}")
        print(f"{'─'*50}")


# ══════════════════════════════════════════════════════════════════════
#  Kafka Consumer
# ══════════════════════════════════════════════════════════════════════

class KafkaReviewConsumer:
    """Читает отзывы из Kafka (топик 'reviews.raw'), обновляет
    кумулятивную статистику и скользящее окно."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str = "reviews-analyzer",
        topic: str = "reviews.raw",
        stats_interval: float = 5.0,
        window_size: float = 300.0,
        slide_interval: float = 60.0,
        agg_topic: str = "reviews.aggregated",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.stats_interval = stats_interval
        self.agg_topic = agg_topic

        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.stats = ReviewStats()
        self.window = SlidingWindowAggregator(
            window_size=window_size,
            slide_interval=slide_interval,
            agg_topic=agg_topic,
        )
        self._shutdown = asyncio.Event()
        self._last_offsets: dict[TopicPartition, int] = {}

    async def start(self):
        """Подключается к Kafka (consumer + producer)."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=500,
            max_poll_interval_ms=300_000,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=5_000,
            request_timeout_ms=45_000,
        )

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks=1,              # ждём подтверждения от лидера
            compression_type="snappy",
        )
        self.window.producer = self.producer  # связываем

        print(f"[CONSUMER] Starting Kafka consumer + producer:")
        print(f"  Bootstrap:    {self.bootstrap_servers}")
        print(f"  Group ID:     {self.group_id}")
        print(f"  Topic (in):   {self.topic}")
        print(f"  Topic (out):  {self.agg_topic}")
        print(f"  Stats every:  {self.stats_interval}s")
        print(f"  Window size:  {window_size / 60:.0f} min / "
              f"slide {slide_interval / 60:.0f} min")

        await self.consumer.start()
        await self.producer.start()

        partitions = self.consumer.assignment()
        print(f"[CONSUMER] Assigned partitions: "
              f"{[str(p) for p in partitions]}")

        if partitions:
            end_offsets = await self.consumer.end_offsets(partitions)
            for tp, offset in end_offsets.items():
                print(f"  Partition {tp.partition}: end_offset={offset}")

        print(f"[CONSUMER] Ready. Waiting for messages...")

    async def run(self):
        """Главный цикл: poll → process → commit.
        Параллельно запускает _stats_loop и _window_loop."""
        if not self.consumer:
            raise RuntimeError("Consumer not started. Call start() first.")

        stats_task = asyncio.create_task(self._stats_loop())
        window_task = asyncio.create_task(self._window_loop())

        try:
            while not self._shutdown.is_set():
                try:
                    msgs = await self.consumer.getmany(
                        timeout_ms=2000,
                        max_records=500,
                    )
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"[CONSUMER] Poll error: {e}", file=sys.stderr)
                    await asyncio.sleep(1)
                    continue

                if not msgs:
                    continue

                for tp, records in msgs.items():
                    for msg in records:
                        try:
                            review = json.loads(msg.value)
                            # ── кумулятивная статистика ──
                            self.stats.record(review)
                            # ── скользящее окно ──
                            pid = review.get("product_id", "unknown")
                            rating = review.get("rating", 0)
                            rev_date = review.get("date")
                            if 1 <= rating <= 5:
                                self.window.add(pid, rating,
                                                review_date=rev_date)
                        except (json.JSONDecodeError, KeyError) as e:
                            print(f"[CONSUMER] Invalid message: {e}",
                                  file=sys.stderr)

                        self._last_offsets[tp] = msg.offset + 1

                if self._last_offsets:
                    try:
                        await self.consumer.commit(self._last_offsets)
                    except Exception as e:
                        print(f"[CONSUMER] Commit error: {e}",
                              file=sys.stderr)

        finally:
            for t in (stats_task, window_task):
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    # ── фоновые задачи ────────────────────────────────────────────

    async def _stats_loop(self):
        """Вывод кумулятивной статистики каждые N секунд."""
        await asyncio.sleep(1)
        while not self._shutdown.is_set():
            self.stats.display()
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=self.stats_interval)
            except asyncio.TimeoutError:
                pass
        print(f"\n─── FINAL STATISTICS ───")
        self.stats.display()

    async def _window_loop(self):
        """Периодический сдвиг окна: prune → compute → display → publish.

        Запускается каждые `slide_interval` секунд.
        При старте ждёт slide_interval, чтобы накопить данные.
        """
        # Пропускаем первый период (даём окну заполниться)
        try:
            await asyncio.wait_for(
                self._shutdown.wait(), timeout=self.window.slide_interval)
            return  # shutdown во время ожидания
        except asyncio.TimeoutError:
            pass

        while not self._shutdown.is_set():
            # 1. Prune — удаляем устаревшие записи
            before = len(self.window._buffer)
            self.window.prune()
            after = len(self.window._buffer)

            # 2. Compute — считаем агрегаты
            results = self.window.compute()

            # 3. Display — выводим в консоль
            self.window.display(results)

            if before != after:
                print(f"[WINDOW] Pruned {before - after} old entries "
                      f"(buffer: {before} → {after})")

            # 4. Publish — отправляем в Kafka
            await self.window.publish(results)

            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=self.window.slide_interval)
                break
            except asyncio.TimeoutError:
                continue

    # ── shutdown ───────────────────────────────────────────────────

    async def stop(self):
        """Graceful shutdown: shutdown → commit → close."""
        print(f"\n[CONSUMER] Shutting down...")
        self._shutdown.set()

        if self.consumer:
            if self._last_offsets:
                try:
                    await self.consumer.commit(self._last_offsets)
                    print(f"[CONSUMER] Final offsets committed")
                except Exception as e:
                    print(f"[CONSUMER] Final commit error: {e}",
                          file=sys.stderr)
            await self.consumer.stop()
            print(f"[CONSUMER] Consumer stopped")

        # Финальный flush агрегатов
        if self.window._buffer:
            self.window.prune()
            results = self.window.compute()
            if results:
                self.window.display(results)
                await self.window.publish(results)

        if self.producer:
            await self.producer.stop()
            print(f"[CONSUMER] Producer stopped")

        print(f"[CONSUMER] Total messages processed: {self.stats.total}")


# ══════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════

async def async_main():
    parser = argparse.ArgumentParser(
        description="Kafka consumer for reviews.raw + sliding window aggregator")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--group", default="reviews-analyzer",
                        help="Consumer group ID")
    parser.add_argument("--topic", default="reviews.raw",
                        help="Input Kafka topic")
    parser.add_argument("--agg-topic", default="reviews.aggregated",
                        help="Output Kafka topic for aggregates")
    parser.add_argument("--stats-interval", type=float, default=5.0,
                        help="Statistics display interval (seconds)")
    parser.add_argument("--window-size", type=float, default=300.0,
                        help="Sliding window size (seconds, default 300 = 5 min)")
    parser.add_argument("--slide-interval", type=float, default=60.0,
                        help="Window slide interval (seconds, default 60 = 1 min)")
    args = parser.parse_args()

    # Валидация
    if args.window_size <= 0:
        print("[ERROR] window-size must be positive", file=sys.stderr)
        sys.exit(1)
    if args.slide_interval <= 0:
        print("[ERROR] slide-interval must be positive", file=sys.stderr)
        sys.exit(1)
    if args.slide_interval > args.window_size:
        print("[WARN] slide-interval > window-size: окно будет "
              "полностью очищаться при каждом сдвиге", file=sys.stderr)

    consumer = KafkaReviewConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group,
        topic=args.topic,
        stats_interval=args.stats_interval,
        window_size=args.window_size,
        slide_interval=args.slide_interval,
        agg_topic=args.agg_topic,
    )

    # Signal handling
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_signal():
        print(f"\n[SIGNAL] Received, shutting down...")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    try:
        await consumer.start()
        consumer_task = asyncio.create_task(consumer.run())
        await shutdown_event.wait()
        await consumer.stop()
        await consumer_task
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        raise


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
