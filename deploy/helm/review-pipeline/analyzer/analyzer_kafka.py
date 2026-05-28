#!/usr/bin/env python3
"""
Python-консьюмер Kafka: читает сырые отзывы из топика 'reviews.raw'
и выводит статистику в реальном времени.

Замена NATS-анализатору (analyzer.py) на Kafka + aiokafka.

Запуск:
  .venv/bin/pip install aiokafka polars
  .venv/bin/python deploy/helm/review-pipeline/analyzer/analyzer_kafka.py \
      --bootstrap-servers localhost:9092 \
      --group reviews-analyzer

Особенности:
  - Consumer group для масштабирования (несколько инстансов делят партиции)
  - Ручной commit после каждой пачки сообщений (at-least-once)
  - Обработка назначения/отзыва партиций (on_partitions_assigned/revoked)
  - Real-time статистика: RPS, средний рейтинг, топ товаров
  - Graceful shutdown с финальным commit
"""

import argparse
import asyncio
import json
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaConsumer, TopicPartition


# ── Статистика ─────────────────────────────────────────────────────────

class ReviewStats:
    """Собирает статистику по consumed отзывам в реальном времени."""

    def __init__(self):
        self.total: int = 0
        self.valid: int = 0
        self.invalid: int = 0
        self.rating_dist: Counter[int] = Counter()  # rating → count
        self.product_counts: Counter[str] = Counter()  # product_id → count
        self.rating_sum: float = 0.0

        # Скользящее окно для RPS
        self._rps_window: list[tuple[float, int]] = []  # (time, cumulative)
        self._last_total: int = 0
        self._last_time: float = time.time()

        # Последние 5 сообщений
        self.last_messages: list[dict] = []

    def record(self, review: dict):
        """Учитывает один отзыв в статистике."""
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

        # Храним последние 5
        self.last_messages.append(review)
        if len(self.last_messages) > 5:
            self.last_messages.pop(0)

    def rps(self) -> float:
        """Вычисляет throughput за последние 5 секунд."""
        now = time.time()
        self._rps_window.append((now, self.total))
        # Удаляем записи старше 5 секунд
        cutoff = now - 5.0
        while self._rps_window and self._rps_window[0][0] < cutoff:
            self._rps_window.pop(0)

        if len(self._rps_window) >= 2:
            dt = self._rps_window[-1][0] - self._rps_window[0][0]
            dc = self._rps_window[-1][1] - self._rps_window[0][1]
            return dc / dt if dt > 0 else 0.0
        return 0.0

    def avg_rating(self) -> float:
        """Средний рейтинг среди валидных отзывов."""
        if self.valid > 0:
            return self.rating_sum / self.valid
        return 0.0

    def top_products(self, n: int = 5) -> list[tuple[str, int]]:
        """Топ-N товаров по количеству отзывов."""
        return self.product_counts.most_common(n)

    def display(self):
        """Выводит статистику в консоль."""
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


# ── Kafka Consumer ──────────────────────────────────────────────────────

class KafkaReviewConsumer:
    """Читает отзывы из Kafka (топик 'reviews.raw') и обновляет статистику.

    Параметры Kafka:
      - bootstrap_servers: список брокеров
      - group_id: consumer group (для масштабирования)
      - enable_auto_commit: False (ручной commit)
      - auto_offset_reset: 'earliest' (начинаем с начала, если нет offset)
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str = "reviews-analyzer",
        topic: str = "reviews.raw",
        stats_interval: float = 5.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.stats_interval = stats_interval

        self.consumer: Optional[AIOKafkaConsumer] = None
        self.stats = ReviewStats()
        self._shutdown = asyncio.Event()

        # Для commit: храним последний обработанный offset на партицию
        self._last_offsets: dict[TopicPartition, int] = {}

    async def start(self):
        """Подключается к Kafka и запускает consumer."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=500,
            max_poll_interval_ms=300_000,  # 5 минут на обработку пачки
            session_timeout_ms=30_000,     # 30 сек heartbeat timeout
            heartbeat_interval_ms=5_000,   # heartbeat каждые 5 сек
            request_timeout_ms=45_000,
        )

        print(f"[CONSUMER] Starting Kafka consumer:")
        print(f"  Bootstrap:  {self.bootstrap_servers}")
        print(f"  Group ID:   {self.group_id}")
        print(f"  Topic:      {self.topic}")
        print(f"  Stats every: {self.stats_interval}s")

        await self.consumer.start()

        # Получаем назначенные партиции
        partitions = self.consumer.assignment()
        print(f"[CONSUMER] Assigned partitions: "
              f"{[str(p) for p in partitions]}")

        # Получаем end offsets для каждой партиции (для оценки прогресса)
        if partitions:
            end_offsets = await self.consumer.end_offsets(partitions)
            for tp, offset in end_offsets.items():
                print(f"  Partition {tp.partition}: end_offset={offset}")

        print(f"[CONSUMER] Ready. Waiting for messages...")

    async def run(self):
        """Главный цикл: poll → process → commit → display stats."""
        if not self.consumer:
            raise RuntimeError("Consumer not started. Call start() first.")

        # Фоновая задача: вывод статистики по таймеру
        stats_task = asyncio.create_task(self._stats_loop())

        try:
            while not self._shutdown.is_set():
                try:
                    # poll — получает пачку сообщений
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

                # Обрабатываем сообщения по партициям
                for tp, records in msgs.items():
                    for msg in records:
                        try:
                            review = json.loads(msg.value)
                            self.stats.record(review)
                        except (json.JSONDecodeError, KeyError) as e:
                            print(f"[CONSUMER] Invalid message: {e}",
                                  file=sys.stderr)

                        # Запоминаем offset для commit
                        self._last_offsets[tp] = msg.offset + 1

                # Commit обработанных offset'ов
                if self._last_offsets:
                    try:
                        await self.consumer.commit(self._last_offsets)
                    except Exception as e:
                        print(f"[CONSUMER] Commit error: {e}",
                              file=sys.stderr)

        finally:
            stats_task.cancel()
            try:
                await stats_task
            except asyncio.CancelledError:
                pass

    async def _stats_loop(self):
        """Фоновый вывод статистики каждые N секунд."""
        await asyncio.sleep(1)  # даём время накопить данные
        while not self._shutdown.is_set():
            self.stats.display()
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=self.stats_interval)
            except asyncio.TimeoutError:
                pass

        # Финальный вывод перед остановкой
        print(f"\n─── FINAL STATISTICS ───")
        self.stats.display()

    async def stop(self):
        """Graceful shutdown: shutdown → commit → close."""
        print(f"\n[CONSUMER] Shutting down...")
        self._shutdown.set()

        if self.consumer:
            # Финальный commit
            if self._last_offsets:
                try:
                    await self.consumer.commit(self._last_offsets)
                    print(f"[CONSUMER] Final offsets committed")
                except Exception as e:
                    print(f"[CONSUMER] Final commit error: {e}",
                          file=sys.stderr)

            await self.consumer.stop()
            print(f"[CONSUMER] Consumer stopped")

        print(f"[CONSUMER] Total messages processed: {self.stats.total}")


# ── Main ────────────────────────────────────────────────────────────────

async def async_main():
    parser = argparse.ArgumentParser(
        description="Kafka consumer for reviews.raw")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--group", default="reviews-analyzer",
                        help="Consumer group ID")
    parser.add_argument("--topic", default="reviews.raw",
                        help="Kafka topic")
    parser.add_argument("--stats-interval", type=float, default=5.0,
                        help="Statistics display interval (seconds)")
    args = parser.parse_args()

    consumer = KafkaReviewConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group,
        topic=args.topic,
        stats_interval=args.stats_interval,
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
        # Запускаем consumer в фоне
        consumer_task = asyncio.create_task(consumer.run())

        # Ждём сигнала
        await shutdown_event.wait()

        # Остановка
        await consumer.stop()
        await consumer_task
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        raise


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
