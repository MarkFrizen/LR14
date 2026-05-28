#!/usr/bin/env python3
"""
Python-сборщик отзывов с маркетплейса (аналог Go-версии).

Читает список product_id из etcd, имитирует API Wildberries/Ozon через aiohttp,
публикует отзывы в NATS JetStream (тема "reviews.raw").

Запуск:
  .venv/bin/pip install aiohttp nats-py
  .venv/bin/python clients/collector.py \\
      --etcd localhost:2379 \\
      --nats nats://localhost:4222 \\
      --source wildberries
"""

import argparse
import asyncio
import base64
import csv
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import psutil


# ====================================================================
# 1. Модель данных (аналог Go: internal/models/review.go)
# ====================================================================

@dataclass
class Review:
    """Один отзыв с маркетплейса."""
    id: str
    product_id: str
    rating: int
    text: str
    likes: int
    dislikes: int
    date: str  # ISO 8601


# ====================================================================
# 1b. Сборщик метрик производительности (psutil + CSV)
# ====================================================================

@dataclass
class MetricsSnapshot:
    """Снимок метрик в один момент времени."""
    timestamp: float       # unix seconds
    elapsed: float         # seconds since start
    total_reviews: int
    reviews_per_sec: float # за последний интервал
    rss_mb: float          # Resident Set Size в MB
    cpu_percent: float     # CPU процесса (% от одного ядра)


class MetricsCollector:
    """Фоновый сбор метрик: psutil + CSV-логирование каждые N секунд.

    Аналог Go: runtime.ReadMemStats + Prometheus, но в упрощённом виде.
    """

    def __init__(self, csv_path: str, interval: float = 5.0,
                 logger: logging.Logger | None = None):
        self.csv_path = Path(csv_path)
        self.interval = interval
        self.logger = logger or logging.getLogger("metrics")
        self.process = psutil.Process()
        self._task: asyncio.Task | None = None

        # Счётчики
        self.total_reviews: int = 0
        self._prev_reviews: int = 0
        self._prev_time: float = 0.0
        self._start_time: float = 0.0

        # Для скользящего RPS (за последние interval секунд)
        self._rps_window: list[tuple[float, int]] = []  # (time, cumulative)

        self._snapshots: list[MetricsSnapshot] = []

        # Инициализация CPU-счётчика (первый вызов cpu_percent возвращает 0)
        self.process.cpu_percent(interval=None)

        # Инициализация CSV (заголовок)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.csv_path.exists():
            with open(self.csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "elapsed_s",
                    "total_reviews", "reviews_per_sec",
                    "rss_mb", "cpu_percent",
                ])

    def record_reviews(self, count: int = 1):
        """Увеличивает счётчик собранных отзывов."""
        self.total_reviews += count

    async def start(self):
        """Запускает фоновый цикл снятия метрик."""
        self._start_time = time.time()
        self._prev_time = self._start_time
        self._task = asyncio.create_task(self._run_loop())
        if self.logger:
            self.logger.info(
                "METRICS_STARTED",
                extra={"csv": str(self.csv_path), "interval": self.interval},
            )

    async def _run_loop(self):
        """Цикл: измерение → запись CSV → ожидание."""
        while True:
            await asyncio.sleep(self.interval)
            try:
                snapshot = self._take_snapshot()
                self._snapshots.append(snapshot)
                self._append_csv(snapshot)
                if self.logger:
                    self.logger.info(
                        "METRICS",
                        extra={
                            "rps": f"{snapshot.reviews_per_sec:.1f}",
                            "rss_mb": f"{snapshot.rss_mb:.1f}",
                            "cpu": f"{snapshot.cpu_percent:.1f}%",
                            "total": snapshot.total_reviews,
                        },
                    )
            except Exception as e:
                if self.logger:
                    self.logger.warning("METRICS_ERROR", extra={"error": str(e)})

    def _take_snapshot(self) -> MetricsSnapshot:
        """Делает снимок текущих метрик."""
        now = time.time()
        elapsed = now - self._start_time
        interval_dt = now - self._prev_time

        # Прирост отзывов за интервал → RPS
        delta_reviews = self.total_reviews - self._prev_reviews
        rps = delta_reviews / interval_dt if interval_dt > 0 else 0.0

        # Память и CPU
        try:
            rss_mb = self.process.memory_info().rss / 1024 / 1024
        except Exception:
            rss_mb = 0.0
        try:
            cpu_pct = self.process.cpu_percent(interval=0)
        except Exception:
            cpu_pct = 0.0

        # Обновляем предыдущие значения
        self._prev_reviews = self.total_reviews
        self._prev_time = now

        return MetricsSnapshot(
            timestamp=now,
            elapsed=elapsed,
            total_reviews=self.total_reviews,
            reviews_per_sec=rps,
            rss_mb=rss_mb,
            cpu_percent=cpu_pct,
        )

    def _append_csv(self, s: MetricsSnapshot):
        """Дописывает одну строку в CSV."""
        with open(self.csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                f"{s.timestamp:.3f}",
                f"{s.elapsed:.3f}",
                s.total_reviews,
                f"{s.reviews_per_sec:.2f}",
                f"{s.rss_mb:.2f}",
                f"{s.cpu_percent:.2f}",
            ])

    def get_summary(self) -> dict:
        """Возвращает сводку по всем снимкам (средние, пиковые значения)."""
        if not self._snapshots:
            return {}
        rps_vals = [s.reviews_per_sec for s in self._snapshots]
        rss_vals = [s.rss_mb for s in self._snapshots]
        cpu_vals = [s.cpu_percent for s in self._snapshots]
        return {
            "duration_s": self._snapshots[-1].elapsed,
            "total_reviews": self._snapshots[-1].total_reviews,
            "avg_rps": sum(rps_vals) / len(rps_vals),
            "peak_rps": max(rps_vals),
            "avg_rss_mb": sum(rss_vals) / len(rss_vals),
            "peak_rss_mb": max(rss_vals),
            "avg_cpu_pct": sum(cpu_vals) / len(cpu_vals),
            "peak_cpu_pct": max(cpu_vals),
        }

    async def stop(self):
        """Останавливает сбор метрик."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # Финальный снимок
        if self.total_reviews > 0:
            final = self._take_snapshot()
            self._snapshots.append(final)
            self._append_csv(final)
        if self.logger:
            summary = self.get_summary()
            self.logger.info("METRICS_FINAL", extra=summary)


# ====================================================================
# 2. Асинхронный etcd-клиент (через HTTP v3 API)
# ====================================================================

class EtcdClient:
    """Клиент etcd v3 через HTTP gateway.

    Использует тот же набор etcd-ключей, что и Go-версия:
      /collector/products/{productID}   — список товаров
      /collector/assignments/{productID} — назначения шардов
    """

    def __init__(self, endpoints: list[str], logger: logging.Logger):
        self.endpoints = endpoints
        self.logger = logger
        self._session: Optional[aiohttp.ClientSession] = None
        self._lease_id: Optional[int] = None
        self._keepalive_task: Optional[asyncio.Task] = None

    async def _request(self, path: str, body: Optional[dict] = None) -> dict:
        """POST-запрос к etcd v3 HTTP API."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5),
            )

        last_err: Optional[Exception] = None
        for ep in self.endpoints:
            url = f"http://{ep}/v3/{path}"
            try:
                async with self._session.post(url, json=body or {}) as resp:
                    data = await resp.json()
                    if resp.status >= 400:
                        raise RuntimeError(
                            f"etcd error {resp.status} on {ep}: {data}")
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as e:
                last_err = e
                continue

        raise RuntimeError(
            f"etcd unreachable (tried {self.endpoints}): {last_err}")

    # ── лизинг ──────────────────────────────────────────────────────

    async def grant_lease(self, ttl: int = 10) -> int:
        """Создаёт лизинг (аналог etcd Lease.Grant)."""
        data = await self._request("lease/grant", {"TTL": ttl})
        self._lease_id = data["ID"]
        self.logger.info("ETCD_LEASE_GRANTED",
                         extra={"lease_id": self._lease_id, "ttl": ttl})
        return self._lease_id

    async def _keepalive_loop(self, interval: float = 5.0):
        """Фоновое продление лизинга (аналог KeepAlive)."""
        while True:
            try:
                await asyncio.sleep(interval)
                if self._lease_id is not None:
                    await self._request("lease/keepalive",
                                        {"ID": self._lease_id})
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.warning("ETCD_KEEPALIVE_FAILED",
                                    extra={"error": str(e)})

    async def start_keepalive(self):
        """Запускает фоновый keepalive."""
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    # ── KV ──────────────────────────────────────────────────────────

    async def put(self, key: str, value: str,
                  lease: Optional[int] = None) -> dict:
        """Записывает ключ (аналог etcd KV.Put)."""
        body: dict = {
            "key": _b64(key),
            "value": _b64(value),
        }
        if lease is not None:
            body["lease"] = lease
        return await self._request("kv/put", body)

    async def get(self, key: str) -> Optional[dict]:
        """Читает один ключ (аналог etcd KV.Range c limit=1)."""
        data = await self._request("kv/range", {
            "key": _b64(key),
            "limit": 1,
        })
        kvs = data.get("kvs", [])
        return kvs[0] if kvs else None

    async def get_prefix(self, prefix: str) -> list[dict]:
        """Читает ключи по префиксу (аналог WithPrefix)."""
        data = await self._request("kv/range", {
            "key": _b64(prefix),
            "range_end": _b64(_prefix_end(prefix)),
        })
        return data.get("kvs", [])

    async def txn_create_if_not_exists(self, key: str, value: str,
                                       lease: Optional[int] = None) -> bool:
        """CAS-транзакция: создаёт ключ, только если его нет.

        Аналог Go-конструкции:
          If(CreateRevision(key) == 0).Then(OpPut(key, value, WithLease(...)))
        """
        ops: list[dict] = [{
            "request_put": {
                "key": _b64(key),
                "value": _b64(value),
            }
        }]
        if lease is not None:
            ops[0]["request_put"]["lease"] = lease

        data = await self._request("kv/txn", {
            "compare": [{
                "key": _b64(key),
                "result": "EQUAL",
                "target": "CREATE",
                "create_revision": 0,
            }],
            "success": ops,
            "failure": [],
        })
        return data.get("succeeded", False)

    async def delete(self, key: str) -> dict:
        """Удаляет ключ (аналог etcd KV.DeleteRange)."""
        return await self._request("kv/deleterange", {
            "key": _b64(key),
        })

    async def revoke_lease(self):
        """Отзывает лизинг (аналог Lease.Revoke)."""
        if self._lease_id is not None:
            try:
                await self._request("lease/revoke", {"ID": self._lease_id})
                self.logger.info("ETCD_LEASE_REVOKED",
                                 extra={"lease_id": self._lease_id})
            except Exception as e:
                self.logger.warning("ETCD_LEASE_REVOKE_FAILED",
                                    extra={"error": str(e)})

    async def close(self):
        """Закрывает соединение."""
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass

        if self._session:
            await self._session.close()

    # ── утилиты ─────────────────────────────────────────────────────

    @staticmethod
    def decode_kv(kv: dict) -> tuple[str, str]:
        """Декодирует ключ и значение из base64."""
        key = base64.b64decode(kv["key"]).decode()
        val = base64.b64decode(kv.get("value", "")).decode() if kv.get("value") else ""
        return key, val


# Base64-вспомогательные функции для etcd API
def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _prefix_end(prefix: str) -> str:
    """Вычисляет range_end для префиксного запроса (эксклюзивная граница).

    Для пустого префикса возвращает \x00 (весь keyspace).
    Иначе инкрементирует последний байт.
    """
    if not prefix:
        return "\x00"
    b = bytearray(prefix.encode("utf-8"))
    for i in range(len(b) - 1, -1, -1):
        if b[i] < 0xFF:
            b[i] += 1
            return bytes(b[:i + 1]).decode("utf-8", errors="replace")
    return "\x00"  # prefix из \xFF…\xFF


# ====================================================================
# 3. Симулятор маркетплейса (аналог Go: internal/marketplace/simulator.go)
# ====================================================================

class MarketSimulator:
    """Имитирует API Wildberries/Ozon.

    В реальном проекте здесь был бы aiohttp-клиент к api.wildberries.ru.
    """

    REVIEW_TEXTS = {
        1: ["Ужасное качество, не советую!",
            "Не работает, деньги на ветер.",
            "Очень разочарован покупкой."],
        2: ["Плохо, но есть плюсы.",
            "Ожидал большего за такие деньги.",
            "Не рекомендую, много недостатков."],
        3: ["Нормально, но не более.",
            "Средне, могло быть и лучше.",
            "Своих денег стоит, но без восторга."],
        4: ["Хороший товар, почти всё устроило.",
            "Доволен покупкой, рекомендую.",
            "Качественно, есть мелкие недочёты."],
        5: ["Отличный товар! Всё супер!",
            "Лучшая покупка в этом месяце!",
            "Быстрая доставка, качество на высоте."],
    }

    def __init__(self, source: str):
        self.source = source  # "wildberries" или "ozon"

    async def fetch_reviews(self, product_id: str, limit: int = 5
                            ) -> list[Review]:
        """Собирает отзывы для товара (симуляция HTTP-запроса).

        Аналог Go: FetchReviews(ctx, productID, limit).
        """
        # Симулируем задержку сетевого запроса (50-200ms)
        delay = 0.05 + random.random() * 0.15
        await asyncio.sleep(delay)

        n = 1 + random.randint(0, limit)
        reviews: list[Review] = []

        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for i in range(n):
            rating = 1 + random.randint(0, 4)
            likes = random.randint(0, 49)
            dislikes = random.randint(0, 19)
            hours_ago = random.randint(0, 719)
            review_date = now - timedelta(hours=hours_ago)

            reviews.append(Review(
                id=f"{self.source}-{product_id}-{i}",
                product_id=product_id,
                rating=rating,
                text=random.choice(self.REVIEW_TEXTS[rating]),
                likes=likes,
                dislikes=dislikes,
                date=review_date.isoformat(),
            ))

        return reviews


# ====================================================================
# 4. NATS JetStream публикатор
# ====================================================================

class JetStreamPublisher:
    """Публикует отзывы в NATS JetStream (тема "reviews.raw")."""

    STREAM_NAME = "reviews"
    RAW_SUBJECT = "reviews.raw"

    def __init__(self, nats_url: str, logger: logging.Logger):
        self.nats_url = nats_url
        self.logger = logger
        self.nc = None
        self.js = None
        self.published = 0
        self.failed = 0

    async def connect(self):
        """Подключается к NATS и создаёт/обновляет стрим.

        Аналог Go: NewJetStreamPublisher → initStream.
        """
        from nats import connect
        from nats.js import JetStreamContext

        self.nc = await connect(self.nats_url)
        self.js = self.nc.jetstream()

        # Создаём или обновляем стрим (file storage, 24h TTL, дедупликация 2m)
        try:
            await self.js.add_stream(
                name=self.STREAM_NAME,
                subjects=[self.RAW_SUBJECT, "reviews.windowed"],
                storage="file",
                max_age=24 * 3600,
                max_msgs=-1,
                max_bytes=-1,
                discard="old",
                duplicates=120,
            )
        except Exception:
            # Стрим уже существует — обновляем конфиг
            try:
                await self.js.update_stream(
                    name=self.STREAM_NAME,
                    subjects=[self.RAW_SUBJECT, "reviews.windowed"],
                    storage="file",
                    max_age=24 * 3600,
                    discard="old",
                    duplicates=120,
                )
            except Exception as exc:
                self.logger.warning("NATS_STREAM_INIT_WARN",
                                    extra={"error": str(exc)})

        self.logger.info("NATS_JETSTREAM_READY",
                         extra={"stream": self.STREAM_NAME,
                                "subjects": [self.RAW_SUBJECT]})

    async def publish_raw(self, review: Review) -> bool:
        """Публикует отзыв в 'reviews.raw' с дедупликацией по review.id.

        Аналог Go: PublishRaw(ctx, review).
        """
        try:
            data = json.dumps(asdict(review)).encode()
            ack = await self.js.publish(
                self.RAW_SUBJECT,
                data,
                headers={"Nats-Msg-Id": review.id},
            )
            self.published += 1
            seq = getattr(ack, "seq", None)
            self.logger.debug("REVIEW_PUBLISHED",
                              extra={"id": review.id,
                                     "product": review.product_id,
                                     "seq": seq})
            return True
        except Exception as e:
            self.failed += 1
            self.logger.error("REVIEW_PUBLISH_FAILED",
                              extra={"id": review.id, "error": str(e)})
            return False

    async def drain(self):
        """Drain NATS-соединения (ждёт доставки всех сообщений).

        Аналог Go: nc.Drain().
        """
        if self.nc:
            try:
                await self.nc.drain()
                self.logger.info("NATS_DRAINED")
            except Exception as e:
                self.logger.warning("NATS_DRAIN_ERROR",
                                    extra={"error": str(e)})

    async def close(self):
        """Закрывает NATS-соединение."""
        if self.nc:
            await self.nc.close()
            self.logger.info("NATS_CLOSED",
                             extra={"published": self.published,
                                    "failed": self.failed})


# ====================================================================
# 5. Collector — оркестрация
# ====================================================================

class Collector:
    """Сборщик отзывов: etcd → marketplace → NATS JetStream.

    Аналог Go: пакет internal/collector (пока не реализован).
    """

    PRODUCT_LIST_PREFIX = "/collector/products/"
    ASSIGNMENT_PREFIX = "/collector/assignments/"
    LEASE_TTL = 10  # секунд, как в Go coordinator.LeaseTTL
    RECLAIM_INTERVAL = 15  # секунд между попытками захвата
    COLLECT_INTERVAL = 5   # секунд между итерациями сбора

    def __init__(
        self,
        etcd_endpoints: list[str],
        nats_url: str,
        worker_id: str,
        source: str,
        logger: logging.Logger,
        metrics: MetricsCollector | None = None,
    ):
        self.etcd = EtcdClient(etcd_endpoints, logger)
        self.market = MarketSimulator(source)
        self.nats = JetStreamPublisher(nats_url, logger)
        self.worker_id = worker_id
        self.source = source
        self.logger = logger
        self.metrics = metrics

        self._owned_products: dict[str, str] = {}  # product_id → status
        self._shutdown = asyncio.Event()
        self._claim_task: Optional[asyncio.Task] = None
        self._collect_task: Optional[asyncio.Task] = None

    async def start(self):
        """Запускает сборщик (подключение + фоновые циклы).

        Аналог Go: main() → collector.Run(ctx).
        """
        # 1. etcd: лизинг + keepalive
        await self.etcd.grant_lease(self.LEASE_TTL)
        await self.etcd.start_keepalive()

        # 2. NATS: подключение + инициализация стрима
        await self.nats.connect()

        # 3. Bootstrap продуктов (как Go: coordinator.BootstrapProducts)
        await self._bootstrap_products()

        # 4. Первичный захват шардов
        claimed = await self._claim_products()
        self.logger.info("INITIAL_CLAIM",
                         extra={"count": len(claimed),
                                "products": claimed})

        # 5. Фоновые задачи
        self._claim_task = asyncio.create_task(self._claim_loop())
        self._collect_task = asyncio.create_task(self._collect_loop())

        self.logger.info("COLLECTOR_STARTED",
                         extra={"worker_id": self.worker_id,
                                "source": self.source})

    # ── bootstrapping ────────────────────────────────────────────────

    async def _bootstrap_products(self):
        """Загружает список товаров в etcd (однократно, CAS).

        Аналог Go: coordinator.BootstrapProducts.
        """
        products = [
            "WB-001", "WB-002", "WB-003", "WB-004", "WB-005",
            "OZ-101", "OZ-102", "OZ-103",
        ]
        for pid in products:
            ok = await self.etcd.txn_create_if_not_exists(
                self.PRODUCT_LIST_PREFIX + pid,
                "",
            )
            if ok:
                self.logger.debug("PRODUCT_BOOTSTRAPPED",
                                  extra={"product": pid})

        self.logger.info("PRODUCTS_BOOTSTRAPPED",
                         extra={"worker": self.worker_id,
                                "count": len(products)})

    # ── захват шардов ───────────────────────────────────────────────

    async def _claim_products(self) -> list[str]:
        """Захватывает свободные product_id (CAS-транзакция).

        Аналог Go: coordinator.ClaimProducts.
        """
        # Список всех продуктов
        prod_kvs = await self.etcd.get_prefix(self.PRODUCT_LIST_PREFIX)
        all_products = []
        for kv in prod_kvs:
            key, _ = EtcdClient.decode_kv(kv)
            pid = key.removeprefix(self.PRODUCT_LIST_PREFIX)
            if pid:
                all_products.append(pid)

        # Текущие назначения
        assign_kvs = await self.etcd.get_prefix(self.ASSIGNMENT_PREFIX)
        taken: set[str] = set()
        for kv in assign_kvs:
            key, _ = EtcdClient.decode_kv(kv)
            pid = key.removeprefix(self.ASSIGNMENT_PREFIX)
            if pid:
                taken.add(pid)

        claimed: list[str] = []
        for pid in all_products:
            if pid in taken or pid in self._owned_products:
                continue

            ok = await self.etcd.txn_create_if_not_exists(
                self.ASSIGNMENT_PREFIX + pid,
                self.worker_id,
                lease=self.etcd._lease_id,
            )
            if ok:
                self._owned_products[pid] = "claiming"
                claimed.append(pid)
                self.logger.info("PRODUCT_CLAIMED",
                                 extra={"product": pid,
                                        "worker": self.worker_id})

        return claimed

    async def _claim_loop(self):
        """Фоновый цикл: периодически захватывает свободные шарды.

        Аналог Go: фоновые горутины ReclaimIntervalSeconds + Watch.
        """
        while not self._shutdown.is_set():
            try:
                await self._claim_products()
            except Exception as e:
                self.logger.error("CLAIM_LOOP_ERROR",
                                  extra={"error": str(e)})

            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=self.RECLAIM_INTERVAL)
            except asyncio.TimeoutError:
                pass

    # ── сбор отзывов ────────────────────────────────────────────────

    async def _collect_loop(self):
        """Фоновый цикл: собирает отзывы для захваченных шардов.

        Аналог Go: горутина col.Reviews() → агрегатор.
        """
        while not self._shutdown.is_set():
            products = list(self._owned_products.keys())

            for pid in products:
                if self._shutdown.is_set():
                    break
                if self._owned_products.get(pid) in ("completed",):
                    continue

                try:
                    self.logger.info("COLLECTING_REVIEWS",
                                     extra={"product": pid})
                    reviews = await self.market.fetch_reviews(pid, limit=5)

                    for review in reviews:
                        if self._shutdown.is_set():
                            break
                        ok = await self.nats.publish_raw(review)
                        if not ok:
                            self.logger.warning("COLLECT_SKIP_REVIEW",
                                                extra={"review_id": review.id})

                    if self.metrics:
                        self.metrics.record_reviews(len(reviews))

                    self._owned_products[pid] = "completed"
                    self.logger.info("PRODUCT_COLLECTED",
                                     extra={"product": pid,
                                            "reviews": len(reviews)})
                except Exception as e:
                    self.logger.error("COLLECT_ERROR",
                                      extra={"product": pid,
                                             "error": str(e)})

            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=self.COLLECT_INTERVAL)
            except asyncio.TimeoutError:
                pass

    # ── shutdown ────────────────────────────────────────────────────

    async def stop(self):
        """Graceful shutdown.

        Аналог Go: обработка сигнала + coord.ReleaseAll + cancel().
        """
        self.logger.info("COLLECTOR_STOPPING")
        self._shutdown.set()

        # Отмена фоновых задач
        for task in (self._claim_task, self._collect_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Освобождение шардов
        for pid in list(self._owned_products.keys()):
            try:
                await self.etcd.delete(self.ASSIGNMENT_PREFIX + pid)
                self.logger.info("PRODUCT_RELEASED",
                                 extra={"product": pid})
            except Exception as e:
                self.logger.error("RELEASE_FAILED",
                                  extra={"product": pid,
                                         "error": str(e)})

        # NATS
        await self.nats.drain()
        await self.nats.close()

        # etcd: отзыв лизинга + закрытие
        await self.etcd.revoke_lease()
        await self.etcd.close()

        self.logger.info("COLLECTOR_STOPPED")


# ====================================================================
# 6. Benchmark-режим (без etcd/NATS, прямая загрузка)
# ====================================================================

async def run_benchmark(
    num_products: int = 1000,
    reviews_per_product: int = 50,
    source: str = "wildberries",
    concurrency: int = 50,
    csv_path: str = "metrics_python.csv",
    logger: logging.Logger | None = None,
) -> dict:
    """Запускает сборщик в benchmark-режиме и возвращает сводку метрик.

    Args:
        num_products: количество product_id (по умолч. 1000)
        reviews_per_product: отзывов на товар (по умолч. 50)
        source: источник ("wildberries" / "ozon")
        concurrency: число параллельных корутин (semaphore)
        csv_path: путь к CSV-файлу метрик

    Returns:
        Словарь со сводкой: duration_s, total_reviews, avg_rps, peak_rps,
        avg_rss_mb, peak_rss_mb, avg_cpu_pct, peak_cpu_pct
    """
    logger = logger or logging.getLogger("benchmark")
    market = MarketSimulator(source)
    metrics = MetricsCollector(csv_path=csv_path, interval=5.0, logger=logger)

    # Генерация product_id
    product_ids = [f"BENCH-{i:04d}" for i in range(num_products)]
    total_expected = num_products * reviews_per_product
    logger.info("BENCH_START",
                extra={"products": num_products,
                       "reviews_per_product": reviews_per_product,
                       "total_expected": total_expected,
                       "concurrency": concurrency})

    sem = asyncio.Semaphore(concurrency)

    async def fetch_one(pid: str) -> int:
        """Собирает ровно reviews_per_product отзывов для одного товара."""
        async with sem:
            # Симулируем задержку HTTP-запроса (как в MarketSimulator)
            delay = 0.05 + random.random() * 0.15
            await asyncio.sleep(delay)

            now = datetime.now(timezone.utc).replace(tzinfo=None)
            for i in range(reviews_per_product):
                rating = 1 + random.randint(0, 4)
                likes = random.randint(0, 49)
                dislikes = random.randint(0, 19)
                hours_ago = random.randint(0, 719)
                review_date = now - timedelta(hours=hours_ago)

                _ = Review(
                    id=f"{source}-{pid}-{i}",
                    product_id=pid,
                    rating=rating,
                    text=random.choice(market.REVIEW_TEXTS[rating]),
                    likes=likes,
                    dislikes=dislikes,
                    date=review_date.isoformat(),
                )
            metrics.record_reviews(reviews_per_product)
            return reviews_per_product

    # Запуск метрик и сбор
    await metrics.start()
    start_wall = time.monotonic()

    tasks = [asyncio.create_task(fetch_one(pid)) for pid in product_ids]
    results = await asyncio.gather(*tasks)

    elapsed_wall = time.monotonic() - start_wall
    await metrics.stop()

    total = sum(results)
    summary = metrics.get_summary()
    summary["wall_clock_s"] = elapsed_wall

    logger.info("BENCH_DONE",
                extra={
                    "total_reviews": total,
                    "wall_clock_s": f"{elapsed_wall:.2f}",
                    "avg_rps": f"{summary.get('avg_rps', 0):.1f}",
                    "peak_rss_mb": f"{summary.get('peak_rss_mb', 0):.1f}",
                    "avg_cpu": f"{summary.get('avg_cpu_pct', 0):.1f}%",
                })

    return summary


# ====================================================================
# 7. Main + graceful shutdown
# ====================================================================

def setup_logging() -> logging.Logger:
    """Настраивает логирование в формате, похожем на Go zap."""
    logger = logging.getLogger("collector")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def default_worker_id() -> str:
    """Генерирует уникальный ID воркера (аналог Go: defaultWorkerID)."""
    hostname = os.uname().nodename
    return f"worker-{hostname}-{int(time.time() * 1000) % 10000}"


async def async_main():
    parser = argparse.ArgumentParser(
        description="Python-сборщик отзывов с маркетплейса (WB/Ozon)")
    parser.add_argument("--etcd", default="localhost:2379",
                        help="etcd endpoints (comma-separated)")
    parser.add_argument("--nats", default="nats://localhost:4222",
                        help="NATS server URL")
    parser.add_argument("--worker", default=default_worker_id(),
                        help="unique worker ID (default: auto)")
    parser.add_argument("--source", default="wildberries",
                        choices=["wildberries", "ozon"],
                        help="marketplace source")
    parser.add_argument("--metrics-csv", default="",
                        help="path to metrics CSV (empty = no metrics)")
    parser.add_argument("--metrics-interval", type=float, default=5.0,
                        help="metrics sampling interval (seconds)")

    # Benchmark mode
    parser.add_argument("--bench", action="store_true",
                        help="run in benchmark mode (no etcd/NATS)")
    parser.add_argument("--bench-products", type=int, default=1000,
                        help="number of products in benchmark")
    parser.add_argument("--bench-reviews", type=int, default=50,
                        help="reviews per product in benchmark")
    parser.add_argument("--bench-concurrency", type=int, default=50,
                        help="concurrent coroutines in benchmark")
    args = parser.parse_args()

    logger = setup_logging()

    # ── Benchmark mode ──────────────────────────────────────────────
    if args.bench:
        csv_path = args.metrics_csv or "metrics_python.csv"
        summary = await run_benchmark(
            num_products=args.bench_products,
            reviews_per_product=args.bench_reviews,
            source=args.source,
            concurrency=args.bench_concurrency,
            csv_path=csv_path,
            logger=logger,
        )
        print(f"\n{'='*60}")
        print("  PYTHON BENCHMARK RESULTS")
        print(f"{'='*60}")
        print(f"  Duration:          {summary.get('wall_clock_s', 0):>8.2f} s")
        print(f"  Total reviews:     {summary.get('total_reviews', 0):>8}")
        print(f"  Avg throughput:    {summary.get('avg_rps', 0):>8.1f} rev/s")
        print(f"  Peak throughput:   {summary.get('peak_rps', 0):>8.1f} rev/s")
        print(f"  Avg RSS:           {summary.get('avg_rss_mb', 0):>8.1f} MB")
        print(f"  Peak RSS:          {summary.get('peak_rss_mb', 0):>8.1f} MB")
        print(f"  Avg CPU:           {summary.get('avg_cpu_pct', 0):>8.1f} %")
        print(f"  Peak CPU:          {summary.get('peak_cpu_pct', 0):>8.1f} %")
        print(f"{'='*60}")
        print(f"  Metrics saved to:  {csv_path}")
        print(f"{'='*60}\n")

        # JSON для парсинга скриптом сравнения
        import json as _json
        total_expected = args.bench_products * args.bench_reviews
        print("SUMMARY_JSON:", end="")
        print(_json.dumps({
            "language": "python",
            "wall_clock_s": round(summary.get("wall_clock_s", 0), 2),
            "total_reviews": summary.get("total_reviews", 0),
            "products": args.bench_products,
            "expected_total": total_expected,
            "rps": round(summary.get("avg_rps", 0), 1),
            "peak_rps": round(summary.get("peak_rps", 0), 1),
            "avg_rss_mb": round(summary.get("avg_rss_mb", 0), 1),
            "peak_rss_mb": round(summary.get("peak_rss_mb", 0), 1),
            "avg_cpu_pct": round(summary.get("avg_cpu_pct", 0), 1),
            "peak_cpu_pct": round(summary.get("peak_cpu_pct", 0), 1),
        }))
        print()
        return

    # ── Normal mode ─────────────────────────────────────────────────
    etcd_endpoints = [ep.strip() for ep in args.etcd.split(",")]

    metrics = None
    if args.metrics_csv:
        metrics = MetricsCollector(
            csv_path=args.metrics_csv,
            interval=args.metrics_interval,
            logger=logger,
        )

    collector = Collector(
        etcd_endpoints=etcd_endpoints,
        nats_url=args.nats,
        worker_id=args.worker,
        source=args.source,
        logger=logger,
        metrics=metrics,
    )

    # Signal handling (асинхронный, неблокирующий).
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_signal():
        logger.info("SIGNAL_RECEIVED", extra={"signal": "SIGINT/SIGTERM"})
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    try:
        if metrics:
            await metrics.start()
        await collector.start()
        logger.info("COLLECTOR_READY",
                    extra={"worker": args.worker, "source": args.source})

        await shutdown_event.wait()

        await collector.stop()
        if metrics:
            await metrics.stop()
    except Exception as e:
        logger.error("FATAL_ERROR", extra={"error": str(e)})
        raise


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
