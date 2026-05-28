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
import json
import logging
import os
import random
import signal
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp


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
    ):
        self.etcd = EtcdClient(etcd_endpoints, logger)
        self.market = MarketSimulator(source)
        self.nats = JetStreamPublisher(nats_url, logger)
        self.worker_id = worker_id
        self.source = source
        self.logger = logger

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
# 6. Main + graceful shutdown
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
    args = parser.parse_args()

    logger = setup_logging()
    etcd_endpoints = [ep.strip() for ep in args.etcd.split(",")]

    collector = Collector(
        etcd_endpoints=etcd_endpoints,
        nats_url=args.nats,
        worker_id=args.worker,
        source=args.source,
        logger=logger,
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
            # Windows или другая платформа без add_signal_handler
            pass

    try:
        await collector.start()
        logger.info("COLLECTOR_READY",
                    extra={"worker": args.worker, "source": args.source})

        await shutdown_event.wait()

        await collector.stop()
    except Exception as e:
        logger.error("FATAL_ERROR", extra={"error": str(e)})
        raise


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
