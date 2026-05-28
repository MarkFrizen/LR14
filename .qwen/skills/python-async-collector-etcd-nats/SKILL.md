---
name: python-async-collector-etcd-nats
description: Портирование Go-сервиса на Python asyncio: etcd (HTTP API), NATS JetStream/Kafka, psutil-метрики, Go vs Python бенчмаркинг, matplotlib-чарты, sliding window агрегация (deque), Streamlit дашборд с Kafka polling
source: auto-skill
extracted_at: '2026-05-28T16:40:00.000Z'
---

# Python async collector: портирование Go-сервиса с etcd + NATS

## Когда применять

Когда нужно реализовать Python-аналог Go-сервиса, который:
- Использует **etcd** для координации (лизинг, CAS-транзакции, префиксные watch)
- Публикует данные в **NATS JetStream** (exactly-once, дедупликация)
- Содержит **симуляцию внешнего API** (marketplace, HTTP-клиент)
- Требует **graceful shutdown** по сигналам SIGINT/SIGTERM
- Логирует в structured-формате (аналог Go zap)

## Процедура

### 1. Анализ Go-моделей (struct → dataclass)

Go-структуру превращаем в `@dataclass`:

```go
type Review struct {
    ID        string    `json:"id"`
    ProductID string    `json:"product_id"`
    Rating    int       `json:"rating"`
    Text      string    `json:"text"`
    Likes     int       `json:"likes"`
    Dislikes  int       `json:"dislikes"`
    Date      time.Time `json:"date"`
}
```

→

```python
@dataclass
class Review:
    id: str
    product_id: str
    rating: int
    text: str
    likes: int
    dislikes: int
    date: str  # ISO 8601
```

Поля — в snake_case. Дата — в ISO-строку (Python-тип `datetime` при необходимости сериализуется отдельно).

### 2. etcd-клиент через HTTP v3 API (без gRPC-зависимостей)

etcd v3 предоставляет **HTTP gateway** на том же порту (2379). Все операции — `POST /v3/{endpoint}` с JSON-телом, где ключи/значения — **base64**. Это чистая альтернатива etcd-клиентским библиотекам.

| Операция | Go-аналог | etcd HTTP endpoint |
|---|---|---|
| Создать лизинг | `cli.Grant(ctx, ttl)` | `POST /v3/lease/grant` |
| Продлить лизинг | `cli.KeepAlive(ctx, leaseID)` | `POST /v3/lease/keepalive` |
| Отозвать лизинг | `cli.Revoke(ctx, leaseID)` | `POST /v3/lease/revoke` |
| Записать ключ | `cli.Put(ctx, key, val)` | `POST /v3/kv/put` |
| Прочитать ключ | `cli.Get(ctx, key)` | `POST /v3/kv/range` |
| Префиксный запрос | `cli.Get(ctx, prefix, WithPrefix())` | `POST /v3/kv/range` с `range_end` |
| CAS-транзакция | `Txn().If(CreateRevision==0).Then(OpPut(...))` | `POST /v3/kv/txn` с `compare` |
| Удалить ключ | `cli.Delete(ctx, key)` | `POST /v3/kv/deleterange` |

**CAS-транзакция** (CreateIfNotExists) — ключевой паттерн для распределённых шардов:

```python
async def txn_create_if_not_exists(self, key: str, value: str,
                                   lease: Optional[int] = None) -> bool:
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
```

**Префиксный range_end** — вычисляется инкрементом последнего байта префикса:

```python
def _prefix_end(prefix: str) -> str:
    if not prefix:
        return "\x00"
    b = bytearray(prefix.encode("utf-8"))
    for i in range(len(b) - 1, -1, -1):
        if b[i] < 0xFF:
            b[i] += 1
            return bytes(b[:i + 1]).decode("utf-8", errors="replace")
    return "\x00"
```

### 3. Лизинги и keepalive в asyncio

- `etcd.grant_lease(ttl)` — создаёт лизинг, сохраняет `_lease_id`
- `asyncio.create_task(self._keepalive_loop())` — фоновое продление каждые 5с
- При shutdown: `lease/revoke` + отмена таски

### 4. Симуляция внешнего API (aiohttp)

```python
class MarketSimulator:
    async def fetch_reviews(self, product_id: str, limit: int = 5) -> list[Review]:
        delay = 0.05 + random.random() * 0.15  # 50-200ms
        await asyncio.sleep(delay)
        # ... генерация отзывов
```

В реальном проекте здесь был бы aiohttp-запрос к внешнему API.

### 5. NATS JetStream публикация

Используется `nats-py`:

```python
self.nc = await connect(nats_url)
self.js = self.nc.jetstream()

# Создание стрима (file storage, 24h TTL, дедупликация 2m)
await self.js.add_stream(
    name="reviews",
    subjects=["reviews.raw", "reviews.windowed"],
    storage="file",
    max_age=24 * 3600,
    duplicates=120,
    # ...
)

# Публикация с дедупликацией (Nats-Msg-Id header = аналог jetstream.WithMsgID)
await self.js.publish(
    "reviews.raw",
    json.dumps(asdict(review)).encode(),
    headers={"Nats-Msg-Id": review.id},
)
```

### 6. Graceful shutdown

```python
loop = asyncio.get_running_loop()
shutdown_event = asyncio.Event()

def _on_signal():
    shutdown_event.set()

for sig in (signal.SIGINT, signal.SIGTERM):
    loop.add_signal_handler(sig, _on_signal)

# ... start collector ...
await shutdown_event.wait()
# ... stop collector (освободить шарды, drain NATS, отозвать лизинг) ...
```

Порядок остановки (соответствует Go-версии):
1. Установить `_shutdown` event → остановить фоновые циклы
2. Отменить фоновые задачи (`cancel()` + `await`)
3. Удалить ключи назначений в etcd (освободить шарды)
4. Drain NATS-соединения (дождаться доставки pending-сообщений)
5. Закрыть NATS-соединение
6. Отозвать etcd-лизинг
7. Закрыть etcd HTTP-сессию

### 7. Логирование (стиль Go zap)

```python
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
```

Ключи событий — UPPER_SNAKE_CASE (как в Go: `PRODUCT_CLAIMED`, `COLLECTOR_STARTED`), экстра-поля — через `extra={}`.

### 8. Сбор метрик производительности (psutil + CSV)

Добавляется `MetricsCollector`, который работает как фоновый asyncio-цикл и каждые N секунд снимает снимок:

```python
import psutil

class MetricsCollector:
    def __init__(self, csv_path: str, interval: float = 5.0, logger=None):
        self.csv_path = Path(csv_path)
        self.interval = interval
        self.process = psutil.Process()
        self.total_reviews = 0
        self._snapshots = []
        # CSV header: timestamp, elapsed_s, total_reviews, reviews_per_sec, rss_mb, cpu_percent

    async def start(self):
        self._start_time = time.time()
        self._task = asyncio.create_task(self._run_loop())

    async def _run_loop(self):
        while True:
            await asyncio.sleep(self.interval)
            snapshot = self._take_snapshot()
            self._snapshots.append(snapshot)
            self._append_csv(snapshot)

    def record_reviews(self, count: int = 1):
        """Вызывается из collect_loop при получении отзывов."""
        self.total_reviews += count

    def _take_snapshot(self):
        now = time.time()
        delta_reviews = self.total_reviews - self._prev_reviews
        rps = delta_reviews / (now - self._prev_time)
        rss_mb = self.process.memory_info().rss / 1024 / 1024
        cpu_pct = self.process.cpu_percent(interval=0)
        # ...
        return MetricsSnapshot(...)

    def get_summary(self) -> dict:
        """Усредняет все снимки → avg_rps, peak_rps, avg_rss_mb, peak_rss_mb, ..."""
```

**Интеграция в Collector.** `MetricsCollector` передаётся в конструктор и вызывается после каждого батча:

```python
class Collector:
    def __init__(self, ..., metrics: MetricsCollector | None = None):
        self.metrics = metrics

    async def _collect_loop(self):
        reviews = await self.market.fetch_reviews(pid, limit)
        if self.metrics:
            self.metrics.record_reviews(len(reviews))
```

**Важно:** `psutil.cpu_percent(interval=0)` возвращает мгновенное значение, которое для коротких (<5с) процессов часто равно 0. Для точного CPU используйте `interval=0.1` или запускайте метрики до начала работы и останавливайте после.

### 9. Benchmark-режим (без etcd/NATS)

Для сравнения производительности Go vs Python создаётся параллельный benchmark-режим:

```python
async def run_benchmark(
    num_products=1000, reviews_per_product=50, concurrency=50,
    csv_path="metrics_python.csv"
) -> dict:
    metrics = MetricsCollector(csv_path=csv_path, interval=5.0)
    sem = asyncio.Semaphore(concurrency)

    async def fetch_one(pid: str) -> int:
        async with sem:
            delay = 0.05 + random.random() * 0.15  # имитация HTTP
            await asyncio.sleep(delay)
            # генерация ровно reviews_per_product отзывов
            for i in range(reviews_per_product):
                _ = Review(...)
            metrics.record_reviews(reviews_per_product)
            return reviews_per_product

    await metrics.start()
    tasks = [asyncio.create_task(fetch_one(pid)) for pid in product_ids]
    await asyncio.gather(*tasks)
    await metrics.stop()

    return metrics.get_summary()
```

**Принцип:** прямой сбор отзывов, без etcd и NATS — измеряется чистая скорость генерации + pipeline. Для Go делается идентичная программа, использующая тот же `marketplace.Simulator` и `runtime.ReadMemStats` для замера памяти.

### 10. Сравнение Go vs Python (методология)

| Аспект | Go | Python |
|---|---|---|
| Параллелизм | goroutines + buffered channel semaphore | asyncio + Semaphore |
| Генерация отзывов | `marketplace.Simulator.FetchReviews` | `MarketSimulator.fetch_reviews` |
| Задержка | `time.Sleep(50-200ms)` | `await asyncio.sleep(0.05-0.15)` |
| Замер памяти | `runtime.ReadMemStats().Alloc` | `psutil.Process().memory_info().rss` |
| Вывод результатов | JSON summary через `SUMMARY_JSON:` | JSON summary через `SUMMARY_JSON:` |

**Ключевые метрики для сравнения:**
- **Wall clock time** (сек) — общая длительность
- **Throughput** (reviews/sec) — общее количество / wall time
- **Peak memory** (MB) — `runtime.Alloc` (Go heap) vs `psutil RSS` (Python полный процесс)
- **CPU usage** — только для длительных (>10s) прогонов

**Результаты типичного сравнения (1000 продуктов × 50 отзывов = 50 000, concurrency=50):**
``` 
Go:        2.61s, ~19100 rev/s, alloc=3.4MB
Python:    2.63s, ~19000 rev/s, RSS=38.2MB
```

Разница <1% по скорости объясняется тем, что доминирующий фактор — симулированная сетевая задержка (50-200ms/product), которая нивелирует различия рантаймов. Python потребляет больше памяти (~5-10×), т.к. RSS включает весь интерпретатор.

## Зависимости (requirements.txt)

```
aiohttp>=3.9.0      # HTTP-клиент (etcd API + внешние API)
nats-py>=2.6.0      # NATS JetStream
psutil>=5.9.0       # метрики (RSS, CPU) — опционально, только для бенчмарков
```

Никаких дополнительных etcd-клиентских библиотек не требуется — etcd HTTP v3 API покрывает все необходимые операции.

## Типичные ошибки

- **Base64 для etcd**: все ключи и значения в etcd HTTP API должны быть base64-encoded. Забыл — получишь "invalid key: not base64".
- **range_end для префикса**: без него etcd вернёт только точное совпадение, а не префикс.
- **CAS без лизинга**: при создании ключа назначения нужно передавать `lease`, иначе при падении воркера шард не освободится.
- **Drain vs Close**: NATS требует drain (дождаться подтверждений), а потом close. Обратный порядок — потеря сообщений.
- **add_signal_handler не везде**: на Windows может не работать. Нужен fallback через стандартный `signal.signal`.

---

## 11. Замена NATS на Kafka (паттерн миграции)

### Мотивация

NATS JetStream — лёгкий in-memory брокер, Kafka — долговременное лог-хранилище с партициями, ретеншеном по диску и consumer groups. Замена актуальна при требованиях к долгому хранению событий (сутки+) или стандартизации на Kafka в инфраструктуре.

### Что меняется

| Аспект | NATS JetStream | Kafka |
|---|---|---|
| Топик | subject (`reviews.raw`) | topic (`reviews.raw`) |
| Продусер | `js.publish(subject, data, headers={Nats-Msg-Id})` | `writer.WriteMessages(ctx, msg{Key, Value})` |
| Партицирование | автоматическое (stream) | `hash(key) % N` (kafka.Hash) |
| Дедупликация | `WithMsgID` + duplicates window (2 min) | идемпотентность producer (config) |
| Консьюмер | push/pull subscribe + explicit ack | poll + manual commit |
| Гарантия доставки | exactly-once (dedup) | at-least-once (manual commit) |
| Зависимости (Go) | `nats.go` + `nats.go/jetstream` | `segmentio/kafka-go` |
| Зависимости (Python) | `nats-py` | `aiokafka` |

### Go: Kafka producer (segmentio/kafka-go)

```go
import (
    "github.com/segmentio/kafka-go"
    "github.com/segmentio/kafka-go/compress"
)

const TopicRawReviews = "reviews.raw"

type KafkaPublisher struct {
    writer *kafka.Writer
}

func NewKafkaPublisher(brokers []string) *KafkaPublisher {
    w := &kafka.Writer{
        Addr:         kafka.TCP(brokers...),
        Topic:        TopicRawReviews,
        Balancer:     &kafka.Hash{},     // key → hash → partition
        Compression:  compress.Snappy,
        WriteTimeout: 30 * time.Second,
        RequiredAcks: kafka.RequireOne,  // ждём от лидера
        Async:        false,             // синхронно
    }
    return &KafkaPublisher{writer: w}
}

func (kp *KafkaPublisher) PublishRaw(ctx context.Context, review models.Review) error {
    data, _ := json.Marshal(review)
    msg := kafka.Message{
        Key:   []byte(review.ProductID),           // → partition по товару
        Value: data,                                 // JSON
    }
    return kp.writer.WriteMessages(ctx, msg)
}

func (kp *KafkaPublisher) PublishRawBatch(ctx context.Context, reviews []models.Review) error {
    msgs := make([]kafka.Message, len(reviews))
    for i, r := range reviews {
        data, _ := json.Marshal(r)
        msgs[i] = kafka.Message{Key: []byte(r.ProductID), Value: data}
    }
    return kp.writer.WriteMessages(ctx, msgs...)
}

func (kp *KafkaPublisher) Close() error {
    return kp.writer.Close()
}
```

**Ключевые моменты:**
- `kafka.Hash` balancer = `hash(product_id) % num_partitions` — все отзывы одного товара в одной партиции (гарантия порядка)
- `RequiredAcks = RequireOne` — лидер подтвердил запись в свой лог (быстрее чем `All`)
- `WriteMessages` с одним или батчем сообщений. batch сообщения могут идти в разные партиции — Kafka writer сам разбивает
- Pull-based: `partition_consumer` не нужен, `kafka.Writer` сам батчит и шардирует

**Интеграция в Go pipeline (замена NATS):**

Было (NATS):
```go
ncConn, jsClient, _ := natspub.NewNATSClient(ctx, natsURL, logger)
publisher, _ := natspub.NewJetStreamPublisher(ctx, ncConn, jsClient, logger)

for review := range col.Reviews() {
    publisher.PublishWindowAgg(ctx, agg)  // только windowed
}
```

Стало (Kafka):
```go
kafkaPub := kafkapub.NewKafkaPublisher(brokers, logger)

for review := range col.Reviews() {
    kafkaPub.PublishRaw(ctx, review)      // сырые отзывы в reviews.raw
    tw.Input() <- review                   // агрегатор остаётся
}
```

### Python: Kafka consumer (aiokafka)

```python
from aiokafka import AIOKafkaConsumer, TopicPartition

class KafkaReviewConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,        # ручной commit
            auto_offset_reset="earliest",    # start from beginning if no offset
            max_poll_records=500,
        )

    async def start(self):
        await self.consumer.start()
        partitions = self.consumer.assignment()
        print(f"Assigned: {[str(p) for p in partitions]}")

    async def run(self):
        self._last_offsets: dict[TopicPartition, int] = {}

        while not self._shutdown.is_set():
            msgs = await self.consumer.getmany(timeout_ms=2000, max_records=500)

            for tp, records in msgs.items():
                for msg in records:
                    review = json.loads(msg.value)
                    self.process_review(review)
                    self._last_offsets[tp] = msg.offset + 1   # next offset = commit marker

            # Ручной commit — at-least-once семантика
            if self._last_offsets:
                await self.consumer.commit(self._last_offsets)

    async def stop(self):
        if self._last_offsets:
            await self.consumer.commit(self._last_offsets)  # перед отключением
        await self.consumer.stop()
```

**Обработка партиций:**
- `consumer.assignment()` — список `TopicPartition`, назначенных этому consumer
- `getmany()` — возвращает `dict[TopicPartition, list[ConsumerRecord]]`
- Ручной commit: `consumer.commit({tp: offset})` — указываете offset **следующего** сообщения для каждой партиции
- Consumer group: если несколько instance с одним `group_id`, Kafka делит партиции между ними (ребалансировка при добавлении/удалении)

**Обработка отзыва партиций (при ребалансе):**
```python
# Вариант 1: обработчики через consumer.subscribe() с on_revoke=
consumer.subscribe(
    [topic],
    listener=ConsumerRebalanceListener(
        on_partitions_revoked=lambda revoked: await consumer.commit(offsets)
    )
)
```

**Вариант 2** (проще): commit перед каждым stop — гарантирует, что при graceful shutdown offsets сохранены.

### Зависимости для Kafka

Go `go.mod`:
```
github.com/segmentio/kafka-go v0.4.47
```

Python `requirements.txt`:
```
aiokafka>=0.14.0
```

---

## 12. Визуализация сравнения Go vs Python (matplotlib)

Скрипт запускает оба бенчмарка и строит три bar-диаграммы рядом.

### Данные для сравнения

Из Go бенчмарка (runtime.ReadMemStats + syscall.Getrusage):
- `wall_clock_s` (сек)
- `rps` (reviews/sec)
- `max_rss_mb` (getrusage.RUSAGE_SELF.Maxrss в MB)
- `cpu_pct` (CPU time / wall time)

Из Python бенчмарка (MetricsCollector с psutil):
- `wall_clock_s`
- `rps` / `peak_rps`
- `avg_rss_mb` / `peak_rss_mb` (psutil.Process.memory_info().rss)
- `avg_cpu_pct` / `peak_cpu_pct` (psutil.Process.cpu_percent(interval=0))

### Извлечение JSON-сводки

Оба бенчмарка печатают строку `SUMMARY_JSON:{"language":"go",...}` — парсится через regex:

```python
m = re.search(r"SUMMARY_JSON:\s*(\{.*\})", output, re.DOTALL)
data = json.loads(m.group(1))
```

### Код визуализации

```python
import matplotlib.pyplot as plt
import numpy as np

def plot_comparison(py: dict, go: dict, output_path: str):
    rps    = [go["rps"], py["rps"]]
    mem    = [go["max_rss_mb"], py["peak_rss_mb"]]
    cpu    = [go["cpu_pct"], py["avg_cpu_pct"]]

    labels = ["Go", "Python"]
    colors = ["#1f77b4", "#ff7f0e"]
    x = np.arange(2)

    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.suptitle("Сравнение Go vs Python", fontsize=13, fontweight="bold")

    # 1. Throughput (RPS)
    ax = axes[0]
    ax.bar(x, rps, 0.5, color=colors)
    ax.set_ylabel("reviews / sec")
    ax.set_title("Пропускная способность (RPS)")
    ax.set_xticks(x); ax.set_xticklabels(labels)
    for bar, val in zip(bars, rps):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f"{val:.0f}", ha="center", va="bottom", fontweight="bold")

    # 2. Memory (RSS)
    ax = axes[1]
    ax.bar(x, mem, 0.5, color=colors)
    ax.set_ylabel("MB")
    ax.set_title("Потребление памяти (Max RSS)")
    ax.set_xticks(x); ax.set_xticklabels(labels)

    # 3. CPU
    ax = axes[2]
    ax.bar(x, cpu, 0.5, color=colors)
    ax.set_ylabel("%")
    ax.set_title("Загрузка CPU")
    ax.set_xticks(x); ax.set_xticklabels(labels)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
```

### Текстовый вывод с коэффициентами

```python
# Go быстрее Python в X раз по пропускной способности
if go_rps >= py_rps:
    rps_part = f"Go быстрее Python в {go_rps/py_rps:.2f} раза"
else:
    rps_part = f"Python быстрее Go в {py_rps/go_rps:.2f} раза"

# Go потребляет в Y раз меньше/больше памяти
if go_mem <= py_mem:
    mem_part = f"Go потребляет в {py_mem/go_mem:.1f} раза меньше памяти (RSS)"
else:
    mem_part = f"Go потребляет в {go_mem/py_mem:.1f} раза больше памяти (RSS)"

# Go загружает CPU в Z раз меньше/больше
if go_cpu <= py_cpu:
    cpu_part = f"Go загружает CPU в {py_cpu/go_cpu:.1f} раза меньше"
else:
    cpu_part = f"Go загружает CPU в {go_cpu/py_cpu:.1f} раза больше"
```

### Зависимости для визуализации

```
matplotlib>=3.7.0
```

---

## 13. Скользящее окно (Sliding Window) на deque для per-product агрегации

### Когда применять

Нужно вычислять скользящие агрегаты по каждому product_id в реальном времени: средний рейтинг, количество отзывов, доля негативных. Типичное окно — 5 минут, сдвиг — 1 минута.

### Структура данных

Используется `collections.deque` как кольцевой буфер. Каждый элемент — кортеж `(timestamp, product_id, rating, review_date_str)`.

```python
from collections import deque
from datetime import datetime, timezone
from typing import Optional

class SlidingWindowAggregator:
    def __init__(self, window_size: float = 300.0, slide_interval: float = 60.0):
        self.window_size = window_size
        self.slide_interval = slide_interval
        self._buffer: deque[tuple[float, str, int, str]] = deque()
        self._total_added = 0

    def add(self, product_id: str, rating: int, review_date: Optional[str] = None):
        now = time.time()
        self._buffer.append((now, product_id, rating,
                             review_date or datetime.now(timezone.utc).isoformat()))
        self._total_added += 1

    def prune(self):
        """Удаляет записи старше window_size секунд."""
        cutoff = time.time() - self.window_size
        while self._buffer and self._buffer[0][0] < cutoff:
            self._buffer.popleft()
```

### Алгоритм сдвига окна (выполняется каждые slide_interval секунд)

```
prune()    → удалить из deque записи с timestamp < now - window_size
compute()  → сгруппировать по product_id, посчитать:
               avg_rating = sum(ratings) / n
               negative_share = count(rating < 3) / n
               max_review_date = max(review_dates в группе)
               latency_sec = max(0, computed_at - max_review_date)
display()  → вывод в консоль с форматированной таблицей
publish()  → отправка в Kafka (если есть producer)
```

### Per-product агрегация

```python
def compute(self) -> list[dict]:
    now = time.time()
    window_end_dt = datetime.fromtimestamp(now, tz=timezone.utc)

    # Группируем по product_id
    per_product: dict[str, tuple[list[int], list[str]]] = {}
    for _ts, pid, rating, rdate in self._buffer:
        if pid not in per_product:
            per_product[pid] = ([], [])
        per_product[pid][0].append(rating)
        if rdate:
            per_product[pid][1].append(rdate)

    results = []
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
            "window_start":   window_end_dt.isoformat(),
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
```

### Фоновый цикл (asyncio задача)

```python
async def _window_loop(self):
    """Периодический сдвиг окна: prune → compute → display → publish."""
    # Пропускаем первый slide_interval, чтобы накопить данные
    await asyncio.sleep(self.window.slide_interval)

    while not self._shutdown.is_set():
        before = len(self.window._buffer)
        self.window.prune()
        after = len(self.window._buffer)

        results = self.window.compute()
        self.window.display(results)

        if before != after:
            print(f"Pruned {before - after} entries")

        await self.window.publish(results)     # → Kafka topic

        await asyncio.sleep(self.window.slide_interval)
```

### Публикация агрегатов в Kafka

```python
from aiokafka import AIOKafkaProducer

producer = AIOKafkaProducer(
    bootstrap_servers="localhost:9092",
    acks=1,
    compression_type="snappy",
)

for agg in results:
    await producer.send(
        "reviews.aggregated",
        key=agg["product_id"].encode(),   # → partition по товару
        value=json.dumps(agg).encode(),
    )
await producer.flush()
```

### Вывод в консоль

```
─── [14:23:05] Sliding Window Aggregates ───
  Window: 5 min, slide: 1 min, buffer: 847 reviews
  Product      Count  AvgRat  NegShare
  ──────────── ────── ─────── ─────────
  WB-001         42    4.15     4.8%
  WB-002         38    3.72    10.5%
  OZ-101         29    2.93    24.1%
```

---

## 14. Streamlit дашборд с Kafka polling (background thread + kafka-python)

### Когда применять

Нужно визуализировать данные из Kafka в реальном времени в Streamlit. Streamlit работает в синхронном однопоточном режиме — Kafka-консьюмер нужно запускать в **фоновом потоке** с передачей данных через `queue.Queue`.

### Архитектура

```
[Kafka topic: reviews.aggregated]
       │
       ▼
[Background thread: KafkaConsumer → queue.Queue]
       │  polling каждые 2 сек
       ▼
[Streamlit main thread: drain queue → session_state → render]
       │  rerun каждые 5 сек
       ▼
[Plotly charts + KPI + table]
```

### Фоновый Kafka listener

```python
import json
import queue
import threading
import time
from kafka import KafkaConsumer

_msg_queue: queue.Queue = queue.Queue(maxsize=5000)

def kafka_listener(bootstrap_servers: str, stop_event: threading.Event):
    """Фоновый поток: читает Kafka, кладёт в queue.Queue."""
    consumer = KafkaConsumer(
        "reviews.aggregated",
        bootstrap_servers=bootstrap_servers,
        group_id="streamlit-dashboard",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode() if k else None,
        value_deserializer=lambda v: json.loads(v.decode()) if v else None,
        max_poll_records=500,
    )

    while not stop_event.is_set():
        msgs = consumer.poll(timeout_ms=2000)
        for _tp, records in msgs.items():
            for msg in records:
                if msg.value is None:
                    continue
                try:
                    _msg_queue.put_nowait(msg.value)
                except queue.Full:
                    # очередь полна — вытесняем старые
                    try:
                        while _msg_queue.qsize() >= 5000:
                            _msg_queue.get_nowait()
                        _msg_queue.put_nowait(msg.value)
                    except queue.Empty:
                        pass
    consumer.close()
```

**Ключевые моменты:**
- `enable_auto_commit=True` — упрощает commit (не нужно ручное управление для дашборда)
- `group_id` позволяет масштабировать дашборды (каждый получает свою долю партиций)
- `queue.Queue(maxsize=5000)` — buffer между потоком и Streamlit, предотвращает переполнение памяти

### Интеграция с Streamlit

```python
import streamlit as st

def init_session_state():
    if "aggregates" not in st.session_state:
        st.session_state.aggregates = []
    if "listener_started" not in st.session_state:
        st.session_state.listener_started = False

def start_listener(bootstrap_servers: str):
    if st.session_state.listener_started:
        return
    stop_event = threading.Event()
    thread = threading.Thread(
        target=kafka_listener,
        args=(bootstrap_servers, stop_event),
        daemon=True,
    )
    thread.start()
    st.session_state.listener_started = True
    st.session_state._stop_event = stop_event

def drain_queue():
    """Вычитывает все сообщения из очереди в session_state."""
    while not _msg_queue.empty():
        try:
            agg = _msg_queue.get_nowait()
            st.session_state.aggregates.append(agg)
        except queue.Empty:
            break
    # Ограничение памяти
    MAX_AGGREGATES = 2000
    if len(st.session_state.aggregates) > MAX_AGGREGATES:
        st.session_state.aggregates = \
            st.session_state.aggregates[-MAX_AGGREGATES:]
```

### Рендеринг графиков (Plotly)

```python
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def get_df() -> pd.DataFrame:
    df = pd.DataFrame(st.session_state.aggregates)
    for col in ("window_start", "window_end", "computed_at", "max_review_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def plot_avg_rating(df: pd.DataFrame):
    """Линейный график среднего рейтинга по времени."""
    fig = go.Figure()
    for pid in df["product_id"].unique():
        pdf = df[df["product_id"] == pid].sort_values("computed_at")
        fig.add_trace(go.Scatter(x=pdf["computed_at"], y=pdf["avg_rating"],
                                 mode="lines+markers", name=pid))
    fig.update_layout(
        title="Средний рейтинг (скользящее окно 5 мин)",
        xaxis_title="Время", yaxis_title="Рейтинг",
        yaxis=dict(range=[1, 5]),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

def plot_latency(df: pd.DataFrame):
    """График задержки end-to-end."""
    latency_df = (df.groupby("computed_at")["latency_sec"]
                  .mean().reset_index().sort_values("computed_at"))
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=latency_df["computed_at"],
                             y=latency_df["latency_sec"],
                             mode="lines+markers", name="Latency",
                             line=dict(color="red"), fill="tozeroy"))
    fig.update_layout(title="Задержка end-to-end", yaxis_title="сек")
    st.plotly_chart(fig, use_container_width=True)
```

### Auto-refresh

```python
REFRESH_SECONDS = 5

if st.sidebar.checkbox("Автообновление", value=True):
    time.sleep(REFRESH_SECONDS)
    st.rerun()
```

### Запуск дашборда

```bash
.venv/bin/pip install streamlit pandas plotly kafka-python
.venv/bin/streamlit run dashboard_kafka.py -- --bootstrap-servers localhost:9092
```

### Требования к данным в Kafka

Ожидается JSON с полями:
```json
{
  "window_start": "2026-05-28T12:00:00+00:00",
  "window_end":   "2026-05-28T12:05:00+00:00",
  "computed_at":  "2026-05-28T12:05:03+00:00",
  "product_id":   "WB-001",
  "review_count": 42,
  "avg_rating":   4.15,
  "negative_share": 0.05,
  "max_review_date": "2026-05-28T12:04:00+00:00",
  "latency_sec":  63.0
}
```

### Типичные ошибки

- **Streamlit + asyncio.** Streamlit не поддерживает `asyncio.run()` внутри скрипта — используйте синхронный `KafkaConsumer` в потоке, а не `AIOKafkaConsumer`.
- **Переполнение памяти.** Без ограничения `MAX_AGGREGATES` список будет расти бесконечно. Устанавливайте лимит (2000-5000 записей).
- **Очередь переполнена.** Если Kafka шлёт быстрее, чем Streamlit рендерит, очередь растёт. Используйте `maxsize` и вытеснение старых записей.
- **group_id.** Без group_id каждый запуск дашборда будет читать все сообщения с начала. Используйте `group_id` с `auto_offset_reset="latest"`.
- **dataframe колонки.** Парсинг дат через `pd.to_datetime(..., errors="coerce")` защищает от невалидных строк.
