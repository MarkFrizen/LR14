---
name: python-async-collector-etcd-nats
description: Шаблон портирования Go-сервиса на Python asyncio: etcd-координация, NATS JetStream/Kafka, сбор метрик (psutil + CSV), бенчмаркинг Go vs Python и matplotlib-визуализация
source: auto-skill
extracted_at: '2026-05-28T16:35:00.000Z'
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
