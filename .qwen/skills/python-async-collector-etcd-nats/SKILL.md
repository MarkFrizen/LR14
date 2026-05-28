---
name: python-async-collector-etcd-nats
description: Шаблон портирования Go-сервиса (etcd-координация + NATS JetStream) на Python asyncio с имитацией внешних API
source: auto-skill
extracted_at: '2026-05-28T12:57:43.840Z'
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

## Зависимости (requirements.txt)

```
aiohttp>=3.9.0      # HTTP-клиент (etcd API + внешние API)
nats-py>=2.6.0      # NATS JetStream
```

Никаких дополнительных etcd-клиентских библиотек не требуется — etcd HTTP v3 API покрывает все необходимые операции.

## Типичные ошибки

- **Base64 для etcd**: все ключи и значения в etcd HTTP API должны быть base64-encoded. Забыл — получишь "invalid key: not base64".
- **range_end для префикса**: без него etcd вернёт только точное совпадение, а не префикс.
- **CAS без лизинга**: при создании ключа назначения нужно передавать `lease`, иначе при падении воркера шард не освободится.
- **Drain vs Close**: NATS требует drain (дождаться подтверждений), а потом close. Обратный порядок — потеря сообщений.
- **add_signal_handler не везде**: на Windows может не работать. Нужен fallback через стандартный `signal.signal`.
