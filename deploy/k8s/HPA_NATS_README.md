# HPA на основе длины очереди NATS JetStream

## Архитектура

```
┌──────────────┐     ┌────────────────┐     ┌──────────────────┐
│ Go Collector │ ◄── │   NATS JetStream  │     │  Kubernetes HPA  │
│  (Deployment) │ ──► │  (Stream: reviews)│────►│ custom.metrics    │
│              │     │  Consumer:       │     │ nats_queue_pending│
│  /metrics    │     │  collector-worker│     │ target: <500      │
└──────┬───────┘     └────────────────┘     └──────────────────┘
       │
       ▼
┌──────────────┐
│  Prometheus   │
│  (ServiceMonitor) │
└──────┬───────┘
       │
       ▼
┌──────────────────────┐
│  Prometheus Adapter  │
│  (custom.metrics API)│
└──────────────────────┘
```

## Компоненты

| Компонент | Описание |
|---|---|
| **Go Collector** | Экспортирует `nats_jetstream_consumer_pending_messages` |
| **ServiceMonitor** | Prometheus Operator — настраивает сбор метрик |
| **Prometheus Adapter** | Преобразует Prometheus-метрики в custom.metrics.k8s.io |
| **HPA** | Использует кастомную метрику + CPU + RAM |

## Метрики

| Имя | Тип | Описание |
|---|---|---|
| `nats_jetstream_consumer_pending_messages` | Gauge | Необработанные сообщения в consumer |
| `nats_jetstream_consumer_info` | Gauge | 1 — consumer жив, 0 — нет |

Правило Prometheus Adapter преобразует первую в `pods/nats_queue_pending`.

## Логика HPA

HPA масштабирует collector:
- **scale up**, если `nats_queue_pending > 500` в среднем на под (очередь растёт)
- **scale down** через 120 сек стабилизации
- также учитывает CPU (70%) и RAM (80%)

## Деплой

```bash
# 1. Установить Prometheus Stack (kube-prometheus-stack)
# 2. Установить prometheus-adapter
# 3. Применить манифесты
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/servicemonitor.yaml
kubectl apply -f deploy/k8s/prometheus-adapter-configmap.yaml
kubectl apply -f deploy/k8s/hpa.yaml

# 4. Перезапустить prometheus-adapter с новой конфигурацией
kubectl rollout restart -n monitoring deployment prometheus-adapter

# 5. Проверить, что метрика доступна
kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1 | jq .
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/review-collector/pods/*/nats_queue_pending" | jq .
```
