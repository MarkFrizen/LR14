# review-pipeline Helm Chart

Комплексный Helm-чарт для развёртывания ETL-конвейера анализа отзывов в minikube.

## Архитектура

```
                    ┌─────────────────────────────────────┐
                    │         etcd (координация шардов)      │
                    └──────────────┬──────────────────────┘
                                   │
┌────────────────┐    ┌────────────┴──────────────┐    ┌────────────────┐
│  Go Collector  │◄──►│     NATS JetStream        │    │  Python        │
│  (Deployment)  │───►│  (Stream: reviews)        │───►│  Analyzer      │
│  + HPA         │    │  ─ reviews.raw            │    │  (Deployment)  │
│  + ServiceMon  │    │  ─ reviews.windowed       │    │                │
└────────────────┘    └────────────┬──────────────┘    └───────┬────────┘
                                   │                           │
                                   │                    ┌──────┴────────┐
                                   │                    │  Streamlit    │
                                   │                    │  Dashboard    │
                                   │                    │  (Deployment) │
                                   │                    └───────────────┘
                                   │
                          ┌────────┴────────┐
                          │   Prometheus     │
                          │  Operator Stack  │
                          └────────┬────────┘
                                   │
                          ┌────────┴────────┐
                          │ Prometheus       │
                          │ Adapter          │
                          │ (custom metrics) │
                          └────────┬────────┘
                                   │
                          ┌────────┴────────┐
                          │    HPA          │
                          │ (nats_queue_)   │
                          │ (pending_cpu_   │
                          │  ram)           │
                          └─────────────────┘
```

## Компоненты

| Компонент | Описание | Тип |
|---|---|---|
| **etcd** | Координация распределённых шардов | bitnami/etcd |
| **NATS** | Потоковая передача данных (JetStream) | bitnami/nats |
| **Go Collector** | Сбор отзывов, оконная агрегация, публикация | Custom Deployment |
| **Python Analyzer** | Чтение из NATS, агрегация в Parquet | Custom Deployment |
| **Streamlit** | Веб-дашборд с графиками | Custom Deployment + Service |

## Быстрый старт

### 1. Установить зависимости (Helm)

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

### 2. Собрать и загрузить Docker-образы

```bash
# Включить minikube docker-env для сборки локально
eval $(minikube docker-env)

# 1) Go Collector
docker build -t review-collector:latest -f Dockerfile .

# 2) Python Analyzer
docker build -t review-analyzer:latest \
  deploy/helm/review-pipeline/analyzer/

# 3) Streamlit
docker build -t review-streamlit:latest \
  deploy/helm/review-pipeline/streamlit/
```

### 3. Установить Prometheus Stack (для ServiceMonitor + Adapter)

```bash
helm upgrade --install prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false
```

### 4. Установить Prometheus Adapter

```bash
helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
  --namespace monitoring \
  --set prometheus.url=http://prometheus-stack-prometheus.monitoring.svc:9090
```

### 5. Установить чарт

```bash
cd deploy/helm/review-pipeline

# Установка с зависимостями
helm dependency build
helm upgrade --install review-pipeline . --namespace review-pipeline --create-namespace
```

### 6. Проверить

```bash
# Все поды
kubectl get pods -n review-pipeline -w

# HPA
kubectl get hpa -n review-pipeline
kubectl describe hpa -n review-pipeline

# NATS consumer
kubectl exec -n review-pipeline deploy/review-pipeline-collector -- \
  wget -qO- http://localhost:8080/metrics | grep nats_jetstream

# Streamlit (port-forward)
kubectl port-forward -n review-pipeline svc/review-pipeline-streamlit 8501:80
# → http://localhost:8501
```

## Тестирование HPA

### Запустить нагрузочный тест

```bash
# Вариант A: локально (если NATS доступен через port-forward)
python deploy/helm/review-pipeline/load-test.py --nats nats://localhost:4222 --reviews 100000

# Вариант B: в кластере
kubectl run -n review-pipeline load-generator --rm -it --image=python:3.12-slim \
  -- /bin/bash -c "
    pip install -q nats-py &&
    curl -s https://raw.githubusercontent.com/MarkFrizen/LR14/main/deploy/helm/review-pipeline/load-test.py \
    | python - --nats nats://review-pipeline-nats:4222 --reviews 100000
  "
```

### Наблюдать автоскалирование

```bash
# В одном терминале — HPA
kubectl get hpa -n review-pipeline -w

# В другом — поды
kubectl get pods -n review-pipeline -w

# Третий — метрики NATS
kubectl exec -n review-pipeline deploy/review-pipeline-collector -- \
  wget -qO- http://localhost:8080/metrics | grep nats_jetstream_consumer_pending
```

Ожидаемое поведение:
1. При `nats_queue_pending > 500` — HPA начинает scale up
2. Через 30–60 сек появляются дополнительные поды collector
3. После снижения очереди — scale down через 120 сек стабилизации

## Параметры values.yaml

Основные параметры (`values.yaml`):

| Параметр | По умолчанию | Описание |
|---|---|---|
| `etcd.enabled` | `true` | Использовать встроенный etcd |
| `nats.enabled` | `true` | Использовать встроенный NATS |
| `collector.replicas` | `3` | Начальное количество сборщиков |
| `collector.hpa.enabled` | `true` | Включить HPA |
| `collector.hpa.customMetrics.natsPendingTarget` | `500` | Порог для scale up |
| `analyzer.flushInterval` | `30` | Интервал сброса буфера (сек) |
| `streamlit.serviceType` | `ClusterIP` | Тип Service для дашборда |

## Остановка и удаление

```bash
helm uninstall review-pipeline -n review-pipeline
helm uninstall prometheus-stack -n monitoring
helm uninstall prometheus-adapter -n monitoring
minikube stop
```
