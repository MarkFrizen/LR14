// Package monitor предоставляет Prometheus-метрики для NATS JetStream,
// в частности длину очереди (количество ожидающих сообщений) для consumer'ов.
package monitor

import (
	"context"
	"fmt"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/natspub"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Metrics содержит Prometheus-метрики.
type Metrics struct {
	natsPendingMsgs  *prometheus.GaugeVec
	natsConsumerInfo *prometheus.GaugeVec
}

// NewMetrics регистрирует и возвращает набор метрик.
func NewMetrics() *Metrics {
	return &Metrics{
		natsPendingMsgs: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "nats_jetstream_consumer_pending_messages",
			Help: "Number of pending (unacknowledged) messages in NATS JetStream consumer",
		}, []string{"stream", "consumer"}),

		natsConsumerInfo: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "nats_jetstream_consumer_info",
			Help: "Consumer metadata: 1 if consumer exists, 0 otherwise",
		}, []string{"stream", "consumer"}),
	}
}

// QueueMonitor периодически опрашивает NATS JetStream и обновляет метрики.
type QueueMonitor struct {
	js      natspub.JetStreamClient
	metrics *Metrics
	logger  *zap.Logger
	stream  string

	consumers []string // имена отслеживаемых consumer'ов
}

// NewQueueMonitor создаёт монитор очереди NATS JetStream.
//
// Параметры:
//   - js: JetStream-клиент
//   - stream: имя JetStream-стрима
//   - consumers: имена consumer'ов, чью pending-очередь отслеживать
//   - logger: логгер
func NewQueueMonitor(js natspub.JetStreamClient, stream string, consumers []string, logger *zap.Logger) *QueueMonitor {
	return &QueueMonitor{
		js:        js,
		metrics:   NewMetrics(),
		logger:    logger,
		stream:    stream,
		consumers: consumers,
	}
}

// Run запускает цикл опроса NATS. Блокируется до отмены ctx.
func (qm *QueueMonitor) Run(ctx context.Context) {
	// Сразу сбрасываем consumer_info в 0 — чтобы метка существовала.
	for _, c := range qm.consumers {
		qm.metrics.natsConsumerInfo.WithLabelValues(qm.stream, c).Set(0)
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	qm.logger.Info("QUEUE_MONITOR_STARTED",
		zap.String("stream", qm.stream),
		zap.Strings("consumers", qm.consumers),
	)

	for {
		select {
		case <-ctx.Done():
			qm.logger.Info("QUEUE_MONITOR_STOPPED")
			return
		case <-ticker.C:
			qm.poll(ctx)
		}
	}
}

func (qm *QueueMonitor) poll(ctx context.Context) {
	for _, consumerName := range qm.consumers {
		info, err := qm.js.Consumer(ctx, qm.stream, consumerName)
		if err != nil {
			qm.logger.Warn("CONSUMER_INFO_FAILED",
				zap.String("stream", qm.stream),
				zap.String("consumer", consumerName),
				zap.Error(err),
			)
			qm.metrics.natsConsumerInfo.WithLabelValues(qm.stream, consumerName).Set(0)
			qm.metrics.natsPendingMsgs.WithLabelValues(qm.stream, consumerName).Set(0)
			continue
		}

		cachedInfo := info.CachedInfo()

		// Consumer существует — помечаем.
		qm.metrics.natsConsumerInfo.WithLabelValues(qm.stream, consumerName).Set(1)

		// NumPending — количество необработанных (не подтверждённых) сообщений.
		pending := float64(cachedInfo.NumPending)
		qm.metrics.natsPendingMsgs.WithLabelValues(qm.stream, consumerName).Set(pending)

		qm.logger.Debug("QUEUE_METRIC_UPDATED",
			zap.String("stream", qm.stream),
			zap.String("consumer", consumerName),
			zap.Float64("pending", pending),
			zap.Int("delivered", cachedInfo.NumAckPending),
		)
	}
}

// MustRegisterConsumer пытается создать/найти consumer на стриме.
// Если consumer уже существует — использует его.
// Возвращает имя consumer'а.
func MustRegisterConsumer(ctx context.Context, js natspub.JetStreamClient, stream, consumerName, filterSubject string, logger *zap.Logger) (jetstream.Consumer, error) {
	// Пробуем найти существующего.
	existing, err := js.Consumer(ctx, stream, consumerName)
	if err == nil {
		logger.Info("CONSUMER_ALREADY_EXISTS",
			zap.String("stream", stream),
			zap.String("consumer", consumerName),
			zap.Uint64("pending", existing.CachedInfo().NumPending),
		)
		return existing, nil
	}

	// Создаём pull-consumer.
	cons, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    3,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer %s on stream %s: %w", consumerName, stream, err)
	}

	logger.Info("CONSUMER_CREATED",
		zap.String("stream", stream),
		zap.String("consumer", consumerName),
		zap.String("filter", filterSubject),
	)
	return cons, nil
}
