// Package kafkapub — Kafka producer для публикации сырых отзывов
// в топик "reviews.raw" (замена NATS JetStream).
//
// Партицирование: ключ = product_id, алгоритм = hash(product_id) % num_partitions.
// Это гарантирует строгий порядок отзывов внутри одного product_id.
package kafkapub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"

	"github.com/markfriz/wb-ozon-review-collector/internal/models"
)

// Топик для сырых отзывов.
const TopicRawReviews = "reviews.raw"

// KafkaPublisher публикует сырые отзывы в Kafka (топик "reviews.raw").
// Аналог natspub.JetStreamPublisher, но через Kafka.
type KafkaPublisher struct {
	writer  *kafka.Writer
	logger  *zap.Logger

	published atomic.Int64
	failed    atomic.Int64

	closeMu sync.Mutex
	closed  bool
}

// NewKafkaPublisher создаёт Kafka producer.
//
// Параметры:
//   - brokers: список брокеров (например, []string{"localhost:9092"})
//   - logger: zap-logger
//
// Использует:
//   - kafka.Hash balancer — отзывы с одинаковым product_id попадают
//     в одну партицию (гарантия порядка)
//   - Snappy сжатие на уровне producer
//   - RequiredAcks = RequireOne (лидер подтвердил запись)
func NewKafkaPublisher(brokers []string, logger *zap.Logger) *KafkaPublisher {
	w := &kafka.Writer{
		Addr:          kafka.TCP(brokers...),
		Topic:         TopicRawReviews,
		Balancer:      &kafka.Hash{},      // key = product_id → hash → partition
		Compression:   compress.Snappy,    // Snappy — быстрый, без потерь
		WriteTimeout:  30 * time.Second,
		ReadTimeout:   10 * time.Second,
		BatchTimeout:  5 * time.Millisecond, // низкая задержка для real-time
		BatchSize:     100,                  // макс 100 сообщений в батче
		RequiredAcks:  kafka.RequireOne,    // ждём подтверждения от лидера
		Async:         false,                // синхронная запись (ждём ack)
		Logger:        kafka.LoggerFunc(func(s string, a ...interface{}) {}),
		ErrorLogger:   kafka.LoggerFunc(func(s string, a ...interface{}) {}),
	}

	return &KafkaPublisher{
		writer: w,
		logger: logger,
	}
}

// PublishRaw отправляет один сырой отзыв в Kafka (топик "reviews.raw").
//
// Партицирование: Kafka.Hash использует hash(product_id) → partition,
// что гарантирует упорядоченность отзывов каждого товара.
//
// Сообщение: JSON-сериализованный models.Review, ключ = product_id.
func (kp *KafkaPublisher) PublishRaw(ctx context.Context, review models.Review) error {
	data, err := json.Marshal(review)
	if err != nil {
		return fmt.Errorf("marshal review: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(review.ProductID),
		Value: data,
	}

	if err := kp.writer.WriteMessages(ctx, msg); err != nil {
		kp.failed.Add(1)
		return fmt.Errorf("kafka write: %w", err)
	}

	kp.published.Add(1)
	return nil
}

// PublishRawBatch отправляет пачку сырых отзывов в Kafka одним WriteMessages.
// Все отзывы в батче могут попасть в разные партиции (по hash(product_id)).
func (kp *KafkaPublisher) PublishRawBatch(ctx context.Context, reviews []models.Review) error {
	if len(reviews) == 0 {
		return nil
	}

	messages := make([]kafka.Message, len(reviews))
	for i, r := range reviews {
		data, err := json.Marshal(r)
		if err != nil {
			kp.failed.Add(1)
			continue
		}
		messages[i] = kafka.Message{
			Key:   []byte(r.ProductID),
			Value: data,
		}
	}

	// WriteMessages атомарно пишет батч (даже если messages попадают в разные партиции).
	if err := kp.writer.WriteMessages(ctx, messages...); err != nil {
		kp.failed.Add(int64(len(reviews)))
		return fmt.Errorf("kafka batch write: %w", err)
	}

	kp.published.Add(int64(len(reviews)))
	return nil
}

// Stats возвращает количество опубликованных и упавших сообщений.
func (kp *KafkaPublisher) Stats() (published, failed int64) {
	return kp.published.Load(), kp.failed.Load()
}

// Close закрывает Kafka writer (flush + close).
func (kp *KafkaPublisher) Close() error {
	kp.closeMu.Lock()
	defer kp.closeMu.Unlock()

	if kp.closed {
		return nil
	}
	kp.closed = true

	if err := kp.writer.Close(); err != nil {
		kp.logger.Error("KAFKA_WRITER_CLOSE_ERROR", zap.Error(err))
		return err
	}

	kp.logger.Info("KAFKA_PUBLISHER_CLOSED",
		zap.Int64("published", kp.published.Load()),
		zap.Int64("failed", kp.failed.Load()),
	)
	return nil
}
