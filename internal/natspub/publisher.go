package natspub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/models"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// NATSConnection определяет контракт для подключения к NATS.
type NATSConnection interface {
	Drain() error
	Close()
}

// JetStreamClient определяет контракт для операций JetStream.
type JetStreamClient interface {
	CreateOrUpdateStream(ctx context.Context, config jetstream.StreamConfig) (jetstream.Stream, error)
	PublishAsync(subject string, data []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
	Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	Consumer(ctx context.Context, stream, consumer string) (jetstream.Consumer, error)
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
}

// natsConnAdapter адаптирует *nats.Conn к NATSConnection.
type natsConnAdapter struct{ conn *nats.Conn }

func (a *natsConnAdapter) Drain() error { return a.conn.Drain() }
func (a *natsConnAdapter) Close()       { a.conn.Close() }

const (
	StreamName          = "reviews"
	RawSubject          = "reviews.raw"
	WindowedSubject     = "reviews.windowed"
	StreamMaxAge        = 24 * time.Hour
	DedupWindow         = 2 * time.Minute
	PublishTimeout      = 30 * time.Second
	MaxRetries          = 3
	AckWaitInterval     = 500 * time.Millisecond
)

// JetStreamPublisher публикует данные в NATS JetStream с exactly-once доставкой.
type JetStreamPublisher struct {
	nc     NATSConnection
	js     JetStreamClient
	logger *zap.Logger

	pubAckCh chan jetstream.PubAckFuture

	published atomic.Int64
	failed    atomic.Int64
	pending   atomic.Int64

	stopCh chan struct{}
	wg     sync.WaitGroup
	stopped atomic.Bool
}

func NewJetStreamPublisher(ctx context.Context, nc NATSConnection, js JetStreamClient, logger *zap.Logger) (*JetStreamPublisher, error) {

	p := &JetStreamPublisher{
		nc:       nc,
		js:       js,
		logger:   logger,
		pubAckCh: make(chan jetstream.PubAckFuture, 10000),
		stopCh:   make(chan struct{}),
	}

	if err := p.initStream(ctx); err != nil {
		nc.Close()
		return nil, fmt.Errorf("init stream: %w", err)
	}

	p.wg.Add(1)
	go p.ackLoop(ctx)

	logger.Info("JETSTREAM_PUBLISHER_READY",
		zap.String("stream", StreamName),
		zap.Strings("subjects", []string{RawSubject, WindowedSubject}),
	)

	return p, nil
}

// NewNATSClient создаёт подключение к NATS и JetStream клиент, обёрнутые в интерфейсы.
func NewNATSClient(ctx context.Context, urls string, logger *zap.Logger) (NATSConnection, JetStreamClient, error) {
	nc, err := nats.Connect(urls,
		nats.Name("review-collector"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream new: %w", err)
	}

	return &natsConnAdapter{conn: nc}, js, nil
}

func (p *JetStreamPublisher) initStream(ctx context.Context) error {
	_, err := p.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{RawSubject, WindowedSubject},
		MaxAge:     StreamMaxAge,
		MaxMsgs:    -1,
		MaxBytes:   -1,
		Storage:    jetstream.FileStorage,
		Duplicates: DedupWindow,
		Retention:  jetstream.LimitsPolicy,
		Discard:    jetstream.DiscardOld,
	})
	return err
}

// PublishRaw отправляет сырой отзыв в "reviews.raw".
func (p *JetStreamPublisher) PublishRaw(ctx context.Context, review models.Review) error {
	return p.publishJSON(ctx, RawSubject, review, review.ID)
}

// PublishWindowAgg отправляет агрегированное окно в "reviews.windowed".
// Dedup-ключ = productID + windowStart для exactly-one доставки.
func (p *JetStreamPublisher) PublishWindowAgg(ctx context.Context, agg models.WindowAgg) error {
	dedupKey := fmt.Sprintf("%s-%d", agg.ProductID, agg.WindowStart.UnixNano())
	return p.publishJSON(ctx, WindowedSubject, agg, dedupKey)
}

// publishJSON сериализует любой объект в JSON и публикует в JetStream.
func (p *JetStreamPublisher) publishJSON(ctx context.Context, subject string, v any, dedupKey string) error {
	if p.stopped.Load() {
		return errors.New("publisher is stopped")
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	p.pending.Add(1)

	future, err := p.js.PublishAsync(subject, data,
		jetstream.WithMsgID(dedupKey),
	)
	if err != nil {
		p.pending.Add(-1)
		return fmt.Errorf("publish async: %w", err)
	}

	select {
	case p.pubAckCh <- future:
	case <-ctx.Done():
		p.pending.Add(-1)
		return ctx.Err()
	}

	return nil
}

// ackLoop обрабатывает подтверждения от PublishAsync.
func (p *JetStreamPublisher) ackLoop(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			p.drainPending()
			return
		case future, ok := <-p.pubAckCh:
			if !ok {
				return
			}

			msg := future.Msg()
			msgID := msg.Header.Get(jetstream.MsgIDHeader)

			select {
			case ack := <-future.Ok():
				p.published.Add(1)
				p.pending.Add(-1)
				p.logger.Debug("PUB_ACK_OK",
					zap.String("msg_id", msgID),
					zap.String("stream", ack.Stream),
					zap.Uint64("seq", ack.Sequence),
					zap.Bool("duplicate", ack.Duplicate),
				)

			case err := <-future.Err():
				p.failed.Add(1)
				p.pending.Add(-1)
				p.logger.Warn("PUB_ACK_ERR",
					zap.String("msg_id", msgID),
					zap.Error(err),
				)
				go p.retryPublish(ctx, msg, msgID, 1)
			}
		}
	}
}

// retryPublish повторяет публикацию с экспоненциальной задержкой.
func (p *JetStreamPublisher) retryPublish(ctx context.Context, msg *nats.Msg, msgID string, attempt int) {
	if attempt > MaxRetries {
		p.logger.Error("PUB_RETRY_EXHAUSTED",
			zap.String("msg_id", msgID),
			zap.Int("attempts", attempt-1),
		)
		return
	}

	backoff := time.Duration(math.Pow(2, float64(attempt))) * 500 * time.Millisecond

	select {
	case <-ctx.Done():
		return
	case <-time.After(backoff):
	}

	pubCtx, cancel := context.WithTimeout(ctx, PublishTimeout)
	defer cancel()

	_, err := p.js.Publish(pubCtx, msg.Subject, msg.Data,
		jetstream.WithMsgID(msgID),
	)
	if err != nil {
		p.logger.Warn("PUB_RETRY_FAILED",
			zap.String("msg_id", msgID),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		p.retryPublish(ctx, msg, msgID, attempt+1)
		return
	}

	p.published.Add(1)
	p.logger.Info("PUB_RETRY_SUCCESS",
		zap.String("msg_id", msgID),
		zap.Int("attempt", attempt),
	)
}

func (p *JetStreamPublisher) Stats() (published, failed, pending int64) {
	return p.published.Load(), p.failed.Load(), p.pending.Load()
}

// JetStream возвращает JetStream-контекст для внешних компонентов (например, монитора очереди).
func (p *JetStreamPublisher) JetStream() JetStreamClient {
	return p.js
}

func (p *JetStreamPublisher) drainPending() {
	p.logger.Info("DRAINING_PENDING_MESSAGES",
		zap.Int64("pending", p.pending.Load()),
	)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if p.pending.Load() == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if p.pending.Load() > 0 {
		p.logger.Warn("PENDING_MESSAGES_REMAINING",
			zap.Int64("pending", p.pending.Load()),
		)
	}
}

func (p *JetStreamPublisher) Close() error {
	p.stopped.Store(true)
	close(p.stopCh)
	p.wg.Wait()

	if err := p.nc.Drain(); err != nil {
		p.logger.Error("NATS_DRAIN_ERROR", zap.Error(err))
	}
	p.nc.Close()

	p.logger.Info("JETSTREAM_PUBLISHER_CLOSED",
		zap.Int64("published", p.published.Load()),
		zap.Int64("failed", p.failed.Load()),
	)

	return nil
}
