package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/aggregator"
	"github.com/markfriz/wb-ozon-review-collector/internal/collector"
	"github.com/markfriz/wb-ozon-review-collector/internal/coordinator"
	"github.com/markfriz/wb-ozon-review-collector/internal/kafkapub"
	"github.com/markfriz/wb-ozon-review-collector/internal/marketplace"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func main() {
	var (
		etcdEndpoints = flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
		kafkaBrokers  = flag.String("kafka", "localhost:9092", "Kafka brokers (comma-separated)")
		workerID      = flag.String("worker", defaultWorkerID(), "unique worker ID")
		source        = flag.String("source", "wildberries", "marketplace source (wildberries/ozon)")
		windowDur     = flag.Duration("window", 1*time.Minute, "tumbling window duration (e.g. 30s, 1m, 5m)")
		watermark     = flag.Duration("watermark", 2*time.Minute, "watermark for late events (e.g. 1m, 2m)")
		crashAfter    = flag.Duration("crash-after", 0, "симулировать падение узла через указанный интервал")
		demoMode      = flag.Bool("demo", false, "демонстрационный режим: подробные логи")
		healthAddr    = flag.String("health-addr", ":8080", "адрес HTTP-сервера для health-чеков")
	)
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Флаг здоровья для Kubernetes probes.
	var healthy atomic.Bool
	healthy.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// ========================================================================
	// 1. etcd — координатор шардов
	// ========================================================================
	endpoints := splitEndpoints(*etcdEndpoints)
	logger.Info("CONNECTING_TO_ETCD",
		zap.Strings("endpoints", endpoints),
		zap.String("worker", *workerID),
	)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      logger,
	})
	if err != nil {
		log.Fatalf("FAILED to create etcd client: %v", err)
	}

	coord, err := coordinator.NewCoordinator(ctx, etcdCli, *workerID, logger)
	if err != nil {
		log.Fatalf("FAILED to create coordinator: %v", err)
	}
	defer func() {
		logger.Info("COORDINATOR_CLEANUP")
		if err := coord.Close(context.Background()); err != nil {
			logger.Error("COORDINATOR_CLOSE_ERROR", zap.Error(err))
		}
	}()

	allProducts := []string{
		"WB-001", "WB-002", "WB-003", "WB-004", "WB-005",
		"OZ-101", "OZ-102", "OZ-103",
	}
	if err := coord.BootstrapProducts(ctx, allProducts); err != nil {
		log.Fatalf("FAILED to bootstrap products: %v", err)
	}
	showAssignments(ctx, logger, coord, "before claiming")

	// ========================================================================
	// 2. Сборщик отзывов
	// ========================================================================
	market := marketplace.NewSimulator(*source)
	col := collector.New(coord, market)

	go func() {
		if err := col.Run(ctx); err != nil && err != context.Canceled {
			logger.Error("COLLECTOR_RUN_ERROR", zap.Error(err))
		}
	}()

	// ========================================================================
	// 3. Kafka producer — публикация сырых отзывов в "reviews.raw"
	// ========================================================================
	brokers := splitEndpoints(*kafkaBrokers)
	logger.Info("CONNECTING_TO_KAFKA",
		zap.Strings("brokers", brokers),
	)

	kafkaPub := kafkapub.NewKafkaPublisher(brokers, logger)

	// Канал для подсчёта опубликованных сырых отзывов.
	var rawPublished atomic.Int64
	var rawFailed atomic.Int64

	// ========================================================================
	// 4. Оконная агрегация (tumbling window) — для WindowAgg
	// ========================================================================
	tw := aggregator.NewTumblingWindow(*windowDur, *watermark, 5000)

	// Forward: collector → Kafka (raw) + aggregator (windowed)
	go func() {
		for review := range col.Reviews() {
			// --- публикуем сырой отзыв в Kafka (топик reviews.raw) ---
			if err := kafkaPub.PublishRaw(ctx, review); err != nil {
				rawFailed.Add(1)
				logger.Warn("KAFKA_PUBLISH_RAW_FAILED",
					zap.String("review_id", review.ID),
					zap.Error(err),
				)
			} else {
				rawPublished.Add(1)
			}

			// --- отправляем в агрегатор для оконной обработки ---
			tw.Input() <- review
		}
		tw.Stop()
	}()

	// Горутина: читает WindowAgg из агрегатора, логирует результаты.
	// В будущем можно добавить публикацию WindowAgg в отдельный Kafka-топик.
	go func() {
		for agg := range tw.Output() {
			if agg.IsUpdate {
				logger.Info("WINDOW_UPDATE_PUBLISHED",
					zap.String("product", agg.ProductID),
					zap.Time("window_start", agg.WindowStart),
					zap.Float64("avg_rating", agg.AvgRating),
					zap.Int("total_likes", agg.TotalLikes),
					zap.Int("review_count", agg.ReviewCount),
					zap.String("note", "late_event_recompute"),
				)
			} else {
				logger.Info("WINDOW_AGG_PUBLISHED",
					zap.String("product", agg.ProductID),
					zap.Time("window_start", agg.WindowStart),
					zap.Float64("avg_rating", agg.AvgRating),
					zap.Int("total_likes", agg.TotalLikes),
					zap.Int("review_count", agg.ReviewCount),
				)
			}
		}
	}()

	// Запуск агрегатора.
	go tw.Run(ctx)

	logger.Info("PIPELINE_STARTED",
		zap.String("worker", *workerID),
		zap.String("source", *source),
		zap.Duration("window", *windowDur),
		zap.Duration("watermark", *watermark),
		zap.Strings("kafka_brokers", brokers),
		zap.String("kafka_topic", kafkapub.TopicRawReviews),
	)

	// ========================================================================
	// 5. HTTP-сервер для Kubernetes health probes
	// ========================================================================
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "shutting_down"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	healthListener, err := net.Listen("tcp", *healthAddr)
	if err != nil {
		logger.Fatal("HEALTH_LISTENER_FAILED", zap.String("addr", *healthAddr), zap.Error(err))
	}
	healthServer := &http.Server{Handler: mux}
	go func() {
		logger.Info("HEALTH_SERVER_STARTED", zap.String("addr", healthListener.Addr().String()))
		if err := healthServer.Serve(healthListener); err != nil && err != http.ErrServerClosed {
			logger.Error("HEALTH_SERVER_ERROR", zap.Error(err))
		}
	}()

	// ========================================================================
	// 6. Режим crash-after
	// ========================================================================
	if *crashAfter > 0 {
		logger.Warn("CRASH_TIMER_ARMED", zap.Duration("will_crash_in", *crashAfter))
		go func() {
			select {
			case <-time.After(*crashAfter):
				logger.Error("CRASH_SIMULATED", zap.String("action", "FORCE_EXIT"))
				if !*demoMode {
					os.Exit(137)
				}
			case <-ctx.Done():
			}
		}()
	}

	// ========================================================================
	// 7. Ожидание сигнала → graceful shutdown
	// ========================================================================
	sig := <-sigCh
	logger.Info("SIGNAL_RECEIVED", zap.String("signal", sig.String()))

	// Отмечаем сервер недоступным для Kubernetes probes.
	healthy.Store(false)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	logger.Info("GRACEFUL_SHUTDOWN_STARTING")

	showAssignments(shutdownCtx, logger, coord, "before release")

	col.Stop()
	<-tw.Flushed()

	// Закрываем Kafka producer (flush + close).
	logger.Info("KAFKA_PUBLISHER_STATS",
		zap.Int64("raw_published", rawPublished.Load()),
		zap.Int64("raw_failed", rawFailed.Load()),
	)
	if err := kafkaPub.Close(); err != nil {
		logger.Error("KAFKA_PUBLISHER_CLOSE_ERROR", zap.Error(err))
	}

	logger.Info("RELEASING_SHARDS", zap.Int("shard_count", len(coord.OwnedProducts())))
	if err := coord.ReleaseAll(shutdownCtx); err != nil {
		logger.Error("RELEASE_ALL_ERROR", zap.Error(err))
	}

	cancel()

	<-shutdownCtx.Done()
	if shutdownCtx.Err() == context.DeadlineExceeded {
		logger.Warn("GRACEFUL_SHUTDOWN_TIMED_OUT")
	}

	logger.Info("COLLECTOR_EXITED")
}

// ========================================================================

func showAssignments(ctx context.Context, logger *zap.Logger, coord *coordinator.Coordinator, phase string) {
	assignments, err := coord.ListAssignments(ctx)
	if err != nil {
		logger.Warn("LIST_ASSIGNMENTS_FAILED", zap.Error(err))
		return
	}
	fields := make([]zap.Field, 0, len(assignments)+1)
	fields = append(fields, zap.String("phase", phase))
	for pid, holder := range assignments {
		fields = append(fields, zap.String(pid, holder))
	}
	logger.Info("ASSIGNMENTS", fields...)
}

func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown-worker"
	}
	return fmt.Sprintf("worker-%s-%d", hostname, time.Now().UnixNano()%10000)
}

func splitEndpoints(s string) []string {
	var eps []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				eps = append(eps, s[start:i])
			}
			start = i + 1
		}
	}
	return eps
}
