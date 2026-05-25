package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/collector"
	"github.com/markfriz/wb-ozon-review-collector/internal/coordinator"
	"github.com/markfriz/wb-ozon-review-collector/internal/marketplace"
	"github.com/markfriz/wb-ozon-review-collector/internal/natspub"
	"go.uber.org/zap"
)

func main() {
	var (
		etcdEndpoints = flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
		natsURL       = flag.String("nats", "nats://localhost:4222", "NATS server URL")
		workerID      = flag.String("worker", defaultWorkerID(), "unique worker ID")
		source        = flag.String("source", "wildberries", "marketplace source (wildberries/ozon)")
		crashAfter    = flag.Duration("crash-after", 0, "симулировать падение узла через указанный интервал (10s, 30s)")
		demoMode      = flag.Bool("demo", false, "демонстрационный режим: логи более подробные")
	)
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Перехват сигналов ОС.
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

	coord, err := coordinator.NewCoordinator(ctx, endpoints, *workerID)
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
	// 2. NATS JetStream — публикатор отзывов
	// ========================================================================
	logger.Info("CONNECTING_TO_NATS",
		zap.String("url", *natsURL),
		zap.String("stream", natspub.StreamName),
		zap.String("subject", natspub.StreamSubject),
	)

	publisher, err := natspub.NewJetStreamPublisher(ctx, *natsURL, logger)
	if err != nil {
		log.Fatalf("FAILED to create NATS publisher: %v", err)
	}
	defer func() {
		logger.Info("NATS_PUBLISHER_CLOSE")
		pub, failed, pending := publisher.Stats()
		logger.Info("NATS_FINAL_STATS",
			zap.Int64("published", pub),
			zap.Int64("failed", failed),
			zap.Int64("pending", pending),
		)
		if err := publisher.Close(); err != nil {
			logger.Error("NATS_PUBLISHER_CLOSE_ERROR", zap.Error(err))
		}
	}()

	// ========================================================================
	// 3. Сборщик отзывов (etcD + marketplace + NATS publisher)
	// ========================================================================
	market := marketplace.NewSimulator(*source)
	col := collector.New(coord, market, publisher)

	// Локальный вывод собранных отзывов.
	go printCollectedReviews(ctx, logger, col)

	// Запуск сборщика.
	go func() {
		if err := col.Run(ctx); err != nil && err != context.Canceled {
			logger.Error("COLLECTOR_RUN_ERROR", zap.Error(err))
		}
	}()

	logger.Info("COLLECTOR_STARTED",
		zap.String("worker", *workerID),
		zap.String("source", *source),
		zap.Strings("etcd", endpoints),
		zap.String("nats", *natsURL),
		zap.Duration("crash_after", *crashAfter),
	)

	// ========================================================================
	// 4. Режим crash-after — симуляция падения узла
	// ========================================================================
	if *crashAfter > 0 {
		logger.Warn("CRASH_TIMER_ARMED",
			zap.Duration("will_crash_in", *crashAfter),
		)
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
	// 5. Ожидание сигнала → graceful shutdown
	// ========================================================================
	sig := <-sigCh
	logger.Info("SIGNAL_RECEIVED", zap.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	logger.Info("GRACEFUL_SHUTDOWN_STARTING")

	// 5a. Показываем назначения перед освобождением.
	showAssignments(shutdownCtx, logger, coord, "before release")

	// 5b. Останавливаем сборщик (ждём завершения горутин).
	col.Stop()

	// 5c. Освобождаем шарды в etcd.
	logger.Info("RELEASING_SHARDS",
		zap.Int("shard_count", len(coord.OwnedProducts())),
	)
	if err := coord.ReleaseAll(shutdownCtx); err != nil {
		logger.Error("RELEASE_ALL_ERROR", zap.Error(err))
	}

	cancel()

	// 5d. Ждём завершения всех горутин.
	<-shutdownCtx.Done()
	if shutdownCtx.Err() == context.DeadlineExceeded {
		logger.Warn("GRACEFUL_SHUTDOWN_TIMED_OUT")
	}

	// Примечание: publisher.Close() и coord.Close() выполняются в defer.
	logger.Info("COLLECTOR_EXITED")
}

// showAssignments выводит карту назначений шардов.
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

// printCollectedReviews читает отзывы из локального канала и выводит в лог.
func printCollectedReviews(ctx context.Context, logger *zap.Logger, col *collector.Collector) {
	count := 0
	for {
		select {
		case <-ctx.Done():
			return
		case review, ok := <-col.Reviews():
			if !ok {
				logger.Info("REVIEW_CHANNEL_CLOSED", zap.Int("total", count))
				return
			}
			count++
			logger.Info("REVIEW_COLLECTED",
				zap.String("id", review.ID),
				zap.String("product", review.ProductID),
				zap.Int("rating", review.Rating),
				zap.String("text", review.Text),
				zap.Int("likes", review.Likes),
				zap.Int("dislikes", review.Dislikes),
				zap.Time("date", review.Date),
			)
			if count%25 == 0 {
				logger.Info("REVIEW_PROGRESS", zap.Int("collected", count))
			}
		}
	}
}
