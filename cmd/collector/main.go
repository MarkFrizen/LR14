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

	"github.com/markfriz/wb-ozon-review-collector/internal/aggregator"
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
		windowDur     = flag.Duration("window", 1*time.Minute, "tumbling window duration (e.g. 30s, 1m, 5m)")
		watermark     = flag.Duration("watermark", 2*time.Minute, "watermark for late events (e.g. 1m, 2m)")
		crashAfter    = flag.Duration("crash-after", 0, "симулировать падение узла через указанный интервал")
		demoMode      = flag.Bool("demo", false, "демонстрационный режим: подробные логи")
	)
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

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
	// 3. Оконная агрегация с поддержкой поздних событий (watermark)
	// ========================================================================
	//
	// Алгоритм:
	//   1. Каждый отзыв маршрутизируется в окно по review.Date (event time).
	//   2. Окна хранятся в хеш-таблице windowStore с sync.Mutex.
	//   3. На каждом тике ticker'а окна, чьё время истекло, флашатся.
	//   4. После флаша окно остаётся в памяти на время watermark.
	//   5. Поздние события вызывают пересчёт → WindowAgg с IsUpdate=true.
	//   6. Окна старше watermark удаляются.
	//
	// Пайплайн:
	//   collector ──► tw.Input()
	//                    │
	//          ┌─────────┴──────────────┐
	//          │ review.Date → windowKey │
	//          │ store.getOrCreate()     │
	//          │ accumulate()            │
	//          │ if flushed → recompute  │
	//          └─────────┬──────────────┘
	//                    │
	//          ┌─────────┴──────────────┐
	//          │ ticker: reapWindows()  │
	//          │   • flush closed       │
	//          │   • evict old          │
	//          └─────────┬──────────────┘
	//                    │
	//                    ▼
	//               tw.Output()
	//              (WindowAgg)
	//                    │
	//          ┌─────────┴──────────┐
	//          ▼                    ▼
	//   NATS reviews.windowed    zap.Logger

	tw := aggregator.NewTumblingWindow(*windowDur, *watermark, 5000)

	// Forward: collector → aggregator
	go func() {
		for review := range col.Reviews() {
			tw.Input() <- review
		}
		tw.Stop()
	}()

	// ========================================================================
	// 4. NATS JetStream — публикатор агрегированных окон
	// ========================================================================
	logger.Info("CONNECTING_TO_NATS",
		zap.String("url", *natsURL),
		zap.String("stream", natspub.StreamName),
		zap.String("subject", natspub.WindowedSubject),
	)

	publisher, err := natspub.NewJetStreamPublisher(ctx, *natsURL, logger)
	if err != nil {
		log.Fatalf("FAILED to create NATS publisher: %v", err)
	}
	defer func() {
		logger.Info("NATS_PUBLISHER_CLOSE")
		pub, failed, pending := publisher.Stats()
		logger.Info("NATS_FINAL_STATS",
			zap.Int64("windowed_published", pub),
			zap.Int64("failed", failed),
			zap.Int64("pending", pending),
		)
		if err := publisher.Close(); err != nil {
			logger.Error("NATS_PUBLISHER_CLOSE_ERROR", zap.Error(err))
		}
	}()

	// Читает WindowAgg из агрегатора, публикует в NATS + логирует.
	go func() {
		for agg := range tw.Output() {
			if err := publisher.PublishWindowAgg(ctx, agg); err != nil {
				logger.Error("PUBLISH_WINDOW_FAILED",
					zap.String("product", agg.ProductID),
					zap.Error(err),
				)
			}

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
		zap.String("nats_subject", natspub.WindowedSubject),
	)

	// ========================================================================
	// 5. Режим crash-after
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
	// 6. Ожидание сигнала → graceful shutdown
	// ========================================================================
	sig := <-sigCh
	logger.Info("SIGNAL_RECEIVED", zap.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	logger.Info("GRACEFUL_SHUTDOWN_STARTING")

	showAssignments(shutdownCtx, logger, coord, "before release")

	col.Stop()
	<-tw.Flushed()

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
