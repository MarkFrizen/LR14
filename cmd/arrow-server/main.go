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
	"github.com/markfriz/wb-ozon-review-collector/internal/arrowserver"
	"github.com/markfriz/wb-ozon-review-collector/internal/collector"
	"github.com/markfriz/wb-ozon-review-collector/internal/coordinator"
	"github.com/markfriz/wb-ozon-review-collector/internal/marketplace"
	"github.com/markfriz/wb-ozon-review-collector/internal/natspub"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func main() {
	var (
		etcdEndpoints = flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
		natsURL       = flag.String("nats", "nats://localhost:4222", "NATS server URL")
		workerID      = flag.String("worker", defaultWorkerID(), "unique worker ID")
		source        = flag.String("source", "wildberries", "marketplace source (wildberries/ozon)")
		windowDur     = flag.Duration("window", 1*time.Minute, "tumbling window duration")
		watermark     = flag.Duration("watermark", 2*time.Minute, "watermark for late events")
		arrowAddr     = flag.String("arrow-addr", "localhost:50051", "Arrow Flight server address")
		storeLimit    = flag.Int("store-limit", 10000, "max WindowAgg records in memory")
		crashAfter    = flag.Duration("crash-after", 0, "simulate crash after duration")
		demoMode      = flag.Bool("demo", false, "demo mode")
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
	logger.Info("CONNECTING_TO_ETCD", zap.Strings("endpoints", endpoints))

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
	defer coord.Close(context.Background())

	allProducts := []string{
		"WB-001", "WB-002", "WB-003", "WB-004", "WB-005",
		"OZ-101", "OZ-102", "OZ-103",
	}
	if err := coord.BootstrapProducts(ctx, allProducts); err != nil {
		log.Fatalf("FAILED to bootstrap products: %v", err)
	}

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
	// 3. Arrow Flight Store — сюда складываем WindowAgg для Flight-сервера
	// ========================================================================
	store := arrowserver.NewWindowStore(*storeLimit)

	// ========================================================================
	// 4. Оконная агрегация + отправка в NATS и Arrow Store
	// ========================================================================
	// Пайплайн:
	//   collector → aggregator → WindowAgg
	//                              ├── NATS (reviews.windowed)
	//                              └── Arrow Store (in-memory для Flight)

	tw := aggregator.NewTumblingWindow(*windowDur, *watermark, 5000)

	go func() {
		for review := range col.Reviews() {
			tw.Input() <- review
		}
		tw.Stop()
	}()

	// NATS publisher — публикует окна для Python-аналитика.
	logger.Info("CONNECTING_TO_NATS", zap.String("url", *natsURL))
	nc, js, err := natspub.NewNATSClient(ctx, *natsURL, logger)
	if err != nil {
		log.Fatalf("FAILED to create NATS client: %v", err)
	}
	publisher, err := natspub.NewJetStreamPublisher(ctx, nc, js, logger)
	if err != nil {
		log.Fatalf("FAILED to create NATS publisher: %v", err)
	}
	defer func() {
		pub, failed, pending := publisher.Stats()
		logger.Info("NATS_FINAL_STATS",
			zap.Int64("published", pub),
			zap.Int64("failed", failed),
			zap.Int64("pending", pending),
		)
		publisher.Close()
	}()

	// Горутина: читает WindowAgg → NATS + Arrow Store.
	go func() {
		for agg := range tw.Output() {
			// 1. NATS для Python-аналитика.
			if err := publisher.PublishWindowAgg(ctx, agg); err != nil {
				logger.Error("PUBLISH_WINDOW_FAILED", zap.String("product", agg.ProductID), zap.Error(err))
			}

			// 2. Arrow Store для Flight-сервера.
			store.Push(agg)

			if agg.IsUpdate {
				logger.Info("WINDOW_UPDATE",
					zap.String("product", agg.ProductID),
					zap.Time("window_start", agg.WindowStart),
					zap.Float64("avg_rating", agg.AvgRating),
					zap.Int("review_count", agg.ReviewCount),
				)
			} else {
				logger.Info("WINDOW_AGG",
					zap.String("product", agg.ProductID),
					zap.Int("review_count", agg.ReviewCount),
				)
			}
		}
	}()

	go tw.Run(ctx)

	// ========================================================================
	// 5. Arrow Flight RPC-сервер
	// ========================================================================
	flightSrv := arrowserver.NewFlightServer(store, logger)

	go func() {
		if err := arrowserver.Run(ctx, *arrowAddr, flightSrv, logger); err != nil {
			logger.Error("FLIGHT_SERVER_ERROR", zap.Error(err))
		}
	}()

	logger.Info("PIPELINE_STARTED",
		zap.String("worker", *workerID),
		zap.String("source", *source),
		zap.Duration("window", *windowDur),
		zap.Duration("watermark", *watermark),
		zap.String("flight_addr", *arrowAddr),
	)

	// ========================================================================
	// 6. crash-after
	// ========================================================================
	if *crashAfter > 0 {
		go func() {
			select {
			case <-time.After(*crashAfter):
				logger.Error("CRASH_SIMULATED")
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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	logger.Info("GRACEFUL_SHUTDOWN_STARTING")
	logger.Info("ARROW_STORE_STATS",
		zap.Int("records", store.Count()),
	)

	col.Stop()
	<-tw.Flushed()

	if err := coord.ReleaseAll(shutdownCtx); err != nil {
		logger.Error("RELEASE_ALL_ERROR", zap.Error(err))
	}

	cancel()
	<-shutdownCtx.Done()

	logger.Info("COLLECTOR_EXITED")
}

func defaultWorkerID() string {
	hostname, _ := os.Hostname()
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
