package collector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/coordinator"
	"github.com/markfriz/wb-ozon-review-collector/internal/marketplace"
	"github.com/markfriz/wb-ozon-review-collector/internal/models"
	"go.uber.org/zap"
)

// workerState отслеживает состояние горутины, обрабатывающей один product_id.
type workerState struct {
	productID string
	startedAt time.Time
	done      chan struct{}
}

// Collector собирает отзывы с маркетплейса, распределяя шарды через etcd.
// Собранные отзывы отправляются в локальный канал Reviews() для последующей
// оконной агрегации. Сырые отзывы больше не публикуются в NATS напрямую.
type Collector struct {
	coord   *coordinator.Coordinator
	market  *marketplace.Simulator
	logger  *zap.Logger
	reviews chan models.Review

	mu      sync.Mutex
	workers map[string]*workerState

	stopCh chan struct{}
}

// New создаёт новый сборщик.
func New(coord *coordinator.Coordinator, market *marketplace.Simulator) *Collector {
	logger, _ := zap.NewProduction()
	return &Collector{
		coord:   coord,
		market:  market,
		logger:  logger,
		reviews: make(chan models.Review, 5000),
		workers: make(map[string]*workerState),
		stopCh:  make(chan struct{}),
	}
}

// Reviews возвращает канал с собранными отзывами (для агрегатора).
func (c *Collector) Reviews() <-chan models.Review {
	return c.reviews
}

// Run запускает главный цикл сборщика.
func (c *Collector) Run(ctx context.Context) error {
	c.logger.Info("COLLECTOR_STARTING")

	freedCh, err := c.coord.WatchAssignmentChanges(ctx)
	if err != nil {
		return fmt.Errorf("watch assignments: %w", err)
	}

	lostCh := c.coord.LostProducts()
	c.coord.StartOwnershipChecker(ctx)

	if err := c.claimAndSpawn(ctx); err != nil {
		c.logger.Warn("INITIAL_CLAIM_FAILED", zap.Error(err))
	}

	reclaimTicker := time.NewTicker(coordinator.ReclaimIntervalSeconds * time.Second)
	defer reclaimTicker.Stop()

	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("COLLECTOR_SHUTTING_DOWN")
			c.stopAllWorkers()
			return ctx.Err()

		case <-c.stopCh:
			c.logger.Info("COLLECTOR_STOPPED_VIA_SIGNAL")
			c.stopAllWorkers()
			return nil

		case pid, ok := <-freedCh:
			if !ok {
				continue
			}
			c.logger.Info("FREED_PRODUCT_DETECTED", zap.String("product", pid))
			c.tryReclaimOne(ctx, pid)

		case pid, ok := <-lostCh:
			if !ok {
				continue
			}
			c.logger.Warn("OWN_PRODUCT_LOST", zap.String("product", pid))
			c.removeWorker(pid)

		case <-reclaimTicker.C:
			claimed, err := c.coord.ClaimProducts(ctx)
			if err != nil {
				c.logger.Warn("PERIODIC_CLAIM_FAILED", zap.Error(err))
			}
			for _, pid := range claimed {
				c.spawnWorker(ctx, pid)
			}

		case <-statusTicker.C:
			c.printStatus()
		}
	}
}

func (c *Collector) tryReclaimOne(ctx context.Context, productID string) {
	ok, err := c.coord.TryClaimOne(ctx, productID)
	if err != nil {
		c.logger.Warn("RECLAIM_TXN_ERROR", zap.String("product", productID), zap.Error(err))
		return
	}
	if ok {
		c.logger.Info("RECLAIM_SUCCESS", zap.String("product", productID))
		c.spawnWorker(ctx, productID)
	} else {
		c.logger.Debug("RECLAIM_RACE_LOST", zap.String("product", productID))
	}
}

func (c *Collector) claimAndSpawn(ctx context.Context) error {
	claimed, err := c.coord.ClaimProducts(ctx)
	if err != nil {
		return err
	}
	for _, pid := range claimed {
		c.spawnWorker(ctx, pid)
	}
	return nil
}

// spawnWorker запускает горутину для сбора отзывов по product_id.
// Отзывы отправляются в канал Reviews() — агрегатор обработает их окнами.
func (c *Collector) spawnWorker(ctx context.Context, productID string) {
	c.mu.Lock()
	if _, exists := c.workers[productID]; exists {
		c.mu.Unlock()
		c.logger.Debug("WORKER_ALREADY_RUNNING", zap.String("product", productID))
		return
	}
	ws := &workerState{
		productID: productID,
		startedAt: time.Now(),
		done:      make(chan struct{}),
	}
	c.workers[productID] = ws
	c.mu.Unlock()

	go func() {
		defer func() {
			close(ws.done)
			c.removeWorker(productID)
		}()

		c.logger.Info("WORKER_SPAWNED", zap.String("product", productID))

		select {
		case <-ctx.Done():
			return
		default:
		}

		reviews, err := c.market.FetchReviews(ctx, productID, 10)
		if err != nil {
			c.logger.Warn("WORKER_FETCH_FAILED", zap.String("product", productID), zap.Error(err))
			return
		}

		for _, r := range reviews {
			select {
			case c.reviews <- r:
			case <-ctx.Done():
				return
			}
		}

		c.coord.MarkProductCollected(productID)

		c.logger.Info("WORKER_FINISHED",
			zap.String("product", productID),
			zap.Int("reviews", len(reviews)),
			zap.Duration("duration", time.Since(ws.startedAt)),
		)
	}()
}

func (c *Collector) removeWorker(productID string) {
	c.mu.Lock()
	delete(c.workers, productID)
	c.mu.Unlock()
}

func (c *Collector) stopAllWorkers() {
	c.mu.Lock()
	workers := make([]*workerState, 0, len(c.workers))
	for _, ws := range c.workers {
		workers = append(workers, ws)
	}
	c.mu.Unlock()

	if len(workers) > 0 {
		c.logger.Info("WAITING_FOR_WORKERS", zap.Int("count", len(workers)))
		for _, ws := range workers {
			<-ws.done
		}
	}

	close(c.reviews)
}

func (c *Collector) printStatus() {
	c.mu.Lock()
	defer c.mu.Unlock()
	owned := c.coord.OwnedProducts()
	c.logger.Info("STATUS",
		zap.Int("claimed_shards", len(owned)),
		zap.Int("active_workers", len(c.workers)),
		zap.Strings("owned_products", owned),
	)
}

func (c *Collector) Stop() {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}
