package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	etcdKeyPrefix       = "/collector"
	productListPrefix   = etcdKeyPrefix + "/products/"
	assignmentPrefix    = etcdKeyPrefix + "/assignments/"
	LeaseTTL              = 10 // секунд
	ReclaimIntervalSeconds = 15 // секунд между проверками
	ownershipCheckEvery   = 5  // секунд между проверками своих шардов
)

// WorkerStatus представляет состояние воркера-обработчика одного шарда.
type WorkerStatus string

const (
	StatusIdle        WorkerStatus = "idle"
	StatusClaiming    WorkerStatus = "claiming"
	StatusCollecting  WorkerStatus = "collecting"
	StatusCompleted   WorkerStatus = "completed"
	StatusLost        WorkerStatus = "lost" // лизинг пропал — шард ушёл
)

// ShardInfo содержит информацию о состоянии шарда.
type ShardInfo struct {
	ProductID string       `json:"product_id"`
	WorkerID  string       `json:"worker_id"` // кто держит сейчас ("" — свободен)
	Status    WorkerStatus `json:"status"`
}

// Coordinator управляет распределением product_id между воркерами через etcd.
type Coordinator struct {
	cli      *clientv3.Client
	leaseID  clientv3.LeaseID
	workerID string
	logger   *zap.Logger

	mu             sync.RWMutex
	ownedProducts  map[string]WorkerStatus // product_id → статус обработки
	leasedProducts map[string]struct{}     // product_id, чьи лизинги держим

	stopKeepAlive chan struct{}
	keepAliveWg   sync.WaitGroup

	// Канал для уведомления сборщика о потерянных шардах (наш лизинг отвалился).
	lostProducts chan string
}

// NewCoordinator создаёт подключение к etcd, лизинг и запускает keepalive.
func NewCoordinator(ctx context.Context, endpoints []string, workerID string) (*Coordinator, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("new logger: %w", err)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      logger,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd client: %w", err)
	}

	// Создаём лизинг.
	resp, err := cli.Grant(ctx, LeaseTTL)
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("grant lease: %w", err)
	}
	leaseID := resp.ID

	logger.Info("LEASE_GRANTED",
		zap.String("worker", workerID),
		zap.Int64("lease_id", int64(leaseID)),
		zap.Int("ttl_sec", LeaseTTL),
	)

	c := &Coordinator{
		cli:            cli,
		leaseID:        leaseID,
		workerID:       workerID,
		logger:         logger,
		ownedProducts:  make(map[string]WorkerStatus),
		leasedProducts: make(map[string]struct{}),
		stopKeepAlive:  make(chan struct{}),
		lostProducts:   make(chan string, 100),
	}

	// keepalive в фоне.
	c.keepAliveWg.Add(1)
	go c.keepAliveLoop(ctx)

	return c, nil
}

// LostProducts возвращает канал, в который приходят product_id,
// чей лизинг был потерян (наш воркер перестал их обслуживать).
func (c *Coordinator) LostProducts() <-chan string {
	return c.lostProducts
}

// keepAliveLoop продлевает лизинг в фоне.
// Если канал keepalive закрылся — это фатально: наш лизинг умрёт.
func (c *Coordinator) keepAliveLoop(ctx context.Context) {
	defer c.keepAliveWg.Done()

	ch, err := c.cli.KeepAlive(ctx, c.leaseID)
	if err != nil {
		c.logger.Error("KEEPALIVE_START_FAILED",
			zap.String("worker", c.workerID),
			zap.Error(err),
		)
		return
	}

	c.logger.Info("KEEPALIVE_STARTED",
		zap.String("worker", c.workerID),
		zap.Int64("lease_id", int64(c.leaseID)),
	)

	for {
		select {
		case <-c.stopKeepAlive:
			c.logger.Info("KEEPALIVE_STOPPED",
				zap.String("worker", c.workerID),
			)
			return
		case _, ok := <-ch:
			if !ok {
				c.logger.Error("KEEPALIVE_CHANNEL_CLOSED",
					zap.String("worker", c.workerID),
					zap.Int64("lease_id", int64(c.leaseID)),
				)
				// Лизинг умрёт через TTL. Оповещаем о потерянных шардах.
				c.notifyLostAll()
				return
			}
		}
	}
}

// notifyLostAll помечает все наши шарды как потерянные.
func (c *Coordinator) notifyLostAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for pid := range c.ownedProducts {
		c.ownedProducts[pid] = StatusLost
		select {
		case c.lostProducts <- pid:
		default:
		}
	}
}

// BootstrapProducts загружает список product_id в etcd (однократно).
func (c *Coordinator) BootstrapProducts(ctx context.Context, products []string) error {
	for _, pid := range products {
		key := productListPrefix + pid
		txn := c.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, ""))
		if _, err := txn.Commit(); err != nil {
			return fmt.Errorf("bootstrap product %s: %w", pid, err)
		}
	}
	c.logger.Info("PRODUCTS_BOOTSTRAPPED",
		zap.String("worker", c.workerID),
		zap.Int("count", len(products)),
	)
	return nil
}

// ListAllProducts возвращает полный список product_id из etcd.
func (c *Coordinator) ListAllProducts(ctx context.Context) ([]string, error) {
	resp, err := c.cli.Get(ctx, productListPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list products: %w", err)
	}
	products := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if pid := extractSuffix(string(kv.Key), productListPrefix); pid != "" {
			products = append(products, pid)
		}
	}
	return products, nil
}

// ListAssignments возвращает информацию о текущих назначениях всех шардов.
func (c *Coordinator) ListAssignments(ctx context.Context) (map[string]string, error) {
	resp, err := c.cli.Get(ctx, assignmentPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list assignments: %w", err)
	}
	assignments := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if pid := extractSuffix(string(kv.Key), assignmentPrefix); pid != "" {
			assignments[pid] = string(kv.Value)
		}
	}
	return assignments, nil
}

// ClaimProducts пытается захватить незанятые product_id.
// Использует etcd-транзакцию: создаёт ключ назначения только если его нет.
func (c *Coordinator) ClaimProducts(ctx context.Context) ([]string, error) {
	// Получаем список всех product_id.
	prodResp, err := c.cli.Get(ctx, productListPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("get products: %w", err)
	}

	// Получаем текущие назначения.
	assignResp, err := c.cli.Get(ctx, assignmentPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("get assignments: %w", err)
	}

	// Составляем множество занятых product_id.
	taken := make(map[string]string, len(assignResp.Kvs))
	for _, kv := range assignResp.Kvs {
		if pid := extractSuffix(string(kv.Key), assignmentPrefix); pid != "" {
			taken[pid] = string(kv.Value)
		}
	}

	var claimed []string

	for _, kv := range prodResp.Kvs {
		pid := extractSuffix(string(kv.Key), productListPrefix)
		if pid == "" {
			continue
		}

		// Если уже назначен — пропускаем.
		if holder, exists := taken[pid]; exists {
			c.logger.Debug("PRODUCT_ALREADY_ASSIGNED",
				zap.String("product", pid),
				zap.String("holder", holder),
			)
			continue
		}

		assignKey := assignmentPrefix + pid

		// Транзакция: создаём ключ если его нет.
		txn := c.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(assignKey), "=", 0)).
			Then(clientv3.OpPut(assignKey, c.workerID, clientv3.WithLease(c.leaseID))).
			Else()

		txnResp, err := txn.Commit()
		if err != nil {
			c.logger.Warn("CLAIM_TXN_FAILED",
				zap.String("product", pid),
				zap.Error(err),
			)
			continue
		}

		if txnResp.Succeeded {
			c.mu.Lock()
			c.ownedProducts[pid] = StatusClaiming
			c.leasedProducts[pid] = struct{}{}
			c.mu.Unlock()

			claimed = append(claimed, pid)

			c.logger.Info("PRODUCT_CLAIMED",
				zap.String("product", pid),
				zap.String("worker", c.workerID),
				zap.Int64("lease_id", int64(c.leaseID)),
			)
		} else {
			// Кто-то успел перехватить между нашим Get и транзакцией.
			c.logger.Debug("CLAIM_TXN_RACE_LOST",
				zap.String("product", pid),
				zap.String("worker", c.workerID),
			)
		}
	}

	return claimed, nil
}

// TryClaimOne пытается захватить конкретный product_id (для перехвата освободившегося).
func (c *Coordinator) TryClaimOne(ctx context.Context, productID string) (bool, error) {
	assignKey := assignmentPrefix + productID

	txn := c.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(assignKey), "=", 0)).
		Then(clientv3.OpPut(assignKey, c.workerID, clientv3.WithLease(c.leaseID))).
		Else()

	txnResp, err := txn.Commit()
	if err != nil {
		return false, fmt.Errorf("claim one txn: %w", err)
	}

	if txnResp.Succeeded {
		c.mu.Lock()
		c.ownedProducts[productID] = StatusClaiming
		c.leasedProducts[productID] = struct{}{}
		c.mu.Unlock()

		c.logger.Info("PRODUCT_RECLAIMED",
			zap.String("product", productID),
			zap.String("worker", c.workerID),
			zap.Int64("lease_id", int64(c.leaseID)),
		)
		return true, nil
	}

	// Проверим, кто успел перехватить.
	for _, r := range txnResp.Responses {
		if r.GetResponseRange() != nil {
			for _, kv := range r.GetResponseRange().Kvs {
				c.logger.Debug("RECLAIM_RACE_LOST",
					zap.String("product", productID),
					zap.String("new_holder", string(kv.Value)),
				)
			}
		}
	}

	return false, nil
}

// MarkProductCollected обновляет статус шарда на "собран".
func (c *Coordinator) MarkProductCollected(productID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.ownedProducts[productID]; exists {
		c.ownedProducts[productID] = StatusCompleted
		c.logger.Info("PRODUCT_COLLECTED",
			zap.String("product", productID),
			zap.String("worker", c.workerID),
		)
	}
}

// WatchAssignmentChanges отслеживает удаление ключей назначения (воркер упал).
// Возвращает канал с освободившимися product_id.
func (c *Coordinator) WatchAssignmentChanges(ctx context.Context) (<-chan string, error) {
	ch := make(chan string, 100)

	watchCh := c.cli.Watch(ctx, assignmentPrefix, clientv3.WithPrefix())

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			case watchResp, ok := <-watchCh:
				if !ok {
					return
				}
				for _, ev := range watchResp.Events {
					if ev.Type != mvccpb.DELETE {
						continue
					}
					pid := extractSuffix(string(ev.Kv.Key), assignmentPrefix)
					if pid == "" {
						continue
					}

					// Пропускаем, если это наш же шард (сами освободили).
					c.mu.RLock()
					_, isOurs := c.leasedProducts[pid]
					c.mu.RUnlock()
					if isOurs {
						// Удаляем из нашего ownedProducts.
						c.mu.Lock()
						delete(c.ownedProducts, pid)
						delete(c.leasedProducts, pid)
						c.mu.Unlock()

						c.logger.Info("PRODUCT_RELEASED_BY_US",
							zap.String("product", pid),
							zap.String("worker", c.workerID),
						)
						continue
					}

					// Узнаем, кто был предыдущим владельцем.
					prevHolder := string(ev.PrevKv.Value)

					c.logger.Info("PRODUCT_FREED",
						zap.String("product", pid),
						zap.String("prev_holder", prevHolder),
					)

					select {
					case ch <- pid:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ch, nil
}

// VerifyOwnership проверяет, что все наши шарды всё ещё закреплены за нами.
// Те, что пропали (лизинг истёк), возвращает через LostProducts.
func (c *Coordinator) VerifyOwnership(ctx context.Context) {
	c.mu.RLock()
	owned := make([]string, 0, len(c.ownedProducts))
	for pid := range c.ownedProducts {
		owned = append(owned, pid)
	}
	c.mu.RUnlock()

	for _, pid := range owned {
		key := assignmentPrefix + pid
		resp, err := c.cli.Get(ctx, key)
		if err != nil {
			c.logger.Warn("OWNERSHIP_CHECK_FAILED",
				zap.String("product", pid),
				zap.Error(err),
			)
			continue
		}

		if len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != c.workerID {
			// Шард потерян — лизинг умер.
			c.mu.Lock()
			prevStatus := c.ownedProducts[pid]
			c.ownedProducts[pid] = StatusLost
			delete(c.leasedProducts, pid)
			c.mu.Unlock()

			c.logger.Warn("PRODUCT_LOST",
				zap.String("product", pid),
				zap.String("worker", c.workerID),
				zap.String("prev_status", string(prevStatus)),
				zap.Int64("lease_id", int64(c.leaseID)),
			)

			select {
			case c.lostProducts <- pid:
			default:
			}
		}
	}
}

// StartOwnershipChecker запускает фоновую проверку своих шардов.
func (c *Coordinator) StartOwnershipChecker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(ownershipCheckEvery * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.VerifyOwnership(ctx)
			}
		}
	}()
}

// ReleaseAll освобождает все захваченные product_id.
func (c *Coordinator) ReleaseAll(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for pid := range c.ownedProducts {
		key := assignmentPrefix + pid
		if _, err := c.cli.Delete(ctx, key); err != nil {
			c.logger.Error("RELEASE_FAILED",
				zap.String("product", pid),
				zap.String("worker", c.workerID),
				zap.Error(err),
			)
			continue
		}

		c.logger.Info("PRODUCT_RELEASED",
			zap.String("product", pid),
			zap.String("worker", c.workerID),
			zap.String("status", string(c.ownedProducts[pid])),
		)

		delete(c.ownedProducts, pid)
		delete(c.leasedProducts, pid)
	}

	return nil
}

// Close закрывает соединение с etcd и отзывает лизинг.
func (c *Coordinator) Close(ctx context.Context) error {
	close(c.stopKeepAlive)
	c.keepAliveWg.Wait()

	_, err := c.cli.Revoke(ctx, c.leaseID)
	if err != nil {
		c.logger.Error("LEASE_REVOKE_FAILED",
			zap.String("worker", c.workerID),
			zap.Error(err),
		)
	} else {
		c.logger.Info("LEASE_REVOKED",
			zap.String("worker", c.workerID),
			zap.Int64("lease_id", int64(c.leaseID)),
		)
	}

	return c.cli.Close()
}

// OwnedProducts возвращает копию списка захваченных product_id.
func (c *Coordinator) OwnedProducts() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ids := make([]string, 0, len(c.ownedProducts))
	for pid := range c.ownedProducts {
		ids = append(ids, pid)
	}
	return ids
}

// extractSuffix извлекает суффикс после известного префикса.
func extractSuffix(key, prefix string) string {
	if len(key) > len(prefix) && key[:len(prefix)] == prefix {
		return key[len(prefix):]
	}
	return ""
}
