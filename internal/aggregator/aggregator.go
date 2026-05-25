package aggregator

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/models"
	"go.uber.org/zap"
)

// ==========================================================================
// Внутренние типы
// ==========================================================================

// productAccumulator накапливает статистику одного product_id внутри окна.
type productAccumulator struct {
	count      int
	sumRating  int
	totalLikes int
}

// windowEntry хранит состояние одного tumbling-окна.
type windowEntry struct {
	mu          sync.RWMutex
	start       time.Time
	buckets     map[string]*productAccumulator
	flushed     bool   // true после первой отправки агрегата в output
	hasLateData bool   // true — были поздние события после флаша
}

// copyBuckets делает глубокую копию buckets для отправки.
func (we *windowEntry) copyBuckets() map[string]*productAccumulator {
	cp := make(map[string]*productAccumulator, len(we.buckets))
	for pid, acc := range we.buckets {
		cp[pid] = &productAccumulator{
			count:      acc.count,
			sumRating:  acc.sumRating,
			totalLikes: acc.totalLikes,
		}
	}
	return cp
}

// windowStore — хеш-таблица окон, защищённая мьютексом.
type windowStore struct {
	mu      sync.Mutex
	windows map[int64]*windowEntry // key = windowStart.UnixNano()
}

func newWindowStore() *windowStore {
	return &windowStore{windows: make(map[int64]*windowEntry)}
}

func (ws *windowStore) getOrCreate(windowKey int64, start time.Time) *windowEntry {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	entry, exists := ws.windows[windowKey]
	if !exists {
		entry = &windowEntry{
			start:   start,
			buckets: make(map[string]*productAccumulator),
		}
		ws.windows[windowKey] = entry
	}
	return entry
}

func (ws *windowStore) get(windowKey int64) *windowEntry {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.windows[windowKey]
}

// remove удаляет окно (очистка старых).
func (ws *windowStore) remove(windowKey int64) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	delete(ws.windows, windowKey)
}

// snapshotAll возвращает копию всех незакрытых entry для проверки флаша.
func (ws *windowStore) snapshotUnflushed() []*windowEntry {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	var result []*windowEntry
	for _, entry := range ws.windows {
		entry.mu.RLock()
		if !entry.flushed {
			// Нужно скопировать метаданные для принятия решения
			cp := &windowEntry{
				start:   entry.start,
				flushed: entry.flushed,
			}
			result = append(result, cp)
		}
		entry.mu.RUnlock()
	}
	return result
}

// count возвращает количество окон в хранилище.
func (ws *windowStore) count() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return len(ws.windows)
}

// ==========================================================================
// TumblingWindow
// ==========================================================================

// TumblingWindow реализует оконную агрегацию с поддержкой поздних событий.
//
//   - Каждый отзыв маршрутизируется в окно на основе review.Date (event time).
//   - Окна хранятся в хеш-таблице windowStore с мьютексом.
//   - Когда окно закрывается (windowStart + window <= now), оно флашится.
//   - После флаша окно остаётся в памяти на время watermark.
//   - Поздние события (пришедшие после флаша) вызывают пересчёт и повторную
//     отправку агрегата с флагом IsUpdate=true.
//   - Окна старше watermark удаляются.
type TumblingWindow struct {
	window    time.Duration
	watermark time.Duration // максимальная задержка для поздних событий

	input  chan models.Review
	output chan models.WindowAgg

	store *windowStore

	logger *zap.Logger

	stopCh  chan struct{}
	flushed chan struct{}
}

// NewTumblingWindow создаёт агрегатор с tumbling window.
//
// window — длительность окна (1m, 30s, ...)
// watermark — в течение этого времени после закрытия окна принимаются поздние события
// buf — размер буфера каналов ввода/вывода
func NewTumblingWindow(window, watermark time.Duration, buf int) *TumblingWindow {
	logger, _ := zap.NewProduction()
	return &TumblingWindow{
		window:    window,
		watermark: watermark,
		input:     make(chan models.Review, buf),
		output:    make(chan models.WindowAgg, buf),
		store:     newWindowStore(),
		logger:    logger,
		stopCh:    make(chan struct{}),
		flushed:   make(chan struct{}),
	}
}

func (tw *TumblingWindow) Input() chan<- models.Review {
	return tw.input
}

func (tw *TumblingWindow) Output() <-chan models.WindowAgg {
	return tw.output
}

// Run запускает главный цикл.
func (tw *TumblingWindow) Run(ctx context.Context) {
	tw.logger.Info("TUMBLING_WINDOW_START",
		zap.Duration("window", tw.window),
		zap.Duration("watermark", tw.watermark),
	)

	reapTicker := time.NewTicker(tw.window)
	defer reapTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			tw.flushAllAndClose()
			return

		case <-tw.stopCh:
			tw.flushAllAndClose()
			return

		case review, ok := <-tw.input:
			if !ok {
				tw.flushAllAndClose()
				return
			}
			tw.handleReview(review)

		case <-reapTicker.C:
			tw.reapWindows()
		}
	}
}

// handleReview маршрутизирует отзыв в нужное окно по review.Date (event time).
func (tw *TumblingWindow) handleReview(review models.Review) {
	// Определяем окно по event time.
	windowStart := review.Date.Truncate(tw.window)
	windowKey := windowStart.UnixNano()
	now := time.Now()

	// Слишком старое событие — за пределами watermark.
	if now.Sub(windowStart) > tw.watermark+tw.window {
		tw.logger.Warn("REVIEW_TOO_OLD_DROPPED",
			zap.String("review_id", review.ID),
			zap.String("product", review.ProductID),
			zap.Time("review_date", review.Date),
			zap.Int64("age_sec", int64(now.Sub(review.Date).Seconds())),
			zap.Time("window_start", windowStart),
		)
		return
	}

	// Получаем или создаём entry.
	entry := tw.store.getOrCreate(windowKey, windowStart)

	entry.mu.Lock()

	acc, exists := entry.buckets[review.ProductID]
	if !exists {
		acc = &productAccumulator{}
		entry.buckets[review.ProductID] = acc
	}
	acc.count++
	acc.sumRating += review.Rating
	acc.totalLikes += review.Likes

	// Если окно уже было закрыто — это позднее событие.
	isLate := entry.flushed
	if isLate {
		entry.hasLateData = true
	}

	// Копируем текущие данные под lock'ом для пересчёта.
	needRecompute := isLate
	bucketsCopy := entry.copyBuckets()
	entry.mu.Unlock()

	if !needRecompute {
		// Нормальный path: окно ещё не флашено.
		return
	}

	// Позднее событие: пересчитываем и отправляем обновление.
	tw.logger.Info("LATE_EVENT_RECOMPUTE",
		zap.String("review_id", review.ID),
		zap.String("product", review.ProductID),
		zap.Time("window_start", windowStart),
		zap.String("action", "recomputing_and_emitting_update"),
	)

	tw.emitWindows(windowStart, bucketsCopy, true)
}

// reapWindows проверяет все окна: флашит закрытые, удаляет слишком старые.
func (tw *TumblingWindow) reapWindows() {
	now := time.Now()
	watermarkThreshold := now.Add(-tw.watermark)

	tw.store.mu.Lock()
	toFlush := make([]*windowEntry, 0)
	toRemove := make([]int64, 0)

	for key, entry := range tw.store.windows {
		entry.mu.Lock()

		// Закрытое и не флашенное окно.
		if entry.start.Add(tw.window).Before(now) && !entry.flushed {
			toFlush = append(toFlush, entry)
			// НЕ отпускаем lock — flusher возьмёт на себя.
			continue // lock остаётся захваченным
		}

		// Слишком старое окно — удаляем.
		if entry.start.Before(watermarkThreshold) {
			entry.mu.Unlock()
			toRemove = append(toRemove, key)
			continue
		}

		entry.mu.Unlock()
	}

	// Удаляем старые.
	for _, key := range toRemove {
		delete(tw.store.windows, key)
		tw.logger.Debug("WINDOW_EVICTED",
			zap.Int64("window_key", key),
			zap.String("reason", "older_than_watermark"),
		)
	}

	tw.store.mu.Unlock()

	// Флашим закрытые окна (lock уже захвачен, отпустим после).
	for _, entry := range toFlush {
		bucketsCopy := entry.copyBuckets()
		entry.flushed = true
		start := entry.start
		entry.mu.Unlock()

		tw.emitWindows(start, bucketsCopy, false)
		tw.logger.Info("WINDOW_FLUSHED",
			zap.Time("window_start", start),
			zap.Int("products", len(bucketsCopy)),
		)
	}
}

// emitWindows формирует и отправляет WindowAgg для каждого product_id.
func (tw *TumblingWindow) emitWindows(windowStart time.Time, buckets map[string]*productAccumulator, isUpdate bool) {
	if len(buckets) == 0 {
		return
	}

	tag := "WINDOW_EMIT"
	if isUpdate {
		tag = "WINDOW_UPDATE"
	}

	count := 0
	for productID, acc := range buckets {
		agg := models.WindowAgg{
			ProductID:   productID,
			WindowStart: windowStart,
			AvgRating:   round2(float64(acc.sumRating) / float64(acc.count)),
			TotalLikes:  acc.totalLikes,
			ReviewCount: acc.count,
			IsUpdate:    isUpdate,
		}

		tw.logger.Info(tag,
			zap.String("product", productID),
			zap.Time("window", windowStart),
			zap.Float64("avg_rating", agg.AvgRating),
			zap.Int("total_likes", agg.TotalLikes),
			zap.Int("review_count", agg.ReviewCount),
			zap.Bool("is_update", isUpdate),
		)

		select {
		case tw.output <- agg:
		default:
			tw.logger.Warn("WINDOW_OUTPUT_FULL",
				zap.String("product", productID),
				zap.Bool("is_update", isUpdate),
			)
		}
		count++
	}

	tw.logger.Info("WINDOW_BATCH_COMPLETED",
		zap.Time("window_start", windowStart),
		zap.Int("products", count),
		zap.Bool("is_update", isUpdate),
	)
}

// flushAllAndClose флашит все незакрытые окна и завершает работу.
func (tw *TumblingWindow) flushAllAndClose() {
	tw.logger.Info("TUMBLING_WINDOW_FLUSHING_ALL",
		zap.Int("open_windows", tw.store.count()),
	)

	tw.store.mu.Lock()
	for key, entry := range tw.store.windows {
		entry.mu.Lock()
		if !entry.flushed {
			bucketsCopy := entry.copyBuckets()
			entry.flushed = true
			start := entry.start
			entry.mu.Unlock()

			tw.emitWindows(start, bucketsCopy, false)
		} else {
			entry.mu.Unlock()
		}
		_ = key
	}
	tw.store.mu.Unlock()

	close(tw.output)
	close(tw.flushed)
}

func (tw *TumblingWindow) Flushed() <-chan struct{} {
	return tw.flushed
}

func (tw *TumblingWindow) Stop() {
	select {
	case <-tw.stopCh:
	default:
		close(tw.stopCh)
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
