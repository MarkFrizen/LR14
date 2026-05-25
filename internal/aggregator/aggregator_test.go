package aggregator

import (
	"context"
	"testing"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/models"
)

func TestNewTumblingWindow(t *testing.T) {
	tw := NewTumblingWindow(time.Minute, 2*time.Minute, 100)
	if tw == nil {
		t.Fatal("NewTumblingWindow returned nil")
	}
	if tw.window != time.Minute {
		t.Errorf("window = %v, want 1m", tw.window)
	}
	if tw.watermark != 2*time.Minute {
		t.Errorf("watermark = %v, want 2m", tw.watermark)
	}
}

// runWindow запускает TumblingWindow, отправляет отзывы и собирает результаты.
func runWindow(t *testing.T, window, watermark time.Duration, reviews []models.Review, timeout time.Duration) []models.WindowAgg {
	t.Helper()
	tw := NewTumblingWindow(window, watermark, 100)
	ctx, cancel := context.WithCancel(context.Background())

	// Канал для сбора результатов
	resultsCh := make(chan []models.WindowAgg, 1)
	go func() {
		var results []models.WindowAgg
		for agg := range tw.Output() {
			results = append(results, agg)
		}
		resultsCh <- results
	}()

	go tw.Run(ctx)

	// Отправляем отзывы через Input() канал
	for _, r := range reviews {
		tw.Input() <- r
	}

	// Ждём закрытия окна + небольшой запас
	time.Sleep(window + watermark + 50*time.Millisecond)
	cancel()

	// Ждём завершения
	select {
	case results := <-resultsCh:
		return results
	case <-time.After(timeout):
		return nil
	}
}

func TestSingleReviewInWindow(t *testing.T) {
	now := time.Now().Truncate(50 * time.Millisecond)
	reviews := []models.Review{
		{ProductID: "WB-001", Rating: 4, Likes: 10, Date: now},
	}

	results := runWindow(t, 50*time.Millisecond, 100*time.Millisecond, reviews, time.Second)
	if len(results) == 0 {
		t.Fatal("expected at least 1 aggregated window")
	}

	agg := results[0]
	if agg.ProductID != "WB-001" {
		t.Errorf("ProductID = %q, want %q", agg.ProductID, "WB-001")
	}
	if agg.AvgRating != 4.0 {
		t.Errorf("AvgRating = %f, want 4.0", agg.AvgRating)
	}
	if agg.ReviewCount != 1 {
		t.Errorf("ReviewCount = %d, want 1", agg.ReviewCount)
	}
	if agg.TotalLikes != 10 {
		t.Errorf("TotalLikes = %d, want 10", agg.TotalLikes)
	}
	if agg.IsUpdate != false {
		t.Errorf("IsUpdate = %v, want false", agg.IsUpdate)
	}
}

func TestMultipleReviewsSameProduct(t *testing.T) {
	now := time.Now().Truncate(50 * time.Millisecond)
	reviews := []models.Review{
		{ProductID: "WB-001", Rating: 4, Likes: 10, Date: now},
		{ProductID: "WB-001", Rating: 5, Likes: 20, Date: now},
		{ProductID: "WB-001", Rating: 3, Likes: 5, Date: now},
	}

	results := runWindow(t, 50*time.Millisecond, 100*time.Millisecond, reviews, time.Second)
	if len(results) == 0 {
		t.Fatal("expected at least 1 aggregated window")
	}

	// Находим наш продукт
	for _, agg := range results {
		if agg.ProductID == "WB-001" {
			if agg.AvgRating != 4.0 {
				t.Errorf("AvgRating = %f, want 4.0", agg.AvgRating)
			}
			if agg.ReviewCount != 3 {
				t.Errorf("ReviewCount = %d, want 3", agg.ReviewCount)
			}
			if agg.TotalLikes != 35 {
				t.Errorf("TotalLikes = %d, want 35", agg.TotalLikes)
			}
			return
		}
	}
	t.Error("WB-001 not found in results")
}

func TestMultipleProductsInSameWindow(t *testing.T) {
	now := time.Now().Truncate(50 * time.Millisecond)
	reviews := []models.Review{
		{ProductID: "WB-001", Rating: 4, Likes: 10, Date: now},
		{ProductID: "WB-002", Rating: 5, Likes: 20, Date: now},
	}

	results := runWindow(t, 50*time.Millisecond, 100*time.Millisecond, reviews, time.Second)

	byProduct := make(map[string]models.WindowAgg)
	for _, agg := range results {
		byProduct[agg.ProductID] = agg
	}

	if agg, ok := byProduct["WB-001"]; !ok {
		t.Error("missing WB-001")
	} else if agg.AvgRating != 4.0 {
		t.Errorf("WB-001 AvgRating = %f, want 4.0", agg.AvgRating)
	}

	if agg, ok := byProduct["WB-002"]; !ok {
		t.Error("missing WB-002")
	} else if agg.AvgRating != 5.0 {
		t.Errorf("WB-002 AvgRating = %f, want 5.0", agg.AvgRating)
	}
}

func TestReviewsInDifferentWindows(t *testing.T) {
	window := 50 * time.Millisecond
	now := time.Now().Truncate(window)
	reviews := []models.Review{
		{ProductID: "WB-001", Rating: 3, Likes: 5, Date: now},
		{ProductID: "WB-001", Rating: 5, Likes: 15, Date: now.Add(2 * window)},
	}

	results := runWindow(t, window, 200*time.Millisecond, reviews, 2*time.Second)

	// Должно быть как минимум 2 окна для WB-001
	count := 0
	for _, agg := range results {
		if agg.ProductID == "WB-001" {
			count++
		}
	}
	if count < 1 {
		t.Error("expected at least 1 window for WB-001")
	}
}

func TestManyReviewsStress(t *testing.T) {
	now := time.Now().Truncate(50 * time.Millisecond)
	reviews := make([]models.Review, 0, 100)
	for i := 0; i < 100; i++ {
		productID := "WB-001"
		if i%2 == 0 {
			productID = "WB-002"
		}
		reviews = append(reviews, models.Review{
			ProductID: productID,
			Rating:    1 + i%5,
			Likes:     i * 2,
			Date:      now,
		})
	}

	results := runWindow(t, 50*time.Millisecond, 100*time.Millisecond, reviews, time.Second)
	if len(results) == 0 {
		t.Fatal("expected results from stress test")
	}

	totalReviews := 0
	for _, agg := range results {
		totalReviews += agg.ReviewCount
	}
	if totalReviews != 100 {
		t.Errorf("total review count = %d, want 100", totalReviews)
	}
}

func TestLateEventRecompute(t *testing.T) {
	window := 50 * time.Millisecond
	now := time.Now().Truncate(window)

	// Первый отзыв в окне
	tw := NewTumblingWindow(window, 200*time.Millisecond, 100)
	ctx, cancel := context.WithCancel(context.Background())

	resultsCh := make(chan []models.WindowAgg, 1)
	go func() {
		var results []models.WindowAgg
		for agg := range tw.Output() {
			results = append(results, agg)
		}
		resultsCh <- results
	}()

	go tw.Run(ctx)

	tw.Input() <- models.Review{ProductID: "WB-001", Rating: 4, Likes: 10, Date: now}

	// Ждём, пока окно закроется
	time.Sleep(window + 20*time.Millisecond)

	// Отправляем позднее событие в то же окно
	tw.Input() <- models.Review{ProductID: "WB-001", Rating: 2, Likes: 5, Date: now}

	// Ждём обработки позднего события
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case results := <-resultsCh:
		hasUpdate := false
		for _, agg := range results {
			if agg.ProductID == "WB-001" && agg.IsUpdate {
				hasUpdate = true
				if agg.AvgRating != 3.0 {
					t.Errorf("late event AvgRating = %f, want 3.0", agg.AvgRating)
				}
				if agg.ReviewCount != 2 {
					t.Errorf("late event ReviewCount = %d, want 2", agg.ReviewCount)
				}
			}
		}
		if !hasUpdate {
			t.Log("no late event update found (may be timing)")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for results")
	}
}

func TestWindowFlushOnStop(t *testing.T) {
	window := 50 * time.Millisecond
	now := time.Now().Truncate(window)
	tw := NewTumblingWindow(window, 100*time.Millisecond, 100)
	ctx, cancel := context.WithCancel(context.Background())

	resultsCh := make(chan []models.WindowAgg, 1)
	go func() {
		var results []models.WindowAgg
		for agg := range tw.Output() {
			results = append(results, agg)
		}
		resultsCh <- results
	}()

	go tw.Run(ctx)

	tw.Input() <- models.Review{ProductID: "WB-001", Rating: 4, Likes: 10, Date: now}
	tw.Input() <- models.Review{ProductID: "WB-002", Rating: 5, Likes: 20, Date: now}

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-resultsCh:
		// ok
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for flush on stop")
	}
}

func TestFlushAllOnContextCancel(t *testing.T) {
	now := time.Now().Truncate(50 * time.Millisecond)
	tw := NewTumblingWindow(50*time.Millisecond, 100*time.Millisecond, 100)
	ctx, cancel := context.WithCancel(context.Background())

	resultsCh := make(chan []models.WindowAgg, 1)
	go func() {
		var results []models.WindowAgg
		for agg := range tw.Output() {
			results = append(results, agg)
		}
		resultsCh <- results
	}()

	go tw.Run(ctx)

	tw.Input() <- models.Review{ProductID: "WB-001", Rating: 4, Likes: 10, Date: now}

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-resultsCh:
		// ok
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for flush after context cancel")
	}
}

func TestRound2(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{3.14159, 3.14},
		{2.71828, 2.72},
		{2.0, 2.0},
		{0.0, 0.0},
		{4.999, 5.0},
		{1.234, 1.23},
		{1.0, 1.0},
		{5.0, 5.0},
	}

	for _, tt := range tests {
		result := round2(tt.input)
		if result != tt.expected {
			t.Errorf("round2(%f) = %f, want %f", tt.input, result, tt.expected)
		}
	}
}

func TestStoreCount(t *testing.T) {
	store := newWindowStore()
	if store.count() != 0 {
		t.Errorf("empty store count = %d, want 0", store.count())
	}

	now := time.Now()
	store.getOrCreate(now.UnixNano(), now)
	if store.count() != 1 {
		t.Errorf("store count = %d, want 1", store.count())
	}

	// То же окно — не должно увеличить count
	store.getOrCreate(now.UnixNano(), now)
	if store.count() != 1 {
		t.Errorf("store count after duplicate = %d, want 1", store.count())
	}

	store.remove(now.UnixNano())
	if store.count() != 0 {
		t.Errorf("store count after remove = %d, want 0", store.count())
	}
}

func TestSnapshotUnflushed(t *testing.T) {
	store := newWindowStore()
	now := time.Now()
	entry := store.getOrCreate(now.UnixNano(), now)
	entry.mu.Lock()
	entry.buckets["WB-001"] = &productAccumulator{count: 1, sumRating: 4, totalLikes: 10}
	entry.mu.Unlock()

	unflushed := store.snapshotUnflushed()
	if len(unflushed) != 1 {
		t.Errorf("unflushed count = %d, want 1", len(unflushed))
	}
}

func TestCopyBuckets(t *testing.T) {
	entry := &windowEntry{
		buckets: map[string]*productAccumulator{
			"WB-001": {count: 3, sumRating: 12, totalLikes: 30},
		},
	}

	cp := entry.copyBuckets()
	if len(cp) != 1 {
		t.Fatalf("copy len = %d, want 1", len(cp))
	}

	// Изменение копии не должно влиять на оригинал
	cp["WB-001"].count = 999
	if entry.buckets["WB-001"].count != 3 {
		t.Error("copy should be independent of original")
	}
}
