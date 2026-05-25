package arrowserver

import (
	"testing"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/models"
)

func TestWindowSchema(t *testing.T) {
	schema := WindowSchema()
	if schema == nil {
		t.Fatal("WindowSchema returned nil")
	}

	if schema.NumFields() != 5 {
		t.Errorf("expected 5 fields, got %d", schema.NumFields())
	}

	fieldNames := make(map[string]bool)
	for _, f := range schema.Fields() {
		fieldNames[f.Name] = true
	}

	expected := []string{"product_id", "window_start", "avg_rating", "total_likes", "review_count"}
	for _, name := range expected {
		if !fieldNames[name] {
			t.Errorf("missing field: %s", name)
		}
	}
}

func TestEncodeTicketAll(t *testing.T) {
	data := EncodeTicket("all", "")
	if len(data) == 0 {
		t.Fatal("EncodeTicket returned empty")
	}

	qt, err := ParseTicket(data)
	if err != nil {
		t.Fatalf("ParseTicket failed: %v", err)
	}

	if qt.Cmd != "all" {
		t.Errorf("Cmd = %q, want %q", qt.Cmd, "all")
	}
	if qt.ProductID != "" {
		t.Errorf("ProductID = %q, want empty", qt.ProductID)
	}
}

func TestEncodeTicketFilter(t *testing.T) {
	data := EncodeTicket("filter", "WB-001")
	if len(data) == 0 {
		t.Fatal("EncodeTicket returned empty")
	}

	qt, err := ParseTicket(data)
	if err != nil {
		t.Fatalf("ParseTicket failed: %v", err)
	}

	if qt.Cmd != "filter" {
		t.Errorf("Cmd = %q, want %q", qt.Cmd, "filter")
	}
	if qt.ProductID != "WB-001" {
		t.Errorf("ProductID = %q, want %q", qt.ProductID, "WB-001")
	}
}

func TestParseTicketInvalidJSON(t *testing.T) {
	_, err := ParseTicket([]byte("not-json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseTicketEmpty(t *testing.T) {
	_, err := ParseTicket([]byte{})
	if err == nil {
		t.Error("expected error for empty ticket")
	}
}

func TestNewWindowStore(t *testing.T) {
	store := NewWindowStore(1000)
	if store == nil {
		t.Fatal("NewWindowStore returned nil")
	}
	if store.Count() != 0 {
		t.Errorf("initial count = %d, want 0", store.Count())
	}
}

func TestWindowStorePushAndSnapshot(t *testing.T) {
	store := NewWindowStore(100)

	agg := models.WindowAgg{
		ProductID:   "WB-001",
		WindowStart: time.Now(),
		AvgRating:   4.5,
		TotalLikes:  10,
		ReviewCount: 2,
	}

	store.Push(agg)
	if store.Count() != 1 {
		t.Errorf("count = %d, want 1", store.Count())
	}

	snapshot := store.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("snapshot length = %d, want 1", len(snapshot))
	}
	if snapshot[0].ProductID != "WB-001" {
		t.Errorf("ProductID = %q, want %q", snapshot[0].ProductID, "WB-001")
	}
}

func TestWindowStoreSnapshotFiltered(t *testing.T) {
	store := NewWindowStore(100)
	now := time.Now()

	store.Push(models.WindowAgg{ProductID: "WB-001", WindowStart: now, AvgRating: 4.0})
	store.Push(models.WindowAgg{ProductID: "WB-002", WindowStart: now, AvgRating: 3.5})
	store.Push(models.WindowAgg{ProductID: "WB-001", WindowStart: now.Add(time.Minute), AvgRating: 5.0})

	// Фильтр по конкретному product_id
	filtered := store.SnapshotFiltered("WB-001")
	if len(filtered) != 2 {
		t.Errorf("filtered WB-001 count = %d, want 2", len(filtered))
	}

	// Все записи
	all := store.SnapshotFiltered("")
	if len(all) != 3 {
		t.Errorf("all count = %d, want 3", len(all))
	}

	// Фильтр по несуществующему
	none := store.SnapshotFiltered("NONEXISTENT")
	if len(none) != 0 {
		t.Errorf("non-existent filter count = %d, want 0", len(none))
	}
}

func TestWindowStoreMaxLimit(t *testing.T) {
	store := NewWindowStore(5)

	for i := 0; i < 10; i++ {
		store.Push(models.WindowAgg{
			ProductID:   "WB-001",
			WindowStart: time.Now(),
			AvgRating:   float64(i),
		})
	}

	snapshot := store.Snapshot()
	if len(snapshot) > 5 {
		t.Errorf("snapshot length = %d, want <= 5", len(snapshot))
	}

	// Проверяем, что остались последние 5
	if len(snapshot) == 5 {
		lastRating := snapshot[len(snapshot)-1].AvgRating
		if lastRating != 9.0 {
			t.Errorf("last rating = %f, want 9.0", lastRating)
		}
	}
}

func TestWindowStoreConcurrency(t *testing.T) {
	store := NewWindowStore(1000)

	// Параллельные Push
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				store.Push(models.WindowAgg{
					ProductID:   "WB-001",
					WindowStart: time.Now(),
				})
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if store.Count() != 1000 {
		t.Errorf("count = %d, want 1000", store.Count())
	}
}

func TestSnapshotFilteredByPrefix(t *testing.T) {
	store := NewWindowStore(100)
	now := time.Now()

	store.Push(models.WindowAgg{ProductID: "WB-001", WindowStart: now})
	store.Push(models.WindowAgg{ProductID: "WB-002", WindowStart: now})
	store.Push(models.WindowAgg{ProductID: "OZ-101", WindowStart: now})

	// Проверяем, что фильтр не работает по префиксу (должно быть точное совпадение или HasPrefix)
	wbFiltered := store.SnapshotFiltered("WB-001")
	if len(wbFiltered) != 1 {
		t.Errorf("WB-001 count = %d, want 1", len(wbFiltered))
	}

	// HasPrefix
	wbAll := store.SnapshotFiltered("WB-")
	if len(wbAll) != 2 {
		t.Errorf("WB- count = %d, want 2", len(wbAll))
	}
}
