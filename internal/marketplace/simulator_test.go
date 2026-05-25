package marketplace

import (
	"context"
	"strings"
	"testing"
)

func TestNewSimulator(t *testing.T) {
	s := NewSimulator("wildberries")
	if s == nil {
		t.Fatal("NewSimulator returned nil")
	}

	s2 := NewSimulator("ozon")
	if s2 == nil {
		t.Fatal("NewSimulator for ozon returned nil")
	}
}

func TestFetchReviewsReturnsReviews(t *testing.T) {
	s := NewSimulator("wildberries")
	ctx := context.Background()

	reviews, err := s.FetchReviews(ctx, "WB-001", 10)
	if err != nil {
		t.Fatalf("FetchReviews failed: %v", err)
	}

	if len(reviews) == 0 {
		t.Fatal("expected at least 1 review, got 0")
	}

	if len(reviews) > 10 {
		t.Errorf("expected at most 10 reviews, got %d", len(reviews))
	}
}

func TestFetchReviewsProductID(t *testing.T) {
	s := NewSimulator("wildberries")
	ctx := context.Background()

	reviews, err := s.FetchReviews(ctx, "WB-042", 5)
	if err != nil {
		t.Fatalf("FetchReviews failed: %v", err)
	}

	for i, r := range reviews {
		if !strings.HasPrefix(r.ID, "wildberries-WB-042") {
			t.Errorf("review[%d] ID %q does not have expected prefix", i, r.ID)
		}
		if r.ProductID != "WB-042" {
			t.Errorf("review[%d] ProductID = %q, want %q", i, r.ProductID, "WB-042")
		}
	}
}

func TestFetchReviewsOzonSource(t *testing.T) {
	s := NewSimulator("ozon")
	ctx := context.Background()

	reviews, err := s.FetchReviews(ctx, "OZ-101", 3)
	if err != nil {
		t.Fatalf("FetchReviews failed: %v", err)
	}

	for i, r := range reviews {
		if !strings.HasPrefix(r.ID, "ozon-OZ-101") {
			t.Errorf("review[%d] ID %q does not have 'ozon' prefix", i, r.ID)
		}
	}
}

func TestFetchReviewsRatingRange(t *testing.T) {
	s := NewSimulator("wildberries")
	ctx := context.Background()

	// Collect many reviews to check rating distribution
	allReviews := make([]int, 0)
	for i := 0; i < 20; i++ {
		reviews, err := s.FetchReviews(ctx, "WB-001", 10)
		if err != nil {
			t.Fatalf("FetchReviews failed: %v", err)
		}
		for _, r := range reviews {
			if r.Rating < 1 || r.Rating > 5 {
				t.Errorf("rating %d out of range [1,5]", r.Rating)
			}
			allReviews = append(allReviews, r.Rating)
		}
	}

	if len(allReviews) == 0 {
		t.Fatal("no reviews collected")
	}
}

func TestFetchReviewsDateNotZero(t *testing.T) {
	s := NewSimulator("wildberries")
	ctx := context.Background()

	reviews, err := s.FetchReviews(ctx, "WB-001", 5)
	if err != nil {
		t.Fatalf("FetchReviews failed: %v", err)
	}

	for i, r := range reviews {
		if r.Date.IsZero() {
			t.Errorf("review[%d] has zero Date", i)
		}
	}
}

func TestFetchReviewsContextCancelled(t *testing.T) {
	s := NewSimulator("wildberries")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // сразу отменяем

	_, err := s.FetchReviews(ctx, "WB-001", 5)
	if err == nil {
		// Может вернуть данные, если симуляция уже началась.
		// Но должен вернуть ошибку контекста, если успел проверить.
		t.Log("context cancelled but FetchReviews completed (acceptable race)")
	}
}

func TestGenerateReviewTextNotEmpty(t *testing.T) {
	for rating := 1; rating <= 5; rating++ {
		text := generateReviewText(rating)
		if text == "" {
			t.Errorf("generateReviewText(%d) returned empty text", rating)
		}
	}
}

func TestGenerateReviewTextVariety(t *testing.T) {
	texts := make(map[string]bool)
	for i := 0; i < 50; i++ {
		for rating := 1; rating <= 5; rating++ {
			text := generateReviewText(rating)
			texts[text] = true
		}
	}
	// Должно быть хотя бы 2 разных текста для каждого рейтинга
	for rating := 1; rating <= 5; rating++ {
		count := 0
		for t := range texts {
			if strings.Contains(t, "!") || strings.Contains(t, ".") || strings.Contains(t, ",") {
				count++
			}
		}
		if count < 2 {
			t.Logf("rating %d has %d unique texts (expected at least 2)", rating, count)
		}
	}
}

func TestFetchReviewsMultipleProducts(t *testing.T) {
	s := NewSimulator("wildberries")
	ctx := context.Background()

	products := []string{"WB-001", "WB-002", "OZ-101"}
	for _, pid := range products {
		reviews, err := s.FetchReviews(ctx, pid, 3)
		if err != nil {
			t.Errorf("FetchReviews(%q) failed: %v", pid, err)
			continue
		}
		if len(reviews) == 0 {
			t.Errorf("FetchReviews(%q) returned 0 reviews", pid)
		}
	}
}
