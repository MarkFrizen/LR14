package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestReviewJSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second).UTC()
	r := Review{
		ID:        "wb-test-001-0",
		ProductID: "WB-001",
		Rating:    4,
		Text:      "Good product",
		Likes:     10,
		Dislikes:  2,
		Date:      now,
	}

	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var r2 Review
	if err := json.Unmarshal(data, &r2); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if r2.ID != r.ID {
		t.Errorf("ID: got %q, want %q", r2.ID, r.ID)
	}
	if r2.ProductID != r.ProductID {
		t.Errorf("ProductID: got %q, want %q", r2.ProductID, r.ProductID)
	}
	if r2.Rating != r.Rating {
		t.Errorf("Rating: got %d, want %d", r2.Rating, r.Rating)
	}
	if r2.Text != r.Text {
		t.Errorf("Text: got %q, want %q", r2.Text, r.Text)
	}
	if r2.Likes != r.Likes {
		t.Errorf("Likes: got %d, want %d", r2.Likes, r.Likes)
	}
	if r2.Dislikes != r.Dislikes {
		t.Errorf("Dislikes: got %d, want %d", r2.Dislikes, r.Dislikes)
	}
	if !r2.Date.Equal(r.Date) {
		t.Errorf("Date: got %v, want %v", r2.Date, r.Date)
	}
}

func TestReviewExtremeValues(t *testing.T) {
	r := Review{
		ID:        "edge-case",
		ProductID: "OZ-999",
		Rating:    5,
		Text:      "",
		Likes:     0,
		Dislikes:  0,
		Date:      time.Time{},
	}

	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var r2 Review
	if err := json.Unmarshal(data, &r2); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if r2.Rating != 5 {
		t.Errorf("Rating: got %d, want 5", r2.Rating)
	}
	if r2.Text != "" {
		t.Errorf("Text should be empty, got %q", r2.Text)
	}
	if r2.Likes != 0 {
		t.Errorf("Likes: got %d, want 0", r2.Likes)
	}
}

func TestReviewNegativeLikes(t *testing.T) {
	r := Review{
		ProductID: "WB-001",
		Rating:    3,
		Text:      "ok",
		Likes:     -5,
	}
	if r.Likes >= 0 {
		t.Error("expected negative likes to be preserved")
	}
}

func TestWindowAggJSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second).UTC()
	w := WindowAgg{
		ProductID:   "WB-001",
		WindowStart: now,
		AvgRating:   4.25,
		TotalLikes:  42,
		ReviewCount: 10,
		IsUpdate:    false,
	}

	data, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var w2 WindowAgg
	if err := json.Unmarshal(data, &w2); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if w2.ProductID != w.ProductID {
		t.Errorf("ProductID: got %q, want %q", w2.ProductID, w.ProductID)
	}
	if w2.AvgRating != w.AvgRating {
		t.Errorf("AvgRating: got %f, want %f", w2.AvgRating, w.AvgRating)
	}
	if w2.TotalLikes != w.TotalLikes {
		t.Errorf("TotalLikes: got %d, want %d", w2.TotalLikes, w.TotalLikes)
	}
	if w2.ReviewCount != w.ReviewCount {
		t.Errorf("ReviewCount: got %d, want %d", w2.ReviewCount, w.ReviewCount)
	}
	if w2.IsUpdate != w.IsUpdate {
		t.Errorf("IsUpdate: got %v, want %v", w2.IsUpdate, w.IsUpdate)
	}
	if !w2.WindowStart.Equal(w.WindowStart) {
		t.Errorf("WindowStart: got %v, want %v", w2.WindowStart, w.WindowStart)
	}
}

func TestWindowAggUpdateFlag(t *testing.T) {
	w := WindowAgg{
		ProductID:   "WB-002",
		WindowStart: time.Now(),
		AvgRating:   3.0,
		TotalLikes:  15,
		ReviewCount: 5,
		IsUpdate:    true,
	}
	if !w.IsUpdate {
		t.Error("IsUpdate should be true")
	}

	data, _ := json.Marshal(w)
	var w2 WindowAgg
	json.Unmarshal(data, &w2)
	if !w2.IsUpdate {
		t.Error("IsUpdate should survive JSON round-trip")
	}
}

func TestReviewStringFieldLimits(t *testing.T) {
	longText := make([]byte, 10000)
	for i := range longText {
		longText[i] = 'a'
	}

	r := Review{
		ProductID: string(longText),
		Text:      string(longText),
	}

	// JSON should still work with long strings
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal with long text failed: %v", err)
	}

	var r2 Review
	if err := json.Unmarshal(data, &r2); err != nil {
		t.Fatalf("json.Unmarshal with long text failed: %v", err)
	}

	if len(r2.ProductID) != 10000 {
		t.Errorf("ProductID length: got %d, want 10000", len(r2.ProductID))
	}
	if len(r2.Text) != 10000 {
		t.Errorf("Text length: got %d, want 10000", len(r2.Text))
	}
}

func TestReviewDefaultValues(t *testing.T) {
	r := Review{}
	if r.Rating != 0 {
		t.Errorf("default Rating should be 0, got %d", r.Rating)
	}
	if r.Likes != 0 {
		t.Errorf("default Likes should be 0, got %d", r.Likes)
	}
	if r.Dislikes != 0 {
		t.Errorf("default Dislikes should be 0, got %d", r.Dislikes)
	}
}

func TestWindowAggDefaultValues(t *testing.T) {
	w := WindowAgg{}
	if w.AvgRating != 0.0 {
		t.Errorf("default AvgRating should be 0.0, got %f", w.AvgRating)
	}
	if w.ReviewCount != 0 {
		t.Errorf("default ReviewCount should be 0, got %d", w.ReviewCount)
	}
	if w.IsUpdate != false {
		t.Errorf("default IsUpdate should be false, got %v", w.IsUpdate)
	}
}
