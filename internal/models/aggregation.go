package models

import "time"

// WindowAgg — результат оконной агрегации по одному product_id за одно окно.
type WindowAgg struct {
	ProductID   string    `json:"product_id"`
	WindowStart time.Time `json:"window_start"`
	AvgRating   float64   `json:"avg_rating"`
	TotalLikes  int       `json:"total_likes"`
	ReviewCount int       `json:"review_count"`
	IsUpdate    bool      `json:"is_update"` // true — пересчёт после позднего события
}
