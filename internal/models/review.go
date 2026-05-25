package models

import "time"

// Review представляет один отзыв с маркетплейса.
type Review struct {
	ID        string    `json:"id"`
	ProductID string    `json:"product_id"`
	Rating    int       `json:"rating"`
	Text      string    `json:"text"`
	Likes     int       `json:"likes"`
	Dislikes  int       `json:"dislikes"`
	Date      time.Time `json:"date"`
}
