package marketplace

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/models"
)

// Simulator имитирует API маркетплейса (Wildberries / Ozon).
// В реальном проекте здесь был бы HTTP-клиент к api.wildberries.ru или api.ozon.ru.
type Simulator struct {
	// source определяет, какой маркетплейс имитируем.
	source string // "wildberries" или "ozon"
}

// NewSimulator создаёт новый экземпляр симулятора.
func NewSimulator(source string) *Simulator {
	return &Simulator{source: source}
}

// FetchReviews «собирает» отзывы для заданного product_id.
// В реальном приложении здесь был бы HTTP-запрос с пагинацией.
func (s *Simulator) FetchReviews(ctx context.Context, productID string, limit int) ([]models.Review, error) {
	// Симулируем задержку сетевого запроса.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(50+rand.Intn(150)) * time.Millisecond):
	}

	n := 1 + rand.Intn(limit)
	reviews := make([]models.Review, 0, n)

	for i := 0; i < n; i++ {
		rating := 1 + rand.Intn(5)
		likes := rand.Intn(50)
		dislikes := rand.Intn(20)

		review := models.Review{
			ID:        fmt.Sprintf("%s-%s-%d", s.source, productID, i),
			ProductID: productID,
			Rating:    rating,
			Text:      generateReviewText(rating),
			Likes:     likes,
			Dislikes:  dislikes,
			Date:      time.Now().Add(-time.Duration(rand.Intn(720)) * time.Hour),
		}
		reviews = append(reviews, review)
	}

	return reviews, nil
}

// generateReviewText возвращает текст-заглушку в зависимости от рейтинга.
func generateReviewText(rating int) string {
	templates := map[int][]string{
		1: {"Ужасное качество, не советую!", "Не работает, деньги на ветер.", "Очень разочарован покупкой."},
		2: {"Плохо, но есть плюсы.", "Ожидал большего за такие деньги.", "Не рекомендую, много недостатков."},
		3: {"Нормально, но не более.", "Средне, могло быть и лучше.", "Своих денег стоит, но без восторга."},
		4: {"Хороший товар, почти всё устроило.", "Доволен покупкой, рекомендую.", "Качественно, есть мелкие недочёты."},
		5: {"Отличный товар! Всё супер!", "Лучшая покупка в этом месяце!", "Быстрая доставка, качество на высоте."},
	}

	opts := templates[rating]
	return opts[rand.Intn(len(opts))]
}
