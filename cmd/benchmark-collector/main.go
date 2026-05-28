// Go-бенчмарк сборщика отзывов (без etcd/NATS, чистое измерение производительности).
//
// Запуск:
//   cd .../LR14 && go run ./cmd/benchmark-collector/
//
// Для сравнения с Python:
//   /usr/bin/time -v go run ./cmd/benchmark-collector/ 2>&1 | tee go_bench.log

package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/markfriz/wb-ozon-review-collector/internal/marketplace"
	"github.com/markfriz/wb-ozon-review-collector/internal/models"
)

// MetricsSnapshot — снимок метрик в момент времени (аналог Python MetricsSnapshot).
type MetricsSnapshot struct {
	Elapsed      float64
	TotalReviews int
	AllocMB      float64
}

func main() {
	numProducts := flag.Int("products", 1000, "number of product IDs")
	reviewsPerProduct := flag.Int("reviews", 50, "reviews per product (fixed)")
	source := flag.String("source", "wildberries", "marketplace source")
	concurrency := flag.Int("concurrency", 50, "concurrent goroutines")
	flag.Parse()

	fmt.Printf("Go Benchmark: %d products × %d reviews = %d total (expected)\n",
		*numProducts, *reviewsPerProduct, *numProducts**reviewsPerProduct)
	fmt.Printf("Concurrency: %d, Source: %s\n\n", *concurrency, *source)

	// Инициализация
	_ = marketplace.NewSimulator(*source) // force import

	// Генерация product_id
	productIDs := make([]string, *numProducts)
	for i := 0; i < *numProducts; i++ {
		productIDs[i] = fmt.Sprintf("BENCH-%04d", i)
	}

	// Счётчики
	var (
		mu           sync.Mutex
		totalReviews int
	)

	sem := make(chan struct{}, *concurrency)

	// Замер времени
	start := time.Now()

	// Канал для сбора метрик
	type reviewBatch struct {
		productID string
		count     int
	}
	batchCh := make(chan reviewBatch, *numProducts)

	var wg sync.WaitGroup

	// worker: собирает отзывы для одного товара
	worker := func(pid string) {
		defer wg.Done()
		sem <- struct{}{}        // acquire
		defer func() { <-sem }() // release

		// Генерируем ровно reviewsPerProduct отзывов (детерминированно)
		reviews := make([]models.Review, *reviewsPerProduct)
		now := time.Now()
		for i := 0; i < *reviewsPerProduct; i++ {
			rating := 1 + rand.IntN(5)
			reviews[i] = models.Review{
				ID:        fmt.Sprintf("%s-%s-%d", *source, pid, i),
				ProductID: pid,
				Rating:    rating,
				Text:      reviewText(rating),
				Likes:     rand.IntN(50),
				Dislikes:  rand.IntN(20),
				Date:      now.Add(-time.Duration(rand.IntN(720)) * time.Hour),
			}
		}

		// Симулируем задержку HTTP-запроса (как в marketplace.Simulator)
		sleepMs := 50 + rand.IntN(150)
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)

		mu.Lock()
		totalReviews += len(reviews)
		mu.Unlock()

		batchCh <- reviewBatch{productID: pid, count: len(reviews)}
	}

	// Запуск воркеров
	for _, pid := range productIDs {
		wg.Add(1)
		go worker(pid)
	}

	// Фоновая горутина: метрики (каждые 5 секунд)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		prevReviews := 0
		for range ticker.C {
			elapsed := time.Since(start).Seconds()
			mu.Lock()
			current := totalReviews
			mu.Unlock()

			rps := float64(current-prevReviews) / 5.0
			prevReviews = current

			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)

			fmt.Printf("  [METRICS] t=%.1fs  reviews=%d  rps=%.0f  alloc=%.1fMB\n",
				elapsed, current, rps, float64(mem.Alloc)/1024/1024)
		}
	}()

	// Сбор результатов (через отдельную горутину)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(batchCh)
		close(done)
	}()

	// Собираем метрики из канала
	productCount := 0
	for range batchCh {
		productCount++
	}
	<-done // все воркеры завершены

	elapsed := time.Since(start).Seconds()

	// Финальная память
	var memFinal runtime.MemStats
	runtime.ReadMemStats(&memFinal)

	// Max RSS через getrusage
	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	maxRSSMB := float64(rusage.Maxrss) / 1024 // на Linux Maxrss в KB

	// CPU time (пользовательское + системное)
	cpuTimeSec := float64(rusage.Utime.Sec+rusage.Stime.Sec) +
		float64(rusage.Utime.Usec+rusage.Stime.Usec)/1_000_000

	wallSec := elapsed
	cpuPercent := 0.0
	if wallSec > 0 {
		cpuPercent = cpuTimeSec / wallSec * 100
	}

	sep := strings.Repeat("=", 60)
	fmt.Printf("\n%s\n", sep)
	fmt.Printf("  GO BENCHMARK RESULTS\n")
	fmt.Printf("%s\n", sep)
	fmt.Printf("  Wall clock:       %8.2f s\n", elapsed)
	fmt.Printf("  Total reviews:    %8d\n", totalReviews)
	fmt.Printf("  Products done:    %8d\n", productCount)
	fmt.Printf("  Throughput:       %8.1f rev/s\n", float64(totalReviews)/elapsed)
	fmt.Printf("  CPU time:         %8.2f s\n", cpuTimeSec)
	fmt.Printf("  CPU usage:        %8.1f %%\n", cpuPercent)
	fmt.Printf("  Max RSS:          %8.1f MB\n", maxRSSMB)
	fmt.Printf("  Alloc (final):    %8.1f MB\n", float64(memFinal.Alloc)/1024/1024)
	fmt.Printf("  Total alloc:      %8.1f MB\n", float64(memFinal.TotalAlloc)/1024/1024)
	fmt.Printf("  GC cycles:        %8d\n", memFinal.NumGC)
	fmt.Printf("%s\n", sep)

	// Вывод summary-строки для парсинга скриптом сравнения
	fmt.Printf("\nSUMMARY_JSON:")
	fmt.Printf(`{"language":"go","wall_clock_s":%.2f,"total_reviews":%d,"products":%d,"rps":%.1f,"cpu_pct":%.1f,"max_rss_mb":%.1f,"alloc_final_mb":%.1f,"total_alloc_mb":%.1f,"gc_cycles":%d}`,
		elapsed, totalReviews, productCount, float64(totalReviews)/elapsed,
		cpuPercent, maxRSSMB,
		float64(memFinal.Alloc)/1024/1024, float64(memFinal.TotalAlloc)/1024/1024, memFinal.NumGC)
	fmt.Println()
}

func reviewText(rating int) string {
	templates := map[int][]string{
		1: {"Ужасное качество, не советую!", "Не работает, деньги на ветер.", "Очень разочарован покупкой."},
		2: {"Плохо, но есть плюсы.", "Ожидал большего за такие деньги.", "Не рекомендую, много недостатков."},
		3: {"Нормально, но не более.", "Средне, могло быть и лучше.", "Своих денег стоит, но без восторга."},
		4: {"Хороший товар, почти всё устроило.", "Доволен покупкой, рекомендую.", "Качественно, есть мелкие недочёты."},
		5: {"Отличный товар! Всё супер!", "Лучшая покупка в этом месяце!", "Быстрая доставка, качество на высоте."},
	}
	opts := templates[rating]
	return opts[rand.IntN(len(opts))]
}
