package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/markfriz/wb-ozon-review-collector/internal/validator"
)

type review struct {
	rating float64
	text   string
	likes  int32
}

func main() {
	const n = 100_000

	// ====================================================================
	// 1. Единый набор данных
	// ====================================================================
	fmt.Printf("Generating %d reviews...\n", n)
	reviews := generateReviews(n)

	// ====================================================================
	// 2. Pure Go (inline validation)
	// ====================================================================
	fmt.Println("\n--- 1/3: Pure Go (inline validation) ---")
	start := time.Now()
	for _, r := range reviews {
		validateGo(r.rating, r.text, r.likes)
	}
	tGo := time.Since(start)
	rpsGo := float64(n) / tGo.Seconds()
	fmt.Printf("  Time: %v  |  %.0f rows/sec\n", tGo, rpsGo)

	// ====================================================================
	// 3. Go + cgo → Rust (.so)
	// ====================================================================
	fmt.Println("\n--- 2/3: Go cgo → Rust ---")
	libPath := findLib()
	fmt.Printf("  Lib: %s\n", libPath)

	var tCgo time.Duration
	var rpsCgo float64

	rv, err := validator.Load(libPath)
	if err != nil {
		fmt.Printf("  SKIP — %v\n", err)
		fmt.Println("  Build Rust lib: cd rust-validator && cargo build --release")
	} else {
		start = time.Now()
		for _, r := range reviews {
			rv.Validate(r.rating, r.text, r.likes)
		}
		tCgo = time.Since(start)
		rpsCgo = float64(n) / tCgo.Seconds()
		rv.Close()
		fmt.Printf("  Time: %v  |  %.0f rows/sec\n", tCgo, rpsCgo)
	}

	// ====================================================================
	// 4. Итог
	// ====================================================================
	fmt.Println("\n" + strings.Repeat("=", 65))
	fmt.Println("  BENCHMARK: 100 000 reviews validated")
	fmt.Println(strings.Repeat("=", 65))
	fmt.Printf("  %-28s %14s %16s\n", "Method", "Time", "Rows/sec")
	fmt.Printf("  %-28s %14s %16s\n", "───", "──", "───────")
	fmt.Printf("  %-28s %14v %16.0f\n", "Pure Go (inline)", tGo, rpsGo)
	if rpsCgo > 0 {
		fmt.Printf("  %-28s %14v %16.0f\n", "Go cgo → Rust (.so)", tCgo, rpsCgo)
		fmt.Printf("  %-28s %14s %16s\n", "", "", "")
		fmt.Printf("  %-28s %14.1f×%16s\n", "Pure Go vs Go+Rust", tCgo.Seconds()/tGo.Seconds(), "")
	} else {
		fmt.Println("  Go cgo → Rust: SKIPPED (build Rust lib first)")
	}

	// Save results to JSON for the comparison script
	type benchResult struct {
		Method     string  `json:"method"`
		TimeSec    float64 `json:"time_sec"`
		RowsPerSec float64 `json:"rows_per_sec"`
	}
	results := []benchResult{
		{"Pure Go (inline)", tGo.Seconds(), rpsGo},
	}
	if rpsCgo > 0 {
		results = append(results, benchResult{"Go cgo → Rust (.so)", tCgo.Seconds(), rpsCgo})
	}

	data, _ := json.MarshalIndent(results, "", "  ")
	os.WriteFile("benchmark_results_go.json", data, 0644)
	fmt.Println("\n  Результаты сохранены в benchmark_results_go.json")
}

func validateGo(rating float64, text string, likes int32) bool {
	if rating < 1.0 || rating > 5.0 {
		return false
	}
	if text == "" || utf8.RuneCountInString(text) > 5000 {
		return false
	}
	if likes < 0 {
		return false
	}
	return true
}

func generateReviews(n int) []review {
	rng := rand.New(rand.NewSource(42))
	reviews := make([]review, n)

	for i := range reviews {
		coin := rng.Float64()

		rating := 1.0 + rng.Float64()*4.0
		if coin < 0.05 {
			rating = rng.Float64()*0.9 - 0.1
		}

		text := "Normal review text. This is a valid review for the benchmark."
		if coin < 0.08 {
			text = ""
		} else if coin < 0.10 {
			text = strings.Repeat("x", 5001)
		}

		likes := int32(rng.Intn(500))
		if coin < 0.13 {
			likes = int32(-rng.Intn(10) - 1)
		}

		reviews[i] = review{rating: rating, text: text, likes: likes}
	}
	return reviews
}

func findLib() string {
	dir, _ := os.Getwd()
	for i := 0; i < 5; i++ {
		info, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil && !info.IsDir() {
			return filepath.Join(dir, "rust-validator", "target", "release", "libreview_validator.so")
		}
		dir = filepath.Dir(dir)
	}
	return "rust-validator/target/release/libreview_validator.so"
}
