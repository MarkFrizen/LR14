package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/markfriz/wb-ozon-review-collector/internal/validator"
)

func main() {
	// Путь к скомпилированной Rust-библиотеке.
	exe, _ := os.Executable()
	repoRoot := findRepoRoot(exe)
	libPath := filepath.Join(repoRoot, "rust-validator", "target", "release", libName())

	fmt.Printf("Loading Rust validator from: %s\n", libPath)

	rv, err := validator.Load(libPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAILED: %v\n", err)
		fmt.Println("\nBuild the library first:")
		fmt.Println("  cd rust-validator && cargo build --release")
		os.Exit(1)
	}
	defer rv.Close()

	// ====================================================================
	// Тестовые случаи
	// ====================================================================

	cases := []struct {
		name   string
		rating float64
		text   string
		likes  int32
	}{
		{"valid review", 3.5, "Good product, fast delivery", 10},
		{"rating too low", 0.5, "ok", 0},
		{"rating too high", 5.5, "ok", 0},
		{"empty text", 3.0, "", 5},
		{"text too long", 4.0, strings.Repeat("x", 5001), 1},
		{"negative likes", 4.0, "good", -5},
		{"boundary min", 1.0, "x", 0},
		{"boundary max", 5.0, "x", 0},
		{"many likes", 2.5, "average", 999999},
	}

	fmt.Println("\n=== Rust Validation Results ===")
	fmt.Printf("%-25s %-8s %-8s %-8s  %s\n", "Case", "Rating", "Likes", "Result", "Error")
	fmt.Println("------------------------------------------------------------")

	for _, c := range cases {
		text := c.text
		if len(text) > 50 {
			text = text[:50] + "..."
		}
		err := rv.Validate(c.rating, c.text, c.likes)
		result := "✅ OK"
		errStr := ""
		if err != nil {
			result = "❌ FAIL"
			errStr = err.Error()
		}
		fmt.Printf("%-25s %-8.1f %-8d %-8s  %s\n", c.name, c.rating, c.likes, result, errStr)
	}

	// ====================================================================
	// Примеры с реальным отзывом
	// ====================================================================
	fmt.Println("\n=== Real review examples ===")

	reviews := []struct {
		rating float64
		text   string
		likes  int32
	}{
		{4.5, "Отличный товар! Всё соответствует описанию, доставка быстрая. Рекомендую!", 127},
		{5.0, "Лучшая покупка в этом месяце, качество на высоте!", 340},
		{2.0, "Товар пришёл бракованный, надеялся на лучшее. Очень расстроен.", 5},
	}

	for i, r := range reviews {
		err := rv.Validate(r.rating, r.text, r.likes)
		status := "✅ valid"
		if err != nil {
			status = "❌ " + err.Error()
		}
		fmt.Printf("Review %d: rating=%.1f likes=%d text_len=%d → %s\n",
			i+1, r.rating, r.likes, len(r.text), status)
	}
}

func libName() string {
	if runtime.GOOS == "darwin" {
		return "libreview_validator.dylib"
	}
	return "libreview_validator.so"
}

func findRepoRoot(exePath string) string {
	// Поднимаемся от exe до корня репозитория.
	dir := filepath.Dir(exePath)
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	// Fallback: текущая директория.
	cwd, _ := os.Getwd()
	return cwd
}
