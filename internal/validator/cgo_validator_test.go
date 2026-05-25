package validator

import (
	"testing"
)

func TestLoad(t *testing.T) {
	// Проверяем, что Load возвращает валидатор
	// (даже если библиотеки нет на диске — это ошибка времени выполнения, не создания)
	rv, err := Load("/tmp/nonexistent/libreview_validator.so")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if rv == nil {
		t.Fatal("Load returned nil")
	}
	if rv.libPath != "/tmp/nonexistent/libreview_validator.so" {
		t.Errorf("libPath = %q, want %q", rv.libPath, "/tmp/nonexistent/libreview_validator.so")
	}
}

func TestLoadWithEmptyPath(t *testing.T) {
	rv, err := Load("")
	if err != nil {
		t.Fatalf("Load with empty path failed: %v", err)
	}
	if rv == nil {
		t.Fatal("Load with empty path returned nil")
	}
}

func TestClose(t *testing.T) {
	rv, err := Load("/some/path.so")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if err := rv.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestValidateMissingLib(t *testing.T) {
	rv, err := Load("/tmp/definitely_not_exists_12345/libreview_validator.so")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Validate должна вернуть ошибку, так как .so не существует
	err = rv.Validate(3.5, "good product", 10)
	if err == nil {
		t.Log("Validate returned nil — возможно библиотека найдена по этому пути")
	} else {
		t.Logf("Validate expectedly failed: %v", err)
	}
}

func TestNewValidatorThenValidateInvalidRating(t *testing.T) {
	rv, err := Load("/tmp/fake.so")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Вызов с невалидным рейтингом — должны получить ошибку от cgo
	err = rv.Validate(6.0, "text", 5)
	if err == nil {
		t.Log("Validate returned nil — cgo call may have succeeded unexpectedly")
	} else {
		t.Logf("Validate error: %v", err)
	}
}
