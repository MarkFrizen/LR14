package validator

/*
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// C-helper: загружает .so, находит символ, вызывает его.
// Возвращает 0 при успехе, -1 при ошибке (error_msg заполнен).
int call_validate_review(
    const char* lib_path,
    double rating,
    const char* text,
    int32_t likes,
    char* error_out,
    size_t error_out_len
) {
    void* handle = dlopen(lib_path, RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        snprintf(error_out, error_out_len, "dlopen: %s", dlerror());
        return -1;
    }

    typedef int (*validate_fn)(double, const char*, int32_t, char*, size_t);
    validate_fn fn = (validate_fn)dlsym(handle, "validate_review");
    if (!fn) {
        snprintf(error_out, error_out_len, "dlsym: %s", dlerror());
        dlclose(handle);
        return -1;
    }

    int ret = fn(rating, text, likes, error_out, error_out_len);

    dlclose(handle);
    return ret;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// RustValidator загружает Rust-библиотеку при каждом вызове Validate.
// Для высоконагруженных сценариев библиотеку можно держать открытой,
// но в демо-примере нагрузка несущественна.
type RustValidator struct {
	libPath string
}

// Load создаёт валидатор с путём к .so.
func Load(libPath string) (*RustValidator, error) {
	return &RustValidator{libPath: libPath}, nil
}

// Close — заглушка (в данной реализации библиотека закрывается после каждого вызова).
func (rv *RustValidator) Close() error {
	return nil
}

// Validate вызывает validate_review из Rust-библиотеки.
func (rv *RustValidator) Validate(rating float64, text string, likes int32) error {
	cPath := C.CString(rv.libPath)
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cPath))
	defer C.free(unsafe.Pointer(cText))

	const bufSize = 1024
	errBuf := (*C.char)(C.malloc(bufSize))
	defer C.free(unsafe.Pointer(errBuf))
	C.memset(unsafe.Pointer(errBuf), 0, bufSize)

	ret := C.call_validate_review(
		cPath,
		C.double(rating),
		cText,
		C.int32_t(likes),
		errBuf,
		C.size_t(bufSize),
	)

	if ret == 0 {
		return nil
	}
	return fmt.Errorf("%s", C.GoString(errBuf))
}
