use std::ffi::c_char;
use std::ffi::CStr;
use std::fmt::Write;

/// Проверяет корректность полей отзыва маркетплейса.
///
/// Возвращает `Ok(())` если все проверки пройдены, или `Err(message)` с описанием
/// первой найденной проблемы.
///
/// Правила:
/// - rating ∈ [1.0, 5.0]
/// - text: не пустой, не длиннее 5000 символов
/// - likes ≥ 0
pub fn review_validate(rating: f64, text: &str, likes: i32) -> Result<(), String> {
    // 1. Рейтинг.
    if rating < 1.0 || rating > 5.0 {
        let mut msg = String::new();
        write!(msg, "rating must be between 1.0 and 5.0, got {rating}").unwrap();
        return Err(msg);
    }

    // 2. Текст.
    if text.is_empty() {
        return Err("text must not be empty".into());
    }
    let char_count = text.chars().count();
    if char_count > 5000 {
        let mut msg = String::new();
        write!(msg, "text too long: {char_count} chars, max 5000").unwrap();
        return Err(msg);
    }

    // 3. Лайки.
    if likes < 0 {
        let mut msg = String::new();
        write!(msg, "likes must be >= 0, got {likes}").unwrap();
        return Err(msg);
    }

    Ok(())
}

// ====================================================================
// C-compatible FFI
// ====================================================================

/// C-совместимая обёртка над `validate_review`.
///
/// # Safety
///
/// - `text` должен быть валидной C-строкой (null-terminated).
/// - `error_out` должен указывать на буфер размером `error_out_len` байт.
///
/// # Возврат
///
/// - `0` — успех (все проверки пройдены).
/// - `-1` — ошибка валидации, сообщение записано в `error_out`.
#[no_mangle]
pub unsafe extern "C" fn validate_review(
    rating: f64,
    text: *const c_char,
    likes: i32,
    error_out: *mut c_char,
    error_out_len: usize,
) -> i32 {
    // Конвертируем C-строку в &str.
    let text_str = match unsafe { CStr::from_ptr(text) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            write_error(error_out, error_out_len, "text is not valid UTF-8");
            return -1;
        }
    };

    // Вызываем Rust-функцию.
    match review_validate(rating, text_str, likes) {
        Ok(()) => 0,
        Err(msg) => {
            write_error(error_out, error_out_len, &msg);
            -1
        }
    }
}

/// Вспомогательная функция: записывает сообщение об ошибке в C-буфер.
fn write_error(buf: *mut c_char, len: usize, msg: &str) {
    if buf.is_null() || len == 0 {
        return;
    }
    let bytes = msg.as_bytes();
    let copy_len = bytes.len().min(len - 1); // оставляем место для '\0'

    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, copy_len);
        *buf.add(copy_len) = 0; // null-terminator
    }
}

// ====================================================================
// Тесты
// ====================================================================

#[cfg(test)]
mod tests {
    use super::review_validate as validate_review;

    #[test]
    fn valid_review() {
        assert!(validate_review(3.5, "Good product", 10).is_ok());
        assert!(validate_review(1.0, "x", 0).is_ok());
        assert!(validate_review(5.0, "Excellent!", 9999).is_ok());
    }

    #[test]
    fn rating_out_of_range() {
        let err = validate_review(0.9, "ok", 0).unwrap_err();
        assert!(err.contains("rating"));
        let err = validate_review(5.1, "ok", 0).unwrap_err();
        assert!(err.contains("rating"));
    }

    #[test]
    fn empty_text() {
        let err = validate_review(3.0, "", 0).unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn text_too_long() {
        let long = "a".repeat(5001);
        let err = validate_review(3.0, &long, 0).unwrap_err();
        assert!(err.contains("too long"));
    }

    #[test]
    fn text_exactly_5000() {
        let long = "a".repeat(5000);
        assert!(validate_review(3.0, &long, 0).is_ok());
    }

    #[test]
    fn negative_likes() {
        let err = validate_review(3.0, "ok", -1).unwrap_err();
        assert!(err.contains("likes"));
    }

    #[test]
    fn boundary_values() {
        assert!(validate_review(1.0, "ok", 0).is_ok());
        assert!(validate_review(5.0, "ok", 0).is_ok());
    }
}
