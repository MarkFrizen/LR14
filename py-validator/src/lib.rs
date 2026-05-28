use pyo3::prelude::*;

/// Проверяет корректность полей отзыва маркетплейса.
///
/// Args:
///     rating (float): рейтинг (1.0–5.0)
///     text (str): текст отзыва (не пустой, ≤5000 символов)
///     likes (int): количество лайков (≥0)
///
/// Returns:
///     tuple (bool, str):
///         - (True, "") если валидация пройдена
///         - (False, "причина ошибки") если есть проблема
#[pyfunction]
pub fn validate_review_py(rating: f64, text: &str, likes: i32) -> PyResult<(bool, String)> {
    // 1. Рейтинг
    if rating < 1.0 || rating > 5.0 {
        return Ok((false, format!(
            "rating must be between 1.0 and 5.0, got {rating}"
        )));
    }

    // 2. Текст
    if text.is_empty() {
        return Ok((false, "text must not be empty".into()));
    }
    let char_count = text.chars().count();
    if char_count > 5000 {
        return Ok((false, format!(
            "text too long: {char_count} chars, max 5000"
        )));
    }

    // 3. Лайки
    if likes < 0 {
        return Ok((false, format!(
            "likes must be >= 0, got {likes}"
        )));
    }

    Ok((true, String::new()))
}

/// Python-модуль.
#[pymodule]
fn py_review_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_review_py, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_review() {
        let (ok, err) = validate_review_py(3.0, "Хороший товар", 10).unwrap();
        assert!(ok);
        assert!(err.is_empty());
    }

    #[test]
    fn test_valid_boundary_rating() {
        let (ok, err) = validate_review_py(1.0, "Плохо", 0).unwrap();
        assert!(ok);
        assert!(err.is_empty());

        let (ok, err) = validate_review_py(5.0, "Отлично", 999).unwrap();
        assert!(ok);
        assert!(err.is_empty());
    }

    #[test]
    fn test_rating_below_min() {
        let (ok, err) = validate_review_py(0.9, "текст", 0).unwrap();
        assert!(!ok);
        assert!(err.contains("1.0"));
    }

    #[test]
    fn test_rating_above_max() {
        let (ok, err) = validate_review_py(5.1, "текст", 0).unwrap();
        assert!(!ok);
        assert!(err.contains("5.0"));
    }

    #[test]
    fn test_empty_text() {
        let (ok, err) = validate_review_py(3.0, "", 0).unwrap();
        assert!(!ok);
        assert!(err.contains("empty"));
    }

    #[test]
    fn test_text_too_long() {
        let long_text = "а".repeat(5001);
        let (ok, err) = validate_review_py(3.0, &long_text, 0).unwrap();
        assert!(!ok);
        assert!(err.contains("5000"));
    }

    #[test]
    fn test_max_length_text() {
        let long_text = "а".repeat(5000);
        let (ok, err) = validate_review_py(3.0, &long_text, 0).unwrap();
        assert!(ok);
        assert!(err.is_empty());
    }

    #[test]
    fn test_negative_likes() {
        let (ok, err) = validate_review_py(3.0, "текст", -1).unwrap();
        assert!(!ok);
        assert!(err.contains(">= 0"));
    }

    #[test]
    fn test_zero_likes() {
        let (ok, err) = validate_review_py(3.0, "текст", 0).unwrap();
        assert!(ok);
        assert!(err.is_empty());
    }
}
