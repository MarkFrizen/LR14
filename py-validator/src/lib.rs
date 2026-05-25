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
