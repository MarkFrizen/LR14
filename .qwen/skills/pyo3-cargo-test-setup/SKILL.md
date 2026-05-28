---
name: pyo3-cargo-test-setup
description: Настройка pyo3-проекта так, чтобы `cargo test` компилировался и запускал unit-тесты, а не падал с ошибками линковки libpython
source: auto-skill
extracted_at: '2026-05-28T09:42:20.646Z'
---

# Как настроить pyo3-проект для `cargo test`

pyo3-проекты с `crate-type = ["cdylib"]` (extension module) не компилируются в тестовом режиме, потому что тестовый бинарник не линкуется с libpython. Вот пошаговая инструкция.

## 1. Cargo.toml

```toml
[lib]
name = "my_module"
crate-type = ["cdylib", "lib"]  # ← обязательно добавить "lib" для тестов

[dependencies]
pyo3 = { version = "0.22", features = ["extension-module", "auto-initialize"] }
#                          ↑ auto-initialize нужно для тестов

[build-dependencies]
pyo3-build-config = "0.22"
```

## 2. build.rs

```rust
fn main() {
    // Линкуем libpython, чтобы тестовый бинарник нашёл символы PyObject_*
    println!("cargo:rustc-link-lib=python3.12");
    println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu");
}
```

Путь может отличаться от системы к системе — проверьте через `python3-config --ldflags --embed`.

## 3. Тесты в lib.rs

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        let (ok, err) = my_function(1.0, "text", 0).unwrap();
        assert!(ok);
    }
}
```

## 4. Запуск

```bash
cargo test
```

## Почему это работает

- `crate-type = ["lib"]` — создаёт обычную rlib, которая может быть слинкована в тестовый бинарник
- `features = ["auto-initialize"]` — позволяет pyo3 инициализировать Python-интерпретатор автоматически, даже когда модуль не загружается Python-рантаймом
- `build.rs` — явно указывает линковщику libpython, чтобы разрешить символы PyObject_Str, PyErr_SetString и т.д.
