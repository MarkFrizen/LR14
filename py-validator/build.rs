fn main() {
    // Линкуем libpython для поддержки тестов.
    println!("cargo:rustc-link-lib=python3.12");
    println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu");
}
