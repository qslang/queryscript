fn main() {
    cxx_build::bridge("src/duckdb/engine.rs")
        .file("src/duckdb/duckdb-extra.cc")
        .flag_if_supported("-std=c++14")
        .flag_if_supported("-stdlib=libc++")
        .flag_if_supported("-stdlib=libstdc++")
        .flag_if_supported("/bigobj")
        .warnings(false)
        .compile("duckdbcpp");

    println!("cargo:rerun-if-changed=src/duckdb/engine.rs");
    println!("cargo:rerun-if-changed=src/duckdb/duckdb-extra.cc");
    println!("cargo:rerun-if-changed=include/duckdb-extra.hpp");
}
