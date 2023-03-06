fn main() {
    cxx_build::bridge("src/runtime/duckdb/engine.rs")
        .file("src/runtime/duckdb/duckdb-extra.cc")
        .flag_if_supported("-std=c++14")
        .flag_if_supported("-stdlib=libc++")
        .flag_if_supported("-stdlib=libstdc++")
        .flag_if_supported("/bigobj")
        .warnings(false)
        .compile("duckdbengine");

    println!("cargo:rerun-if-changed=src/runtime/duckdb/engine.rs");
    println!("cargo:rerun-if-changed=src/runtime/duckdb/duckdb-extra.cc");
    println!("cargo:rerun-if-changed=include/duckdb-extra.hpp");
    println!("cargo:rerun-if-changed=include/duckdb.hpp");
}
