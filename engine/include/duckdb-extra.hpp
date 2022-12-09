#pragma once

// NOTE: This included duckdb.hpp file is exactly the released version of duckdb.hpp
// copied from libduckdb-sys (bundled in duckdb-rs). If we update the library, we must
// update this file as well.
#include "engine/include/duckdb.hpp"

namespace duckdb
{
    struct ArrowProjectedColumns
    {
        unordered_map<idx_t, string> projection_map;
        vector<string> columns;
    };

    struct ArrowStreamParameters
    {
        ArrowProjectedColumns projected_columns;
        TableFilterSet *filters;
    };
}

using ArrowArrayStreamWrapper = duckdb::ArrowArrayStreamWrapper;
using Value = duckdb::Value;

uint32_t *get_create_stream_fn();
Value *duckdb_create_pointer(uint32_t *value);
void init_arrow_scan(uint32_t *connection_ptr);
