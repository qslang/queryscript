#include <iostream>
#include "engine/include/duckdb-extra.hpp"
#include "engine/src/duckdb/engine.rs.h"

std::unique_ptr<ArrowArrayStreamWrapper> new_array_stream_wrapper(uintptr_t data, duckdb::ArrowStreamParameters &parameters)
{
    auto ret = duckdb::make_unique<ArrowArrayStreamWrapper>();

    rust_build_array_stream(
        (uint32_t *)data,
        parameters.projected_columns.columns,
        (uint32_t *)&ret->arrow_array_stream);

    return ret;
}

uint32_t *get_create_stream_fn()
{
    return (uint32_t *)new_array_stream_wrapper;
}

Value *duckdb_create_pointer(uint32_t *value)
{
    auto val = duckdb::Value::POINTER((uintptr_t)value);
    return (Value *)new duckdb::Value(val);
}

// These declarations are copied from duckdb.cpp (the file included within duckdb-rs,
// pinned to version 0.6.0). They are re-declared here so that we can reference them
// in the redeclaration of the arrow_scan function as arrow_scan_qvm.
//
// If we update duckdb-rs, we should manually update these declarations to make sure they
// align with duckdb.cpp, otherwise we may see undefined behavior. Of note, ArrowTableFunction
// is a struct comprised only of static functions (no state) and we've only copied the relevant
// subset of its declarations.
namespace duckdb
{
    //===--------------------------------------------------------------------===//
    // Arrow Variable Size Types
    //===--------------------------------------------------------------------===//
    enum class ArrowVariableSizeType : uint8_t
    {
        FIXED_SIZE = 0,
        NORMAL = 1,
        SUPER_SIZE = 2
    };

    //===--------------------------------------------------------------------===//
    // Arrow Time/Date Types
    //===--------------------------------------------------------------------===//
    enum class ArrowDateTimeType : uint8_t
    {
        MILLISECONDS = 0,
        MICROSECONDS = 1,
        NANOSECONDS = 2,
        SECONDS = 3,
        DAYS = 4,
        MONTHS = 5
    };

    struct ArrowConvertData
    {
        ArrowConvertData(LogicalType type) : dictionary_type(type){};
        ArrowConvertData(){};

        //! Hold type of dictionary
        LogicalType dictionary_type;
        //! If its a variable size type (e.g., strings, blobs, lists) holds which type it is
        vector<pair<ArrowVariableSizeType, idx_t>> variable_sz_type;
        //! If this is a date/time holds its precision
        vector<ArrowDateTimeType> date_time_precision;
    };

    // These declarations are copied from duckdb.cpp
    struct ArrowTableFunction
    {
    public:
        static void RegisterFunction(BuiltinFunctions &set);

        //! Binds an arrow table
        static unique_ptr<FunctionData> ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names);

        //! Initialize Global State
        static unique_ptr<GlobalTableFunctionState> ArrowScanInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input);

        //! Initialize Local State
        static unique_ptr<LocalTableFunctionState> ArrowScanInitLocal(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state);

        //! Scan Function
        static void ArrowScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

        //! Defines Maximum Number of Threads
        static idx_t ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data);

        //! Allows parallel Create Table / Insertion
        static idx_t ArrowGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                        LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state);

        //! -----Utility Functions:-----
        //! Gets Arrow Table's Cardinality
        static unique_ptr<NodeStatistics> ArrowScanCardinality(ClientContext &context, const FunctionData *bind_data);
        //! Gets the progress on the table scan, used for Progress Bars
        static double ArrowProgress(ClientContext &context, const FunctionData *bind_data,
                                    const GlobalTableFunctionState *global_state);
        //! Renames repeated columns and case sensitive columns
        static void RenameArrowColumns(vector<string> &names);
        //! Helper function to get the DuckDB logical type
        static LogicalType GetArrowLogicalType(ArrowSchema &schema,
                                               std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                                               idx_t col_idx);
    };

}

void init_arrow_scan(uint32_t *connection_ptr)
{
    using namespace duckdb;

    // This code is mirrored from ArrowTableFunction::RegisterFunction
    TableFunction arrow("arrow_scan_qvm", {LogicalType::POINTER, LogicalType::POINTER, LogicalType::POINTER},
                        ArrowTableFunction::ArrowScanFunction, ArrowTableFunction::ArrowScanBind,
                        ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);

    arrow.cardinality = ArrowTableFunction::ArrowScanCardinality;
    arrow.get_batch_index = ArrowTableFunction::ArrowGetBatchIndex;
    arrow.projection_pushdown = true;
    arrow.filter_pushdown = false; // CHANGED FROM duckdb
    arrow.filter_prune = true;

    auto tf_info = CreateTableFunctionInfo(move(arrow));

    // This code is mirrored from duckdb_register_table_function
    auto con = (duckdb::Connection *)connection_ptr;
    con->context->RunFunctionInTransaction([&]()
                                           {
        auto &catalog = duckdb::Catalog::GetCatalog(*con->context);
        catalog.CreateTableFunction(*con->context, &tf_info); });
}