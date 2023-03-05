use queryscript::runtime::SQLEngineType;

pub fn get_engine_url(engine_type: SQLEngineType) -> String {
    match engine_type {
        SQLEngineType::DuckDB => "duckdb://db.duckdb".to_string(),
        SQLEngineType::ClickHouse => "clickhouse://default:example@127.0.0.1:5472/db".to_string(),
    }
}
pub fn show_views_query(engine_type: SQLEngineType) -> sqlparser::ast::Statement {
    sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            match engine_type {
                SQLEngineType::DuckDB => "SELECT name FROM sqlite_master WHERE type = 'view'",
                SQLEngineType::ClickHouse => {
                    "select name from system.tables where engine = 'View' AND database=currentDatabase()"
                }
            },
        )
        .unwrap()
        .swap_remove(0)
}
