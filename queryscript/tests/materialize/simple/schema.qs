import 'duckdb://db.duckdb'; -- NOTE in the future, we could use environment variables here

export let simple_t = db.t;

export let u_report = SELECT MAX(a) FROM db.t;
-- TODO: Uncomment this once we support inlining materialized tables
-- export let m_report = SELECT MAX(a) FROM simple_t;
