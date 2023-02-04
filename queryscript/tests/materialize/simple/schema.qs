import 'duckdb://db.duckdb'; -- NOTE in the future, we could use environment variables here

export let simple_t = db.t;
export let report = SELECT MAX(a) FROM simple_t;
