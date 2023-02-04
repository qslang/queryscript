import 'duckdb://db.duckdb'; -- NOTE in the future, we could use environment variables here

type T {
    a int,
};

export let t [T] = materialize(load('t.csv'), db);
