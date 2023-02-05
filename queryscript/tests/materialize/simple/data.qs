import 'duckdb://db.duckdb'; -- NOTE in the future, we could use environment variables here

type T {
    a int,
};

export mat(db) t [T] = load('t.csv');
