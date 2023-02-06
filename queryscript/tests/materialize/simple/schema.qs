import 'duckdb://db.duckdb'; -- NOTE in the future, we could use environment variables here

export let simple_t = db.t;

export let report_1 = SELECT MAX(a) FROM db.t;
export let report_1a = SELECT MAX(a) FROM simple_t;
export let report_2 = report_1;
export let report_3 = SELECT * FROM report_2;

mat report_4 = SELECT * FROM report_3;
export let report_5 = SELECT COUNT(*) FROM report_4;
