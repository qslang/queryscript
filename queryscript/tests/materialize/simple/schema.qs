import 'duckdb://db.duckdb'; -- NOTE in the future, we could use environment variables here

export let simple_t = db.t;

export let report_1 = SELECT MAX(a) FROM db.t;
export let report_1a = SELECT MAX(a) FROM simple_t;
export let report_2 = report_1;
export let report_3 = SELECT * FROM report_2;

-- TODO: This case is weird because we'll write a view (report_5) into the database
-- that references a temporary table (report_4). We should probably throw an error
-- in this case, or upgrade report_5 to a mat.
--
-- mat report_4 = SELECT * FROM report_3;
-- export let report_5 = SELECT COUNT(*) FROM report_4;
