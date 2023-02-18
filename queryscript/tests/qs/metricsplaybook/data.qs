let src_contract_stream = load('raw_csvs/src_contract_stream.csv');
let src_dim_customer = load('raw_csvs/src_dim_customer.csv');

mat contract_stream =
	select
		id,
		customer_id,
		activity,
		plan_type,
		timestamp,
		row_number() over(partition by customer_id, activity order by timestamp asc) as activity_occurrence,
		-- XXX: Need to support lead()
		-- lead(timestamp, 1) over(partition by customer_id, activity order by timestamp asc) as activity_repeated_at
		timestamp AS activity_repeated_at
	from
		src_contract_stream;


mat dim_customer =
	select *,
	-- XXX need to support substring, date_trunc, etc.
	-- substring(date_trunc('month', first_contract_signed_date)::text, 1, 7) as cohort
	'todo' as cohort
	from src_dim_customer;


SELECT * FROM contract_stream ORDER BY id LIMIT 5;
SELECT * FROM dim_customer ORDER BY id LIMIT 5;
