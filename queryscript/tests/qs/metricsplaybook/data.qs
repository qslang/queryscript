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
		lead(timestamp, 1) over(partition by customer_id, activity order by timestamp asc) as activity_repeated_at
	from
		src_contract_stream;


mat dim_customer =
	select *,
	-- TODO: We had to add a timestamp cast here, which is not in the original metrics playbook, because
	-- we do not coerce function argument types (or do not coerce string -> timestamp)
	-- https://github.com/qscl/queryscript/issues/68
	substring(date_trunc('month', first_contract_signed_date::timestamp)::text, 1, 7) as cohort
	from src_dim_customer;


SELECT * FROM contract_stream ORDER BY id LIMIT 5;
SELECT * FROM dim_customer ORDER BY id LIMIT 5;
