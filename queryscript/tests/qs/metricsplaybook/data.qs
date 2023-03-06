-- We define types here because:
-- 1. The CSV parser doesn't recognize first_contract_signed_date as a timestamp
-- 2. We omit the (ignored and later replaced) activity_occurence and activity_repeated_at fields
-- 	  in ContractStream
type ContractStream {
	id bigint,
	customer_id bigint,
	timestamp timestamp,
	activity text,
	revenue_impact double,
	plan_type text,
    activity_occurrence bigint,
    activity_repeated_at timestamp,
}
type DimCustomer {
	id bigint,
	segment text,
	channel text,
	first_contract_signed_date timestamp,
};

let src_contract_stream [ContractStream] = load('raw_csvs/src_contract_stream.csv');
let src_dim_customer [DimCustomer] = load('raw_csvs/src_dim_customer.csv');

export mat contract_stream =
	select
		*
        REPLACE (
            row_number() over(partition by customer_id, activity order by timestamp asc) as activity_occurrence,
            lead(timestamp, 1) over(partition by customer_id, activity order by timestamp asc) as activity_repeated_at
        )
	from
		src_contract_stream;


export mat dim_customer =
	select *,
	-- TODO: We had to add a timestamp cast here, which is not in the original metrics playbook, because
	-- we do not coerce function argument types (or do not coerce string -> timestamp)
	-- https://github.com/qscl/queryscript/issues/68
	substring(date_trunc('month', first_contract_signed_date)::text, 1, 7) as cohort
	from src_dim_customer;


SELECT * FROM contract_stream ORDER BY id LIMIT 5;
SELECT * FROM dim_customer ORDER BY id LIMIT 5;
