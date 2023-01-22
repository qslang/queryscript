import raw_orders, raw_payments, raw_customers from raw_tables;

export let orders =
select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from raw_orders;

export let payments =
select
    id as payment_id,
    order_id,
    payment_method,

    -- The jaffle repo divides by 100, which in Snowflake will always return
    -- a double, but in postgres and datafusion, returns an int
    amount / 100.0 as amount
from raw_payments;

export let customers =
select
    id as customer_id,
    first_name,
    last_name
from raw_customers;

SELECT * FROM orders LIMIT 10;
SELECT * FROM payments LIMIT 10;
SELECT * FROM customers LIMIT 10;

SELECT COUNT(*) FROM orders;
SELECT count(*) FROM payments;
SELECT Count(*) FROM customers;
