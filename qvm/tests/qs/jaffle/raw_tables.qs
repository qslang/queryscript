type raw_order {
    id int,
    user_id int,
    order_date date,
    status varchar,
}

type raw_payment {
    id int,
    order_id int,
    payment_method varchar,
    amount int,
}

type raw_customer {
    id int, -- TODO: Need a way to specify PK?
    first_name varchar,
    last_name varchar,
}

-- TODO: Need a way to specify foreign keys:
--  orders.user_id => customers.id
--  payments.order_id => orders.id

export let raw_orders [raw_order] = load('data/raw_orders.csv');
export let raw_payments [raw_payment] = load('data/raw_payments.csv');
export let raw_customers [raw_customer] = load('data/raw_customers.csv');

select * from raw_orders LIMIT 10;
select * from raw_payments LIMIT 10;
select * from raw_customers LIMIT 10;
