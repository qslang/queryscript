import staging;

let order_payments =
    select
        order_id,

        -- TODO: DBT does this with a macro variable (payment_methods) and a loop which
        -- spits out one projection term per value.
        sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
        sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount,

        sum(amount) as total_amount
    from staging.payments
    group by order_id
;

export let orders =
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        order_payments.credit_card_amount,
        order_payments.coupon_amount,
        order_payments.bank_transfer_amount,
        order_payments.gift_card_amount,

        order_payments.total_amount as amount

    from staging.orders
    left join order_payments
        on orders.order_id = order_payments.order_id
;

SELECT COUNT(*) FROM orders;
