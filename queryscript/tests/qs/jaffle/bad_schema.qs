type raw_order {
    bool_value bool,
}

export let orders [raw_order] = load('data/raw_orders.csv');

-- This should throw a runtime error
SELECT MAX(bool_value) FROM orders;
