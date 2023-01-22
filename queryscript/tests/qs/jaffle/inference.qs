export let orders = load('data/raw_orders.csv');

SELECT * FROM orders ORDER BY id LIMIT 10;
SELECT MAX(order_date) FROM orders;
