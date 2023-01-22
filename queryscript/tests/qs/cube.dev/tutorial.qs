-- This tutorial follows https://cube.dev/docs/schema/getting-started
-- which answers several questions about paying users

type User {
    id bigint,
    paying bool,
    city text,
    company_name text,
}

-- 1. Creating the "cube"
export let users [User] = load('users.csv');

-- 2. Adding Measures and Dimensions
SELECT count(*) FROM users;
SELECT city, count(id) FROM users GROUP BY 1;

-- 3. Adding Filters to Measures
SELECT COUNT(*) FROM users WHERE paying;

-- 4. Using Calculated Measures
SELECT
    100.0*COUNT(CASE WHEN paying THEN users.id END)/COUNT(users.id)
FROM
    users;
