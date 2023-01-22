-- Variables (including relations) are namespaced and can be imported. The below queries will
-- work for any definition of `users` and `events` as long as they typecheck.
import users, events from schema;

-- You can define variables (including scalars and relations)
-- using `let <name> [<type>] = <expr>`. These expressions are
-- lazily evaluated (and potentially inlined), so there is no
-- cost to defining them.
let active_users = SELECT * FROM users WHERE active;

-- You can run queries inline to help with debugging
SELECT COUNT(*) FROM active_users;

-- You can define functions that operate over values. Just like
-- variables, functions can be inlined and pushed down into the
-- target database, or evaluated directly.
fn username(user_id bigint) {
    SELECT name FROM active_users WHERE id = user_id
}

-- Find the users with the most events
SELECT username(user_id), COUNT(*) FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Find the users who are at risk of churn
SELECT username(user_id), MAX(ts) FROM events GROUP BY 1 HAVING MAX(ts) < NOW() - INTERVAL 1 MONTH;
