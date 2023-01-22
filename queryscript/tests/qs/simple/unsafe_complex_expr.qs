import * from inference;

-- This tests to make sure that _after_ resolving schemas, but _before_ resolving unsafe expressions, the compiler has
-- enough oxygen to infer types (e.g. for the complex_expr variable in the case below).
--
-- I kind of caught this test case like lightning in a bottle. It is a bit hard / hacky to get it to reproduce
-- like this. But the basic intuition is that if there's enough complexity in the expression, we'll have some
-- work to do between figuring out the type of users (via schema inference) and figuring out the type of complex_expr
-- that we'll fail to compile the unsafe query below if we don't let the handles run in between.
let complex_expr  = SELECT
    id,
    org_id,
    name,
    active,
    id > 0 or active,
    (SELECT id FROM users i WHERE i.id = o.id AND i.org_id = o.org_id AND i.active AND o.active) max_user
  FROM users o
  WHERE active AND id < 1 AND id > 5
;

unsafe select * from complex_expr;
