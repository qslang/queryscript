import * from schema;

-- Aggs

SELECT COUNT(*) FROM users;
SELECT COUNT(1) FROM users;
SELECT COUNT(DISTINCT org_id) FROM users;

-- Joins

select * from users join events on true;

select * from users join users u2 on true;
select * from users join users u2 on users.id = u2.id;
select u2.id from users join users u2 on users.id = u2.id ORDER BY u2.id;

-- Scalar subselects

SELECT id, (SELECT MAX(id) FROM users) max_user FROM users;
SELECT id FROM users WHERE id = (SELECT MAX(id) FROM users);
select description, (select name from users where users.id = events.user_id) from events order by ts;

-- EXCLUDE, EXCEPT, RENAME

SELECT * EXCEPT (id) FROM users ORDER BY name;
SELECT * EXCEPT (id, org_id) FROM users ORDER BY name;
SELECT * EXCLUDE id FROM users ORDER BY name;
SELECT * EXCLUDE (id, org_id) FROM users ORDER BY name;

SELECT * RENAME (id AS user_id) FROM users ORDER BY id;
SELECT * REPLACE (id+1 AS id, 'foo' as org_id) FROM users ORDER BY id;
SELECT * REPLACE (id+1 AS id, id+10 AS id) FROM users ORDER BY id;

-- Should fail
SELECT * REPLACE (id+1 AS dne, 'foo' as org_id) FROM users ORDER BY id;

SELECT
	id, 1,
FROM users
;

SELECT 1, 2, ;

select id, 1 as a, FROM users GROUP BY id, a, ;
