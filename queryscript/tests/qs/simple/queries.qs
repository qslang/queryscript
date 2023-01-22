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

-- Unsupported
select u2.* from users join users u2 on users.id = u2.id;

-- Should error
select * from users join users on true;
select count(*) from users, users u2 where users.id = u2.a;
select description, (select * from users where users.id = events.user_id) from events order by ts;
