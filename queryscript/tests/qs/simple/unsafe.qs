import * from schema;

-- Aggs

unsafe SELECT COUNT(*) FROM users;
unsafe SELECT COUNT(1) FROM users;
unsafe SELECT COUNT(DISTINCT org_id) FROM users;

-- Joins

unsafe select * from users join events on true;

unsafe select * from users join users u2 on true;
unsafe select * from users join users u2 on users.id = u2.id;
unsafe select u2.id from users join users u2 on users.id = u2.id ORDER BY u2.id;

-- Unsupported
unsafe select u2.* from users join users u2 on users.id = u2.id;

-- Should error
unsafe select * from users join users on true;
unsafe select count(*) from users, users u2 where users.id = u2.a;
unsafe select 1 as "a", 2 as "a", '3' as "a";
