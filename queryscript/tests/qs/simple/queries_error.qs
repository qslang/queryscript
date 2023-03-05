-- Unsupported
select u2.* from users join users u2 on users.id = u2.id;

-- Should error
select * from users join users on true;
select count(*) from users, users u2 where users.id = u2.a;
select description, (select * from users where users.id = events.user_id) from events order by ts;
WITH u AS (SELECT * FROM users), u AS (SELECT * FROM users) SELECT * FROM u;
