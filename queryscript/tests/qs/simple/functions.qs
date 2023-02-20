import * from inference;

select min(id) from users;
select max(id) from users;
select count(id) from users;
select count(*) from users;
select sum(id) from users;
select avg(id) from users;
select array_agg(id) from users;

select concat(id::text, 'a') from users;
select concat(id::text, 'a', 'b') from users;
select concat(id::text, 'a', 'b', 'c') from users;

select __native_identity(id) from users;
