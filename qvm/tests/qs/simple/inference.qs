export let users = load('users.json');
export let events = load('events.json');

select * from users order by id;
select * from events order by ts;
