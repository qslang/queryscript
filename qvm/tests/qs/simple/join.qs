import * from inference;

let foo = select * from (select * from users join events on users.id = events.user_id);

SELECT user_id FROM foo;
