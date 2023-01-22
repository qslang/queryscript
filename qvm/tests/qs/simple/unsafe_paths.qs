import schema;

unsafe select * from schema.users;

let bar = schema.users;
unsafe select * from bar;

let baz = 1;

-- NOTE: This will have a weird alias (e.g. $1), because we don't
-- really distinguish between the value and the alias in unsafe mode
unsafe select baz from bar;

-- This will have the correct alias
unsafe select baz as baz from bar;
