import schema;

-- This query fails but returns a flaky error
-- unsafe select * from schema.users;

let bar = schema.users;
unsafe select * from bar;

let baz = 1;

-- NOTE: Without the `as` statement, this will have a weird alias, because we don't
-- really distinguish between the value and the alias in unsafe mode.
unsafe select baz as baz from bar;

-- This will have the correct alias
unsafe select baz as baz from bar;
