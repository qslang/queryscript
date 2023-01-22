import * from schema;

let star = SELECT * FROM users;
let free_var = 123;
let no_from = SELECT free_var;
let unbound = SELECT free_var FROM events;
let unqualified_ts = SELECT ts FROM events;
let unqualified_ts_alias = SELECT ts FROM events e;
let qualified_ts = SELECT events.ts FROM events;
let qualified_ts_alias = SELECT e.ts FROM events e;
