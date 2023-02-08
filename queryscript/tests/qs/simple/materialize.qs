import users from schema;

mat foo = SELECT org_id, COUNT(*) c FROM users GROUP BY 1;

SELECT MAX(c) FROM foo;
