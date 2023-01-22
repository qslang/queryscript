import * from schema;

SELECT * FROM (SELECT * FROM users) ORDER BY id;
SELECT * FROM (SELECT * FROM users) sub ORDER BY id;

SELECT org_id FROM (SELECT id, org_id FROM users) sub ORDER BY id;

SELECT id, org_id FROM users WHERE id IN (SELECT org_id FROM users);

-- Should error
SELECT org_id FROM (SELECT org_id FROM users) sub ORDER BY id;
