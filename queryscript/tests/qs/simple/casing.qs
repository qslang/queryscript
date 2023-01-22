import users from schema;

let users_unq_upper = SELECT id AS ID, org_id AS ORG_ID from users;
let users_q_upper = SELECT id AS "ID", org_id AS "ORG_ID" from users;

SELECT id, org_id FROM users_unq_upper;
SELECT Id, ORG_ID FROM users_unq_upper;
SELECT "Id", "ORG_ID" FROM users_unq_upper;
SELECT ID, ORG_ID FROM users_unq_upper;
SELECT "ID", "ORG_ID" FROM users_unq_upper;

SELECT id, org_id FROM users_q_upper;
SELECT Id, ORG_ID FROM users_q_upper;
SELECT "Id", "ORG_ID" FROM users_q_upper;
SELECT ID, ORG_ID FROM users_q_upper;
SELECT "ID", "ORG_ID" FROM users_q_upper;

-- This should work
SELECT
 ID,
 (SELECT COUNT(DISTINCT ID) FROM users_unq_upper i WHERE i.ORG_ID = o.ORG_ID LIMIT 1) NUM_USERS
FROM
    users_unq_upper o;

-- This should error if case sensitive
SELECT
 ID,
 (SELECT COUNT(DISTINCT ID) FROM users_q_upper i WHERE i.ORG_ID = o.ORG_ID LIMIT 1) NUM_USERS
FROM
    users_q_upper o;

-- But this should work
SELECT
 ID,
 (SELECT COUNT(DISTINCT "ID") FROM users_q_upper i WHERE i."ORG_ID" = o."ORG_ID" LIMIT 1) NUM_USERS
FROM
    users_q_upper o;
