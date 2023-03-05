import users from schema;

WITH u AS (SELECT MAX(id) id FROM users)
    SELECT users.* FROM users JOIN u ON true WHERE users.id = u.id;
