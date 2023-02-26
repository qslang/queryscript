import users from schema;

SELECT for item in [id, org_id] {
	item
} FROM users;

SELECT for item in [id, org_id] {
	item AS item
} FROM users;


SELECT for item in [id, org_id] {
    users.*
} FROM users;

SELECT for (item in [id, org_id]) {
    item,
    item+1
} FROM users;

SELECT for item in [1, 2] {
    item
} FROM users;

let slices = ['month', 'day'];

select
    for item in slices {
        item
    }
;

/*
SELECT foreach ([a, b] AS item) {
    item AS IDENT("metric_", item)
} FROM foo;
*/

/*
SELECT for item in [users, org_id] {
    item
} FROM foo
GROUP BY for item in [users, org_id] {
    item
};
*/
