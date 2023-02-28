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

SELECT for item in [id, org_id] {
    item AS f"metric_{item}"
} FROM users;

let strings = ['id', 'org_id'];
let numbers = ['id', 'org_id'];

strings;
SELECT strings;

numbers;
SELECT numbers;

SELECT for item in numbers {
    item
} FROM users;


SELECT for item in strings {
    item
} FROM users;

-- Idents
SELECT for item in strings {
    f"{item}"
} FROM users;

let id_types = ['', 'org_'];
SELECT for item in id_types {
    f"{item}id"
} FROM users;

SELECT for item in [id, org_id] {
    item
} FROM users
GROUP BY for item in [id, org_id] {
    item
};

select
    case
        for item in strings {
            when f"{item}"=1 then item
        }
    end as "foo"
from users
;
