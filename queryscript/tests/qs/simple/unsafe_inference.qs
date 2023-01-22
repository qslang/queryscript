import * from inference;

let user_count = SELECT COUNT(*) FROM users;

SELECT COUNT(*) FROM user_count;
unsafe SELECT COUNT(*) FROM user_count;

let ordered_events = unsafe SELECT ROW_NUMBER() OVER (ORDER BY ts) rn FROM events;

SELECT MIN(rn), MAX(rn) FROM ordered_events;
unsafe SELECT MIN(rn), MAX(rn) FROM ordered_events;

-- Define the types explicitly
type EventRange {
    bottom bigint,
    top bigint,
};

let correct_type_safe [EventRange] = SELECT MIN(rn) "bottom", MAX(rn) "top" FROM ordered_events;
let correct_type_unsafe [EventRange] = unsafe SELECT MIN(rn) "bottom", MAX(rn) "top" FROM ordered_events;

correct_type_safe;
correct_type_unsafe;

type BogusEventRange {
    bottom int,
    top text,
}

select 'bogus types';
let bogus_type_unsafe [BogusEventRange] = unsafe SELECT MIN(rn) "bottom", MAX(rn) "top" FROM ordered_events;
SELECT * FROM bogus_type_unsafe;
