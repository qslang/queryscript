import schema;
import inference;

fn foo<R>(a R) {
    a
}

foo(1);
foo(5.0);

fn bar<R>(a R) {
    a+1
}

fn rel_id<R>(a R) {
    a.id
}

fn rel_foo<R>(a R) {
    a.foo
}

select * from schema.users u where rel_id(u) = 1;
select * from inference.users u where rel_id(u) = 1;

select * from schema.users u where rel_foo(u) = 1;
select * from inference.users u where rel_foo(u) = 1;

fn identity<R>(u R) -> R {
    u
}

identity(schema.users);

bar(5);
bar(5.5);

fn baz<A, B, C>(a A, b B, c C) {
    a + b + c
}

baz(1, 3.0, 'asdf');

-- These both fail with a runtime error
1 + 3.0 + null;
baz(1, 3.0, null);

baz(1, 3.0, 123);
