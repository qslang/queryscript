import schema;
import inference;

fn foo<R>(a R) {
    a
}

foo(1);
foo(5.0);

-- This function will fail to compile because coerce requires types to be known
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
