fn bar<R>(value R) -> R {
    value + 1
}

fn metrics_cube<R, S>(source R, metric_fn S) {
    select
      metric_fn("revenue_impact") as metric_value
    from
      source
}

let foo = SELECT 1 as revenue_impact;
metrics_cube(
    foo,
    bar,
);

metrics_cube(
    foo,
    abs,
);
