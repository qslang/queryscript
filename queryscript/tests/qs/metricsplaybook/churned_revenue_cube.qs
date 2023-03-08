import * from data;
import metrics_cube from cube;

fn activity_filter(activity_type string) {
      select
        c.id as customer_id,
        c.segment,
        c.channel,
        c.cohort,
        m.timestamp,
        m.revenue_impact,
        m.activity,
        m.plan_type
    from
        "contract_stream" m
        join "dim_customer" c
            on m.customer_id = c.id
    where
        m.activity = activity_type
}

let churned_revenue_cube = metrics_cube(
    ['month', 'day'],
    ['segment', 'channel', 'plan_type'],
    activity_filter('customer_churn_committed'),
);
SELECT COUNT(*) FROM churned_revenue_cube;

let new_revenue_cube = metrics_cube(
    ['month', 'day'],
    ['segment', 'channel', 'plan_type'],
    activity_filter('new_contract_started'),
);
SELECT COUNT(*) FROM new_revenue_cube;

let contraction_revenue_cube = metrics_cube(
    ['month', 'day'],
    ['segment', 'channel', 'plan_type'],
    activity_filter('contraction_contract_started'),
);
