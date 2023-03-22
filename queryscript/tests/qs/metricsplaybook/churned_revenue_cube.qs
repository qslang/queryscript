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
    sum,
    'revenue_impact',
);

let new_revenue_cube = metrics_cube(
    ['month', 'day'],
    ['segment', 'channel', 'plan_type'],
    activity_filter('new_contract_started'),
    sum,
    'revenue_impact',
);

let contraction_revenue_cube = metrics_cube(
    ['month', 'day'],
    ['segment', 'channel', 'plan_type'],
    activity_filter('contraction_contract_started'),
    sum,
    'revenue_impact',
);


let expansion_contract_started = metrics_cube(
    ['month', 'day'],
    ['segment', 'channel', 'plan_type'],
    activity_filter('expansion_contract_started'),
    sum,
    'revenue_impact',
);

SELECT COUNT(*) FROM churned_revenue_cube;
SELECT COUNT(*) FROM new_revenue_cube;
SELECT COUNT(*) FROM contraction_revenue_cube;
SELECT COUNT(*) FROM expansion_contract_started;


let churned_bar_graph = churned_revenue_cube WITH VIZ {
    mark: 'bar',
    encoding: {
        x: { field: 'metric_date' , type: 'nominal'    },
        y: { field: 'metric_value', type: 'quantitative' },
    },
};
