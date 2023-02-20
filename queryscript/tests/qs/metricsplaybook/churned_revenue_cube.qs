import * from data;

let cte_prep =
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
        m.activity = 'customer_churn_committed'
;

let cte_grouping_sets =
  select
    date_trunc('month', timestamp)::date as metric_month,
      grouping(metric_month) as month_bit,
    date_trunc('day', timestamp)::date as metric_day,
      grouping(metric_day) as day_bit,
    'Total' as total_object,
    concat('{"dim_name": "segment", "dim_value": "', segment , '"}') as combination_1,
    concat('{"dim_name": "channel", "dim_value": "', channel , '"}') as combination_2,
    concat('{"dim_name": "plan_type", "dim_value": "', plan_type , '"}') as combination_3,
    grouping(combination_1) as combination_1_bit,
    grouping(combination_2) as combination_2_bit,
    grouping(combination_3) as combination_3_bit,
    grouping(total_object) as total_bit,
    null as metric_denominators,
    'sum(revenue_impact)' as metric_calculation,
    sum(revenue_impact) as metric_value
  from
    cte_prep
  where timestamp between '2014-01-01'::timestamp and current_date() + interval 365 day
  group by grouping sets (
    (metric_month, combination_1),
      (metric_month, combination_2),
      (metric_month, combination_3),
      (metric_month, total_object),
    (metric_day, combination_1),
      (metric_day, combination_2),
      (metric_day, combination_3),
      (metric_day, total_object)
    )
;
