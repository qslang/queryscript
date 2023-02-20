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
    -- select null returns a strange type in duckdb
    -- https://github.com/qscl/queryscript/issues/73
    -- null as metric_denominators,
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

SELECT COUNT(*) FROM cte_grouping_sets;

let cte_final = select
        'churned_revenue_cube' as metric_model,
        False as is_snapshot_reliant_metric,
        'timestamp' as anchor_date,
        case
          when month_bit = 0 then 'month'
          when day_bit = 0 then 'day'
          end as date_grain,
        case
          when month_bit = 0 then metric_month
          when day_bit = 0 then metric_day
          end as metric_date,
        case
          when combination_1_bit = 0 then combination_1
          when combination_2_bit = 0 then combination_2
          when combination_3_bit = 0 then combination_3
          when total_bit = 0 then total_object
          end as slice_object,
        case
          when combination_1_bit = 0 then concat(ifnull(json_extract_string(slice_object, '$.dim_name'), 'null'))
          when combination_2_bit = 0 then concat(ifnull(json_extract_string(slice_object, '$.dim_name'), 'null'))
          when combination_3_bit = 0 then concat(ifnull(json_extract_string(slice_object, '$.dim_name'), 'null'))
          when total_bit = 0 then 'total'
          end as slice_dimension,
        case
          when combination_1_bit = 0 then concat(ifnull(json_extract_string(slice_object, '$.dim_value'), 'null'))
          when combination_2_bit = 0 then concat(ifnull(json_extract_string(slice_object, '$.dim_value'), 'null'))
          when combination_3_bit = 0 then concat(ifnull(json_extract_string(slice_object, '$.dim_value'), 'null'))
          when total_bit = 0 then 'Total'
          end as slice_value,
        metric_calculation,
        case
          when /* metric_denominators != 0 and */ metric_value is null then 0
          else metric_value
        end as metric_value
    from
      cte_grouping_sets;

select count(*) from cte_final;
