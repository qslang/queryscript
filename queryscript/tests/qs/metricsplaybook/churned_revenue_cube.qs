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

let date_slices = ['month', 'day'];
let metric_slices = ['segment', 'channel', 'plan_type'];

let cte_grouping_sets =
  select
    for slice in date_slices {
      date_trunc(slice, timestamp)::date as f"metric_{slice}",
      grouping(f"metric_{slice}") as f"{slice}_bit",
    },


    for slice in metric_slices {
      concat('{"dim_name": "', slice, '", "dim_value": "', f"{slice}", '"}') as f"combination_{slice}",
      grouping(f"combination_{slice}") as f"{slice}_bit",
    },

    'Total' as total_object,
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
    for date_s in date_slices, metric_s in metric_slices {
      (f"metric_{date_s}", f"combination_{metric_s}"),
    },
    for date_s in date_slices {
      (f"metric_{date_s}", "total_object"),
    },
  )
;

SELECT COUNT(*) FROM cte_grouping_sets;

let cte_final = select
        'churned_revenue_cube' as metric_model,
        False as is_snapshot_reliant_metric,
        'timestamp' as anchor_date,
        case
          for slice in date_slices {
            when f"{slice}_bit" = 0 then slice
          }
          end as date_grain,
        case
          for slice in date_slices {
            when f"{slice}_bit" = 0 then f"metric_{slice}"
          }
          end as metric_date,
        case
          for slice in metric_slices {
            when f"{slice}_bit" = 0 then f"combination_{slice}"
          }
          end as slice_object,
        case
          for slice in metric_slices {
            when f"{slice}_bit" = 0 then concat(ifnull(json_extract_string(slice_object, '$.dim_name'), 'null'))
          }
          when total_bit = 0 then 'total'
          end as slice_dimension,
        case
          for slice in metric_slices {
            when f"{slice}_bit" = 0 then concat(ifnull(json_extract_string(slice_object, '$.dim_value'), 'null'))
          }
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
