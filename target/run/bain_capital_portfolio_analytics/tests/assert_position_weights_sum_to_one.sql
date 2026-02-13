
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Position weights should sum to approximately 100% per portfolio per day
-- Allows for small rounding differences

with weight_totals as (
    select
        portfolio_id,
        position_date,
        sum(weight_pct) as total_weight
    from DBT_DEMO.DEV_pipeline_c.fact_position_snapshot
    group by portfolio_id, position_date
)

select *
from weight_totals
where abs(total_weight - 1.0) > 0.05  -- Allow 5% tolerance for cash/derivatives
  
  
      
    ) dbt_internal_test