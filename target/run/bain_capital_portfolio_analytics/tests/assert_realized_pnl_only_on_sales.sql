
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Realized P&L should only exist for SALE trades

select *
from DBT_DEMO.DEV_pipeline_b.int_trade_pnl
where trade_category != 'SALE'
  and realized_pnl is not null
  and realized_pnl != 0
  
  
      
    ) dbt_internal_test