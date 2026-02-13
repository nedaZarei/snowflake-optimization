
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_id
from DBT_DEMO.DEV_pipeline_b.int_trade_pnl
where trade_id is null



  
  
      
    ) dbt_internal_test