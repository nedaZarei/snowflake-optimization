
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_date
from DBT_DEMO.DEV_pipeline_b.stg_trades
where trade_date is null



  
  
      
    ) dbt_internal_test