
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select portfolio_id
from DBT_DEMO.DEV_pipeline_c.int_portfolio_returns_daily
where portfolio_id is null



  
  
      
    ) dbt_internal_test