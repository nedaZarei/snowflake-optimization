
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valuation_date
from DBT_DEMO.DEV_pipeline_c.fact_portfolio_performance
where valuation_date is null



  
  
      
    ) dbt_internal_test