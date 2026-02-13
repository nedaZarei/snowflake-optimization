
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cashflow_date
from DBT_DEMO.DEV_pipeline_a.stg_cashflows
where cashflow_date is null



  
  
      
    ) dbt_internal_test