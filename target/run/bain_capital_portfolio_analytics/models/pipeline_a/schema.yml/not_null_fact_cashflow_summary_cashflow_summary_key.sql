
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cashflow_summary_key
from DBT_DEMO.DEV_pipeline_a.fact_cashflow_summary
where cashflow_summary_key is null



  
  
      
    ) dbt_internal_test