
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fund_summary_key
from DBT_DEMO.DEV_pipeline_c.fact_fund_summary
where fund_summary_key is null



  
  
      
    ) dbt_internal_test