
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticker
from DBT_DEMO.DEV_pipeline_b.stg_securities
where ticker is null



  
  
      
    ) dbt_internal_test