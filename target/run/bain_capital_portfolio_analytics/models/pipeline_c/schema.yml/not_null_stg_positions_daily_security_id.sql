
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select security_id
from DBT_DEMO.DEV_pipeline_c.stg_positions_daily
where security_id is null



  
  
      
    ) dbt_internal_test