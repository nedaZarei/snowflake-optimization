
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select position_date
from DBT_DEMO.DEV_pipeline_c.stg_positions_daily
where position_date is null



  
  
      
    ) dbt_internal_test