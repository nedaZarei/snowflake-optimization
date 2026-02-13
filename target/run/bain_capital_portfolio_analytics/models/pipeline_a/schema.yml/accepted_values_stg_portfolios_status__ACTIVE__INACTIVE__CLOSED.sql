
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from DBT_DEMO.DEV_pipeline_a.stg_portfolios
    group by status

)

select *
from all_values
where value_field not in (
    'ACTIVE','INACTIVE','CLOSED'
)



  
  
      
    ) dbt_internal_test