
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        trade_category as value_field,
        count(*) as n_records

    from DBT_DEMO.DEV_pipeline_b.stg_trades
    group by trade_category

)

select *
from all_values
where value_field not in (
    'PURCHASE','SALE','INCOME','OTHER'
)



  
  
      
    ) dbt_internal_test