
    
    

select
    position_key as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV_pipeline_b.fact_portfolio_positions
where position_key is not null
group by position_key
having count(*) > 1


