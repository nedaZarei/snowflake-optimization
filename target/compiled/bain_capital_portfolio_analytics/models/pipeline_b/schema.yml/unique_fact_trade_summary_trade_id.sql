
    
    

select
    trade_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV_pipeline_b.fact_trade_summary
where trade_id is not null
group by trade_id
having count(*) > 1


