
    
    

select
    trade_key as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV_pipeline_b.fact_trade_summary
where trade_key is not null
group by trade_key
having count(*) > 1


