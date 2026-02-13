
    
    

select
    cashflow_summary_key as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV_pipeline_a.fact_cashflow_summary
where cashflow_summary_key is not null
group by cashflow_summary_key
having count(*) > 1


