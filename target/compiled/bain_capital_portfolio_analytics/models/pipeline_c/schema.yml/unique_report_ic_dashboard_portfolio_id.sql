
    
    

select
    portfolio_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV_pipeline_c.report_ic_dashboard
where portfolio_id is not null
group by portfolio_id
having count(*) > 1


