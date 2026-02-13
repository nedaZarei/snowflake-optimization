
    
    

select
    sector_performance_key as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV_pipeline_c.fact_sector_performance
where sector_performance_key is not null
group by sector_performance_key
having count(*) > 1


