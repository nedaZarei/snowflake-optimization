
    
    

select
    benchmark_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.benchmarks
where benchmark_id is not null
group by benchmark_id
having count(*) > 1


