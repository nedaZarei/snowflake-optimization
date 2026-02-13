
    
    

select
    broker_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.brokers
where broker_id is not null
group by broker_id
having count(*) > 1


