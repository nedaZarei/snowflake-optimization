
    
    

select
    security_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.securities
where security_id is not null
group by security_id
having count(*) > 1


