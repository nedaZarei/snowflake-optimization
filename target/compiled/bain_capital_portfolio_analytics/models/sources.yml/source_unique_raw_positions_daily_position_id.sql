
    
    

select
    position_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.positions_daily
where position_id is not null
group by position_id
having count(*) > 1


