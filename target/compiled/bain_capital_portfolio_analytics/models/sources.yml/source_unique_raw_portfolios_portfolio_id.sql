
    
    

select
    portfolio_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.portfolios
where portfolio_id is not null
group by portfolio_id
having count(*) > 1


