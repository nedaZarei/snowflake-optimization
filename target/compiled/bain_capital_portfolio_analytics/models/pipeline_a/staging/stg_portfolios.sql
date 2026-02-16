-- Pipeline A: Simple Cashflow Pipeline
-- Model: stg_portfolios
-- Description: Staging model for portfolio master data
--

with source as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        fund_id,
        inception_date,
        status,
        aum_usd,
        row_number() over (
            partition by portfolio_id
            order by portfolio_id desc
        ) as rn
    from BAIN_ANALYTICS.DEV.SAMPLE_PORTFOLIOS
),

deduplicated as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        fund_id,
        inception_date,
        status,
        aum_usd
    from source
    where rn = 1
),

active_only as (
    select *
    from deduplicated
    where status = 'ACTIVE'
)

select * from active_only