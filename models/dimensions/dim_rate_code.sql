-- dim_rate_code.sql
-- rate codes determine how the fare is calculated
-- most trips are standard rate (1) but airport trips use flat rates

{{
    config(
        materialized='table'
    )
}}

with rate_codes as (
    select * from {{ ref('rate_codes') }}
),

final as (
    select
        rate_code_id,
        rate_code_name,
        description
    from rate_codes
)

select * from final
-- rate code 99 is our "unknown" value for nulls in the source data
-- should already be in the seed file
