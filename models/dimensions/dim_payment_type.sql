-- dim_payment_type.sql
-- pretty simple dimension - just wraps the seed file
-- keeping it as a separate model in case we need to add logic later

{{
    config(
        materialized='table'
    )
}}

with payment_types as (
    select * from {{ ref('payment_types') }}
),

final as (
    select
        payment_type_id,
        payment_name,
        -- is_cash flag is useful for filtering in reports
        -- credit card payments have tip data, cash payments don't
        is_cash
    from payment_types
)

select * from final
-- the seed has an "unknown" type (id=0) already so no need to add one here
