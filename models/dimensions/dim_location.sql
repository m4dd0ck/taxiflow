-- dim_location.sql
-- taxi zone dimension based on tlc zone definitions
-- there are 265 zones across the 5 boroughs plus airports

{{
    config(
        materialized='table'
    )
}}

with zones as (
    select * from {{ ref('stg_taxi_zones') }}
),

final as (
    select
        location_id,
        zone_name,
        borough,
        service_zone,
        false as is_unknown
    from zones

    union all

    -- adding an unknown record (location_id = 0) to handle nulls in the fact table
    -- this is a classic kimball pattern - lets us keep referential integrity
    -- while still handling dirty data gracefully
    select
        0 as location_id,
        'Unknown' as zone_name,
        'Unknown' as borough,
        'Unknown' as service_zone,
        true as is_unknown
)

select * from final
-- note: some zones have weird names like "NA" or just numbers
-- but I'm leaving them as-is since that's what tlc uses
