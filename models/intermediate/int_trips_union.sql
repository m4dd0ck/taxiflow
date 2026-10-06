-- int_trips_union.sql
-- combines yellow and green taxi trips into a single stream
-- this is the foundation for all downstream trip analysis

{{
    config(
        materialized='view'
    )
}}

with yellow_trips as (
    select * from {{ ref('stg_yellow_trips') }}
),

green_trips as (
    select * from {{ ref('stg_green_trips') }}
),

-- using explicit column list instead of select * so the union doesn't depend on column order
-- this way if one source adds a new column it won't break the union
-- learned this the hard way at my previous job when a vendor added columns
unioned as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        store_and_fwd_flag,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        taxi_type,
        _loaded_at
    from yellow_trips

    union all

    -- could use dbt_utils.union_relations here but explicit is better for readability
    -- plus we have control over column order this way
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        store_and_fwd_flag,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        taxi_type,
        _loaded_at
    from green_trips
)

select * from unioned
-- note: no deduplication here - that happens in fct_trips
-- keeping the intermediate layer simple
