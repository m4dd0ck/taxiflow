-- stg_green_trips.sql
-- staging model for green taxi data
-- green taxis operate in outer boroughs where yellow cabs are less common
-- schema is mostly the same as yellow but with some differences (noted below)

{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nyc_tlc', 'green_tripdata') }}
),

renamed as (
    select
        -- Identifiers
        VendorID as vendor_id,
        coalesce(RatecodeID, 99)::integer as rate_code_id,
        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,
        coalesce(payment_type, 0)::integer as payment_type_id,

        -- Timestamps
        -- green taxis use "lpep" prefix instead of "tpep"
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,

        -- Trip attributes
        coalesce(passenger_count, 1)::integer as passenger_count,
        coalesce(trip_distance, 0)::double as trip_distance,
        store_and_fwd_flag,

        -- Fare components
        coalesce(fare_amount, 0)::double as fare_amount,
        coalesce(extra, 0)::double as extra,
        coalesce(mta_tax, 0)::double as mta_tax,
        coalesce(tip_amount, 0)::double as tip_amount,
        coalesce(tolls_amount, 0)::double as tolls_amount,
        coalesce(improvement_surcharge, 0)::double as improvement_surcharge,
        coalesce(total_amount, 0)::double as total_amount,
        coalesce(congestion_surcharge, 0)::double as congestion_surcharge,
        -- green taxis don't have an airport_fee column in the source
        -- hardcoding to 0 so schema matches yellow trips for the union
        0::double as airport_fee,

        -- Metadata
        'green' as taxi_type,
        current_timestamp as _loaded_at

    from source
)

select * from renamed
-- same filters as stg_yellow_trips for consistency
-- should probably extract these into a macro at some point
where pickup_datetime is not null
  and dropoff_datetime is not null
  and pickup_datetime < dropoff_datetime
  and pickup_datetime >= '{{ var("start_date") }}'
  and pickup_datetime < '{{ var("end_date") }}'::date + interval '1 day'
  and trip_distance >= 0
  and fare_amount >= 0
  and fare_amount < 1000
