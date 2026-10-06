-- int_trips_enriched.sql
-- adds calculated fields that we'll need in the fact table and marts
-- keeping these calculations in one place so they're consistent everywhere

{{
    config(
        materialized='view'
    )
}}

with trips as (
    select * from {{ ref('int_trips_union') }}
),

enriched as (
    select
        -- Pass through all columns
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
        _loaded_at,

        -- trip duration calculation
        -- using epoch to get seconds then converting to minutes
        -- duckdb's timestamp arithmetic is a bit different from postgres
        round(
            extract(epoch from (dropoff_datetime - pickup_datetime)) / 60.0,
            2
        ) as trip_duration_minutes,

        -- tip percentage - only meaningful for credit card payments (payment_type_id = 1)
        -- cash tips aren't recorded in the data
        -- this is why yellow cab tip stats are skewed toward credit card users
        case
            when payment_type_id = 1 and fare_amount > 0
            then round((tip_amount / fare_amount) * 100, 2)
            else 0
        end as tip_percentage,

        -- avg speed - useful for traffic analysis
        -- need at least 60 seconds to get a meaningful speed calculation
        -- otherwise short trips at red lights would show 0 mph or crazy high values
        case
            when extract(epoch from (dropoff_datetime - pickup_datetime)) > 60
                and trip_distance > 0
            then round(
                trip_distance / (extract(epoch from (dropoff_datetime - pickup_datetime)) / 3600.0),
                2
            )
            else null  -- null instead of 0 so it doesn't skew averages
        end as avg_speed_mph,

        -- fare per mile - good for comparing zones
        case
            when trip_distance > 0
            then round(fare_amount / trip_distance, 2)
            else null
        end as fare_per_mile

    from trips
    -- filter out trips that are too short or too long
    -- 1 minute minimum catches test trips and meter errors
    -- 4 hour max catches trips where the meter was left running
    -- these thresholds are based on what I saw in the data distribution
    where extract(epoch from (dropoff_datetime - pickup_datetime)) between 60 and 14400
)

select * from enriched
-- trips over 100 mph are gps or meter errors, not real taxi rides
-- speed is null when distance is 0 or the trip is exactly 60 seconds; keep those
where avg_speed_mph is null
   or avg_speed_mph <= 100
