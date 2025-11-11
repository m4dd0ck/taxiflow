-- fct_trips.sql
-- main fact table for taxi trip analysis
-- this is the heart of the data model - everything rolls up from here

{#
    using delete+insert because duckdb doesn't support merge
    it's less efficient than merge but works reliably
#}
{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='delete+insert'
    )
}}

with trips as (
    select * from {{ ref('int_trips_enriched') }}

    -- incremental logic: only process new trips
    -- comparing on pickup_datetime since that's what we filter on in staging
    {% if is_incremental() %}
    where pickup_datetime > (
        select coalesce(max(pickup_datetime), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),

final as (
    select
        -- generating a surrogate key from the business keys
        -- there's no natural primary key in the source data (no trip_id)
        -- so we hash a combination of fields that should be unique together
        -- this is a bit fragile but it's the best we can do
        {{ dbt_utils.generate_surrogate_key([
            'taxi_type',
            'vendor_id',
            'pickup_datetime',
            'dropoff_datetime',
            'pickup_location_id',
            'dropoff_location_id',
            'passenger_count',
            'trip_distance',
            'fare_amount',
            'tip_amount',
            'total_amount'
        ]) }} as trip_id,

        -- Dimension keys
        -- matching the format used in dim_date (YYYYMMDD as integer)
        cast(strftime(pickup_datetime, '%Y%m%d') as integer) as pickup_date_key,
        -- matching dim_time format (HHMM as integer)
        cast(strftime(pickup_datetime, '%H') as integer) * 100 + cast(strftime(pickup_datetime, '%M') as integer) as pickup_time_key,
        cast(strftime(dropoff_datetime, '%Y%m%d') as integer) as dropoff_date_key,
        cast(strftime(dropoff_datetime, '%H') as integer) * 100 + cast(strftime(dropoff_datetime, '%M') as integer) as dropoff_time_key,
        -- coalesce to 0 for unknown locations (maps to our unknown record in dim_location)
        coalesce(pickup_location_id, 0) as pickup_location_id,
        coalesce(dropoff_location_id, 0) as dropoff_location_id,
        rate_code_id,
        payment_type_id,

        -- Attributes (degenerate dimensions)
        taxi_type,
        vendor_id,
        passenger_count,

        -- Measures
        trip_distance,
        trip_duration_minutes,
        fare_amount,
        tip_amount,
        total_amount,

        -- Derived measures (calculated in int_trips_enriched)
        tip_percentage,
        avg_speed_mph,
        fare_per_mile,

        -- keeping the raw timestamps for debugging and ad-hoc queries
        -- technically redundant with the dimension keys but useful
        pickup_datetime,
        dropoff_datetime,
        _loaded_at

    from trips
),

-- handle duplicates that slip through from the source
-- this happens occasionally with the nyc tlc data
-- I spent way too long debugging this before I realized the source has dupes
deduplicated as (
    select *,
        row_number() over (partition by trip_id order by _loaded_at) as row_num
    from final
)

select
    trip_id,
    pickup_date_key,
    pickup_time_key,
    dropoff_date_key,
    dropoff_time_key,
    pickup_location_id,
    dropoff_location_id,
    rate_code_id,
    payment_type_id,
    taxi_type,
    vendor_id,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    total_amount,
    tip_percentage,
    avg_speed_mph,
    fare_per_mile,
    pickup_datetime,
    dropoff_datetime,
    _loaded_at
from deduplicated
where row_num = 1
-- TODO: should probably log how many duplicates we're dropping
-- would be good to track data quality over time
