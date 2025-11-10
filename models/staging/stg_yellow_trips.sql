-- stg_yellow_trips.sql
-- staging model for yellow taxi data from nyc tlc
-- the column names in the parquet files are inconsistent (camelCase vs snake_case)
-- so we standardize everything here

{{
    config(
        materialized='view'
    )
}}

-- duckdb can read parquet files directly with glob patterns which is pretty nice
-- took me a while to figure out you don't need to specify the schema
with source as (
    select * from read_parquet('data/raw/yellow/*.parquet')
),

renamed as (
    select
        -- Identifiers
        VendorID as vendor_id,
        -- rate code 99 = unknown, learned this the hard way after null values broke downstream joins
        coalesce(RatecodeID, 99)::integer as rate_code_id,
        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,
        -- payment_type 0 = unknown, same issue as rate codes
        coalesce(payment_type, 0)::integer as payment_type_id,

        -- Timestamps
        -- yellow taxis use "tpep" prefix (taxi & limousine commission passenger enhancement program)
        -- green taxis use "lpep" - I always forget which is which
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,

        -- Trip attributes
        -- defaulting passenger_count to 1 when null is a judgment call
        -- might want to revisit this if it skews avg calculations
        coalesce(passenger_count, 1)::integer as passenger_count,
        coalesce(trip_distance, 0)::double as trip_distance,
        store_and_fwd_flag,

        -- Fare components
        -- coalescing to 0 for all fare fields - nulls would mess up sum() aggregations
        coalesce(fare_amount, 0)::double as fare_amount,
        coalesce(extra, 0)::double as extra,
        coalesce(mta_tax, 0)::double as mta_tax,
        coalesce(tip_amount, 0)::double as tip_amount,
        coalesce(tolls_amount, 0)::double as tolls_amount,
        coalesce(improvement_surcharge, 0)::double as improvement_surcharge,
        coalesce(total_amount, 0)::double as total_amount,
        coalesce(congestion_surcharge, 0)::double as congestion_surcharge,
        -- note: Airport_fee has weird capitalization in the source data
        coalesce(Airport_fee, 0)::double as airport_fee,

        -- Metadata
        'yellow' as taxi_type,
        current_timestamp as _loaded_at

    from source
)

select * from renamed
-- basic data quality filters - these catch most of the garbage records
where pickup_datetime is not null
  and dropoff_datetime is not null
  -- trips that end before they start are clearly data errors
  and pickup_datetime < dropoff_datetime
  -- date range filter using dbt vars so I can easily adjust for different analyses
  and pickup_datetime >= '{{ var("start_date") }}'
  and pickup_datetime < '{{ var("end_date") }}'::date + interval '1 day'
  -- negative values make no sense, filter them out
  and trip_distance >= 0
  and fare_amount >= 0
-- TODO: should probably add a filter for trips with unreasonably high fares
-- saw some records with $9999 which are clearly test data or errors
