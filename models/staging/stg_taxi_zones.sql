-- stg_taxi_zones.sql
-- reference data for nyc taxi zones from the tlc
-- this comes from a seed file, not the parquet data

{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ ref('taxi_zone_lookup') }}
),

-- the csv has quoted column names which is why we need the double quotes here
-- kind of annoying but it's how duckdb handles case sensitivity
renamed as (
    select
        "LocationID" as location_id,
        "Borough" as borough,
        "Zone" as zone_name,
        -- note: service_zone is already lowercase in the source, inconsistent with other columns
        "service_zone" as service_zone
    from source
)

select * from renamed
-- TODO: might want to add some data quality checks here
-- I've seen some zone names with trailing whitespace in the past
