-- mart_zone_performance.sql
-- zone-to-zone route analysis
-- this is the biggest table in the marts layer due to the O(n^2) zone combinations

{{
    config(
        materialized='table'
    )
}}

with trips as (
    select * from {{ ref('fct_trips') }}
),

-- joining dim_location twice - once for pickup, once for dropoff
-- aliasing them to keep things clear
pickup_locations as (
    select * from {{ ref('dim_location') }}
),

dropoff_locations as (
    select * from {{ ref('dim_location') }}
),

zone_performance as (
    select
        pl.location_id as pickup_location_id,
        pl.zone_name as pickup_zone_name,
        pl.borough as pickup_borough,
        dl.location_id as dropoff_location_id,
        dl.zone_name as dropoff_zone_name,
        dl.borough as dropoff_borough,

        -- Trip counts
        count(*) as total_trips,
        sum(t.passenger_count) as total_passengers,

        -- Distance metrics
        round(sum(t.trip_distance), 2) as total_distance_miles,
        round(avg(t.trip_distance), 2) as avg_distance_miles,

        -- Duration metrics
        round(avg(t.trip_duration_minutes), 2) as avg_duration_minutes,

        -- Revenue metrics
        round(sum(t.total_amount), 2) as total_revenue,
        round(avg(t.fare_amount), 2) as avg_fare_amount,
        round(avg(t.tip_percentage), 2) as avg_tip_percentage,

        -- Efficiency
        round(avg(t.avg_speed_mph), 2) as avg_speed_mph,
        round(avg(t.fare_per_mile), 2) as avg_fare_per_mile

    from trips t
    left join pickup_locations pl
        on t.pickup_location_id = pl.location_id
    left join dropoff_locations dl
        on t.dropoff_location_id = dl.location_id
    group by 1, 2, 3, 4, 5, 6
    -- filtering out routes with fewer than 10 trips
    -- this reduces noise from rare one-off trips and keeps the table manageable
    -- 10 is arbitrary but seems reasonable - could make this a dbt var
    having count(*) >= 10
)

select * from zone_performance
order by total_trips desc
-- most popular routes first makes the data more immediately useful
-- jfk to manhattan is usually near the top
