-- mart_daily_summary.sql
-- daily aggregates for dashboards and trend analysis
-- this is the main table for executive reporting

{{
    config(
        materialized='table'
    )
}}

with trips as (
    select * from {{ ref('fct_trips') }}
),

locations as (
    select * from {{ ref('dim_location') }}
),

-- aggregating by date, borough, and taxi type
-- this gives us a good balance of detail vs. row count
-- originally I had this at just date level but stakeholders wanted to see borough breakdowns
daily_summary as (
    select
        t.pickup_date_key,
        l.borough,
        t.taxi_type,

        -- Trip counts
        count(*) as total_trips,
        sum(t.passenger_count) as total_passengers,

        -- Distance metrics
        round(sum(t.trip_distance), 2) as total_distance_miles,
        round(avg(t.trip_distance), 2) as avg_distance_miles,

        -- Duration metrics
        round(sum(t.trip_duration_minutes), 2) as total_duration_minutes,
        round(avg(t.trip_duration_minutes), 2) as avg_duration_minutes,

        -- Revenue metrics
        -- rounding to 2 decimal places for currency
        round(sum(t.fare_amount), 2) as total_fare_amount,
        round(sum(t.tip_amount), 2) as total_tip_amount,
        round(sum(t.total_amount), 2) as total_revenue,
        round(avg(t.fare_amount), 2) as avg_fare_amount,
        round(avg(t.tip_amount), 2) as avg_tip_amount,

        -- Derived metrics
        -- these averages exclude nulls which is what we want
        round(avg(t.tip_percentage), 2) as avg_tip_percentage,
        round(avg(t.avg_speed_mph), 2) as avg_speed_mph,
        round(avg(t.fare_per_mile), 2) as avg_fare_per_mile

    from trips t
    left join locations l
        on t.pickup_location_id = l.location_id
    -- using pickup location for the borough, not dropoff
    -- could argue either way but pickup seemed more relevant for demand analysis
    group by 1, 2, 3
)

select * from daily_summary
order by pickup_date_key, borough, taxi_type
-- TODO: might want to add week-over-week and month-over-month calculations
-- would make the dashboards more useful out of the box
