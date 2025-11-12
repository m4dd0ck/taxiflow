-- mart_hourly_patterns.sql
-- aggregates trips by day-of-week and hour for demand forecasting
-- useful for understanding when and where taxis are needed

{{
    config(
        materialized='table'
    )
}}

with trips as (
    select * from {{ ref('fct_trips') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

locations as (
    select * from {{ ref('dim_location') }}
),

-- grouping by day_of_week (not date) so we can see patterns across all weeks
-- this smooths out one-off events and gives a clearer picture of typical demand
hourly_patterns as (
    select
        d.day_of_week,
        d.day_name,
        -- extracting hour from the HHMM format time key
        -- integer division drops the minutes which is what we want
        t.pickup_time_key / 100 as hour_of_day,
        l.borough,

        -- Trip metrics
        count(*) as total_trips,
        sum(t.passenger_count) as total_passengers,

        -- Revenue metrics
        round(sum(t.total_amount), 2) as total_revenue,
        round(avg(t.fare_amount), 2) as avg_fare_amount,

        -- Duration and distance
        round(avg(t.trip_distance), 2) as avg_distance_miles,
        round(avg(t.trip_duration_minutes), 2) as avg_duration_minutes,

        -- Efficiency metrics
        -- speed varies a lot by time of day due to traffic
        round(avg(t.avg_speed_mph), 2) as avg_speed_mph

    from trips t
    inner join dates d
        on t.pickup_date_key = d.date_key
    left join locations l
        on t.pickup_location_id = l.location_id
    group by 1, 2, 3, 4
)

select * from hourly_patterns
order by day_of_week, hour_of_day, borough
-- this ordering makes it easy to visualize the weekly cycle in bi tools
