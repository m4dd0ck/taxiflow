-- dim_time.sql
-- time dimension at minute grain (1440 rows total)
-- originally I had this at hour grain but minute gives more flexibility

{{
    config(
        materialized='table'
    )
}}

-- generating the time spine using duckdb's generate_series
-- the unnest is needed because generate_series returns a list in duckdb
-- this tripped me up at first coming from postgres
with time_spine as (
    select
        hour_of_day,
        minute_of_hour
    from (
        select unnest(generate_series(0, 23)) as hour_of_day
    ) hours
    cross join (
        select unnest(generate_series(0, 59)) as minute_of_hour
    ) minutes
),

final as (
    select
        -- HHMM format as integer - 0 to 2359
        -- easy to read and sorts correctly
        (hour_of_day * 100 + minute_of_hour)::integer as time_key,

        -- Time attributes
        hour_of_day as hour,
        minute_of_hour as minute,
        lpad(hour_of_day::varchar, 2, '0') || ':' || lpad(minute_of_hour::varchar, 2, '0') as time_string,

        -- time of day buckets for analysis
        -- these are somewhat arbitrary but match common business definitions
        case
            when hour_of_day between 5 and 11 then 'Morning'
            when hour_of_day between 12 and 16 then 'Afternoon'
            when hour_of_day between 17 and 20 then 'Evening'
            else 'Night'
        end as time_of_day,

        -- rush hour flag based on typical nyc commute times
        -- note: this doesn't account for weekends but that's handled in the join with dim_date
        case
            when hour_of_day between 7 and 9 then true
            when hour_of_day between 16 and 19 then true
            else false
        end as is_rush_hour,

        -- taxi driver shifts - these match the typical yellow cab lease schedules
        -- day shift is roughly 5am-5pm, night shift is 5pm-5am
        -- I'm simplifying to hour boundaries here
        case
            when hour_of_day between 6 and 13 then 'Day'
            when hour_of_day between 14 and 21 then 'Evening'
            else 'Night'
        end as shift

    from time_spine
)

select * from final
order by time_key
