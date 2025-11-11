-- dim_date.sql
-- standard date dimension for the star schema
-- pre-generates dates so we don't have to do date math at query time

{{
    config(
        materialized='table'
    )
}}

-- dbt_utils.date_spine is a lifesaver here
-- generates a row for each day in the range
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
),

dates as (
    select
        cast(date_day as date) as full_date
    from date_spine
),

final as (
    select
        -- using YYYYMMDD as the surrogate key because it's human-readable
        -- and sorts correctly without needing to join back to the dimension
        cast(strftime(full_date, '%Y%m%d') as integer) as date_key,

        -- Date attributes
        full_date,
        extract(year from full_date)::integer as year,
        extract(quarter from full_date)::integer as quarter,
        extract(month from full_date)::integer as month_num,
        strftime(full_date, '%B') as month_name,
        extract(week from full_date)::integer as week_of_year,
        extract(day from full_date)::integer as day_of_month,
        -- duckdb uses 0=Sunday, 6=Saturday which matches javascript but not python
        extract(dayofweek from full_date)::integer as day_of_week,
        strftime(full_date, '%A') as day_name,

        -- Flags
        -- weekend definition is straightforward
        case when extract(dayofweek from full_date) in (0, 6) then true else false end as is_weekend,

        -- US Federal Holidays - simplified version
        -- only doing fixed-date holidays here, not floating ones like thanksgiving
        -- a proper implementation would use a seed table with actual holiday dates
        -- but this is good enough for a portfolio project
        case
            when extract(month from full_date) = 1 and extract(day from full_date) = 1 then true  -- New Year's Day
            when extract(month from full_date) = 7 and extract(day from full_date) = 4 then true  -- Independence Day
            when extract(month from full_date) = 11 and extract(day from full_date) = 11 then true  -- Veterans Day
            when extract(month from full_date) = 12 and extract(day from full_date) = 25 then true -- Christmas
            else false
        end as is_holiday
        -- TODO: add thanksgiving, mlk day, memorial day, labor day
        -- these require calculating "nth weekday of month" which is annoying

    from dates
)

select * from final
