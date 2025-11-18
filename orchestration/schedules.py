"""Schedule definitions for taxiflow pipeline.

Schedules define when jobs should run automatically.
"""

from dagster import ScheduleDefinition

from .jobs import taxiflow_full_pipeline, marts_refresh


# run full pipeline monthly on the 5th (to allow TLC data to be published)
# NYC TLC typically publishes data a few days into the month
monthly_full_refresh = ScheduleDefinition(
    job=taxiflow_full_pipeline,
    cron_schedule="0 6 5 * *",  # 6 AM on the 5th of each month
    description="Monthly full pipeline refresh - runs on the 5th to allow for TLC data publication",
)

# run marts refresh daily for any incremental updates
daily_marts_refresh = ScheduleDefinition(
    job=marts_refresh,
    cron_schedule="0 7 * * *",  # 7 AM daily
    description="Daily marts refresh for incremental updates",
)
