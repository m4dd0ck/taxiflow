"""Schedule definitions for taxiflow pipeline."""

from dagster import ScheduleDefinition

from .jobs import taxiflow_full_pipeline, marts_refresh


# TLC publishes new data a few days into each month
monthly_full_refresh = ScheduleDefinition(
    job=taxiflow_full_pipeline,
    cron_schedule="0 6 5 * *",  # 6 AM on the 5th of each month
    description="Monthly full pipeline refresh - runs on the 5th to allow for TLC data publication",
)

daily_marts_refresh = ScheduleDefinition(
    job=marts_refresh,
    cron_schedule="0 7 * * *",  # 7 AM daily
    description="Daily marts refresh for incremental updates",
)
