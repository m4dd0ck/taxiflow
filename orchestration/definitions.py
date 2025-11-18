"""Dagster definitions for the taxiflow pipeline.

This is the main entry point for Dagster. Run with:
    dagster dev -m orchestration.definitions

Or for production:
    dagster-webserver -m orchestration.definitions
"""

from dagster import Definitions, load_assets_from_modules

from . import assets
from .assets.dbt_assets import dbt_taxiflow_assets
from .resources import dbt_resource
from .jobs import (
    taxiflow_full_pipeline,
    dbt_only,
    ingestion_only,
    marts_refresh,
)
from .schedules import monthly_full_refresh, daily_marts_refresh


# load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# combine with dbt assets
# note: dbt_taxiflow_assets is already included via assets module import

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": dbt_resource,
    },
    jobs=[
        taxiflow_full_pipeline,
        dbt_only,
        ingestion_only,
        marts_refresh,
    ],
    schedules=[
        monthly_full_refresh,
        daily_marts_refresh,
    ],
)
