"""Job definitions for taxiflow pipeline."""

from dagster import define_asset_job, AssetSelection


taxiflow_full_pipeline = define_asset_job(
    name="taxiflow_full_pipeline",
    description="Full ELT pipeline: download raw data, run dbt models, generate quality report",
    selection=AssetSelection.all(),
)

dbt_only = define_asset_job(
    name="dbt_only",
    description="Run only dbt models (skip data download)",
    selection=AssetSelection.groups("staging", "intermediate", "dimensions", "facts", "marts"),
)

ingestion_only = define_asset_job(
    name="ingestion_only",
    description="Download raw data only",
    selection=AssetSelection.groups("ingestion"),
)

marts_refresh = define_asset_job(
    name="marts_refresh",
    description="Refresh only the mart tables (assumes upstream models are current)",
    selection=AssetSelection.groups("marts", "quality"),
)
