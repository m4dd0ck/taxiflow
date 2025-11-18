"""Job definitions for taxiflow pipeline.

Jobs define which assets to materialize together and in what order.
"""

from dagster import define_asset_job, AssetSelection


# full pipeline: download data -> run dbt -> quality checks
taxiflow_full_pipeline = define_asset_job(
    name="taxiflow_full_pipeline",
    description="Full ELT pipeline: download raw data, run dbt models, generate quality report",
    selection=AssetSelection.all(),
)

# just the dbt models (assumes data already downloaded)
dbt_only = define_asset_job(
    name="dbt_only",
    description="Run only dbt models (skip data download)",
    selection=AssetSelection.groups("staging", "intermediate", "dimensions", "facts", "marts"),
)

# just the ingestion
ingestion_only = define_asset_job(
    name="ingestion_only",
    description="Download raw data only",
    selection=AssetSelection.groups("ingestion"),
)

# marts refresh - useful for daily updates
marts_refresh = define_asset_job(
    name="marts_refresh",
    description="Refresh only the mart tables (assumes upstream models are current)",
    selection=AssetSelection.groups("marts", "quality"),
)
