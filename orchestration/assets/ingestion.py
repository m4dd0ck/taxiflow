"""Data ingestion assets for NYC taxi data."""

from pathlib import Path

from dagster import asset, AssetExecutionContext, Config, MaterializeResult, MetadataValue

# Reason: both entry points (Makefile dagster targets, CI's `python -m scripts...`) run from
# the repo root and scripts/ is a package, so the script's helper is importable here.
from scripts.download_data import BASE_URL, ZONE_URL, download_file, iter_months


# project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
SEEDS_DIR = PROJECT_ROOT / "seeds"


class TaxiDataConfig(Config):
    """Configuration for taxi data download."""

    start_month: str = "2024-10"
    end_month: str = "2024-11"
    taxi_types: list[str] = ["yellow", "green"]


@asset(
    group_name="ingestion",
    description="Raw NYC taxi trip data in parquet format from TLC data portal",
    compute_kind="python",
)
def raw_taxi_data(context: AssetExecutionContext, config: TaxiDataConfig) -> MaterializeResult:
    """Download raw taxi trip parquet files."""
    downloaded_files = []
    failed_files = []

    for taxi_type in config.taxi_types:
        taxi_dir = DATA_DIR / taxi_type
        taxi_dir.mkdir(parents=True, exist_ok=True)

        for year, month in iter_months(config.start_month, config.end_month):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            dest = taxi_dir / filename

            if download_file(f"{BASE_URL}/{filename}", dest, context.log):
                downloaded_files.append(str(dest))
            else:
                failed_files.append(filename)

    # calculate total size of downloaded data
    total_size = sum(Path(f).stat().st_size for f in downloaded_files if Path(f).exists())

    return MaterializeResult(
        metadata={
            "files_downloaded": MetadataValue.int(len(downloaded_files)),
            "files_failed": MetadataValue.int(len(failed_files)),
            "total_size_mb": MetadataValue.float(round(total_size / (1024 * 1024), 2)),
            "date_range": MetadataValue.text(f"{config.start_month} to {config.end_month}"),
            "taxi_types": MetadataValue.text(", ".join(config.taxi_types)),
        }
    )


@asset(
    group_name="ingestion",
    description="Taxi zone lookup reference data",
    compute_kind="python",
)
def taxi_zone_lookup(context: AssetExecutionContext) -> MaterializeResult:
    """Download taxi zone lookup CSV."""
    dest = SEEDS_DIR / "taxi_zone_lookup.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)

    success = download_file(ZONE_URL, dest, context.log)

    if success and dest.exists():
        # count rows in the CSV
        with open(dest) as f:
            row_count = sum(1 for _ in f) - 1  # subtract header

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "file_path": MetadataValue.path(str(dest)),
            }
        )

    return MaterializeResult(
        metadata={"status": MetadataValue.text("failed")}
    )
