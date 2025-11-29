"""Data ingestion assets for NYC taxi data."""

from pathlib import Path
from datetime import datetime

import httpx
from dagster import asset, AssetExecutionContext, Config, MaterializeResult, MetadataValue


# NYC TLC data portal URLs
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
SEEDS_DIR = PROJECT_ROOT / "seeds"


class TaxiDataConfig(Config):
    """Configuration for taxi data download."""

    start_month: str = "2024-10"
    end_month: str = "2024-11"
    taxi_types: list[str] = ["yellow", "green"]


def _download_file(url: str, dest: Path, context: AssetExecutionContext) -> bool:
    """Download a file from URL to destination."""
    if dest.exists():
        context.log.info(f"Skipping {dest.name} (already exists)")
        return True

    context.log.info(f"Downloading {dest.name}...")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as response:
            if response.status_code == 404:
                context.log.warning(f"File not found: {url}")
                return False
            response.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        context.log.info(f"Downloaded {dest.name}")
        return True
    except httpx.HTTPError as e:
        context.log.error(f"Error downloading {url}: {e}")
        return False


@asset(
    group_name="ingestion",
    description="Raw NYC taxi trip data in parquet format from TLC data portal",
    compute_kind="python",
)
def raw_taxi_data(context: AssetExecutionContext, config: TaxiDataConfig) -> MaterializeResult:
    """Download raw taxi trip parquet files."""
    start = datetime.strptime(config.start_month, "%Y-%m")
    end = datetime.strptime(config.end_month, "%Y-%m")

    downloaded_files = []
    failed_files = []

    for taxi_type in config.taxi_types:
        taxi_dir = DATA_DIR / taxi_type
        taxi_dir.mkdir(parents=True, exist_ok=True)

        year, month = start.year, start.month
        while (year, month) <= (end.year, end.month):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{TLC_BASE_URL}/{filename}"
            dest = taxi_dir / filename

            if _download_file(url, dest, context):
                downloaded_files.append(str(dest))
            else:
                failed_files.append(filename)

            month += 1
            if month > 12:
                month = 1
                year += 1

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

    success = _download_file(TLC_ZONE_URL, dest, context)

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
