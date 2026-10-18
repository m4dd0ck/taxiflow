"""Download NYC TLC taxi trip data from official sources."""

import argparse
import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Protocol

import httpx


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


class Logger(Protocol):
    """Anything with info/warning/error taking a message.

    Both `logging.Logger` and Dagster's `context.log` satisfy it, so the script and
    the Dagster assets share one download helper.
    """

    def info(self, msg: str) -> object: ...

    def warning(self, msg: str) -> object: ...

    def error(self, msg: str) -> object: ...


def iter_months(start: str, end: str) -> Iterator[tuple[int, int]]:
    """Yield (year, month) pairs from start to end inclusive.

    Args:
        start: First month as "YYYY-MM".
        end: Last month as "YYYY-MM".

    Returns:
        Iterator of (year, month) tuples.

    Raises:
        ValueError: If either string is not "YYYY-MM".
    """
    first = datetime.strptime(start, "%Y-%m")
    last = datetime.strptime(end, "%Y-%m")
    year, month = first.year, first.month
    while (year, month) <= (last.year, last.month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def download_file(
    url: str, dest: Path, log: Logger = logging.getLogger(__name__)
) -> bool:
    """Download a file from URL to destination path.

    Args:
        url: Source URL.
        dest: Destination file; skipped if it already exists.
        log: Where progress messages go.

    Returns:
        True if the file exists afterwards, False on 404 or HTTP error.
    """
    if dest.exists():
        log.info(f"Skipping {dest.name} (already exists)")
        return True

    log.info(f"Downloading {dest.name}...")
    part = dest.with_suffix(dest.suffix + ".part")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as response:
            if response.status_code == 404:
                log.warning(f"File not found: {url}")
                return False
            response.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            # Reason: stream to a .part file so an interrupted download never leaves a
            # truncated parquet that the exists() check above would treat as complete
            with open(part, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)
            part.rename(dest)
        log.info(f"Downloaded {dest.name}")
        return True
    except httpx.HTTPError as e:
        log.error(f"Error downloading {url}: {e}")
        part.unlink(missing_ok=True)
        return False


def download_taxi_data(
    start: str,
    end: str,
    taxi_types: list[str] | None = None,
) -> None:
    """Download taxi trip data for an inclusive "YYYY-MM" range."""
    if taxi_types is None:
        taxi_types = ["yellow", "green"]

    log = logging.getLogger(__name__)
    for taxi_type in taxi_types:
        log.info(f"\nDownloading {taxi_type} taxi data...")
        taxi_dir = DATA_DIR / taxi_type
        taxi_dir.mkdir(parents=True, exist_ok=True)

        for year, month in iter_months(start, end):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            download_file(f"{BASE_URL}/{filename}", taxi_dir / filename, log)


def download_zone_lookup() -> None:
    """Download taxi zone lookup CSV."""
    log = logging.getLogger(__name__)
    dest = DATA_DIR.parent.parent / "seeds" / "taxi_zone_lookup.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info("\nDownloading taxi zone lookup...")
    download_file(ZONE_URL, dest, log)


def main() -> None:
    """Main entry point for data download."""
    parser = argparse.ArgumentParser(description="Download NYC TLC taxi data")
    parser.add_argument(
        "--start",
        type=str,
        default="2024-10",
        help="Start month (YYYY-MM format)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="2024-11",
        help="End month (YYYY-MM format)",
    )
    parser.add_argument(
        "--taxi-types",
        type=str,
        nargs="+",
        default=["yellow", "green"],
        help="Taxi types to download",
    )
    parser.add_argument(
        "--no-zones",
        action="store_true",
        help="Skip the taxi zone lookup download",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger(__name__)

    log.info(f"Downloading data from {args.start} to {args.end}")
    download_taxi_data(args.start, args.end, args.taxi_types)

    if not args.no_zones:
        download_zone_lookup()

    log.info("\nDownload complete!")


if __name__ == "__main__":
    main()
