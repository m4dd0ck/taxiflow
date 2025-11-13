"""Download NYC TLC taxi trip data from official sources."""

import argparse
from datetime import datetime
from pathlib import Path

import httpx


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


def download_file(url: str, dest: Path) -> bool:
    """Download a file from URL to destination path."""
    if dest.exists():
        print(f"  Skipping {dest.name} (already exists)")
        return True

    print(f"  Downloading {dest.name}...")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as response:
            if response.status_code == 404:
                print(f"  File not found: {url}")
                return False
            response.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        print(f"  Downloaded {dest.name}")
        return True
    except httpx.HTTPError as e:
        print(f"  Error downloading {url}: {e}")
        return False


def download_taxi_data(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    taxi_types: list[str] | None = None,
) -> None:
    """Download taxi trip data for specified date range."""
    if taxi_types is None:
        taxi_types = ["yellow", "green"]

    for taxi_type in taxi_types:
        print(f"\nDownloading {taxi_type} taxi data...")
        taxi_dir = DATA_DIR / taxi_type
        taxi_dir.mkdir(parents=True, exist_ok=True)

        year = start_year
        month = start_month
        while (year, month) <= (end_year, end_month):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}/{filename}"
            dest = taxi_dir / filename
            download_file(url, dest)

            month += 1
            if month > 12:
                month = 1
                year += 1


def download_zone_lookup() -> None:
    """Download taxi zone lookup CSV."""
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    dest = DATA_DIR.parent.parent / "seeds" / "taxi_zone_lookup.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    print("\nDownloading taxi zone lookup...")
    download_file(url, dest)


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
        "--include-zones",
        action="store_true",
        default=True,
        help="Also download zone lookup",
    )

    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m")
    end = datetime.strptime(args.end, "%Y-%m")

    print(f"Downloading data from {args.start} to {args.end}")
    download_taxi_data(
        start.year,
        start.month,
        end.year,
        end.month,
        args.taxi_types,
    )

    if args.include_zones:
        download_zone_lookup()

    print("\nDownload complete!")


if __name__ == "__main__":
    main()
