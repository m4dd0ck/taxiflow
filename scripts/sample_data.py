"""Write a small sample of one month of NYC TLC trip data to data/raw.

Used by CI so `dbt build` can run end to end without downloading full months
(a single month of yellow trips is ~50 MB). DuckDB reads only the row groups
it needs from the public bucket.
"""

import argparse
from pathlib import Path

import duckdb

from scripts.download_data import BASE_URL, DATA_DIR


def write_sample(kind: str, month: str, rows: int, data_dir: Path = DATA_DIR) -> int:
    """Copy the first ``rows`` trips of ``kind`` for ``month`` (YYYY-MM) into data_dir.

    Returns the number of rows written.
    """
    filename = f"{kind}_tripdata_{month}.parquet"
    dest = data_dir / kind / filename
    dest.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Reason: COPY ... TO does not accept a bound parameter for the target path,
    # so the path is quoted by hand; the URL and row limit are bound normally.
    target = "'" + str(dest).replace("'", "''") + "'"
    con.execute(
        f"COPY (SELECT * FROM read_parquet($url::VARCHAR) LIMIT $rows::BIGINT) TO {target} (FORMAT PARQUET)",
        {"url": f"{BASE_URL}/{filename}", "rows": rows},
    )
    count = con.execute(
        "SELECT count(*) FROM read_parquet($p::VARCHAR)", {"p": str(dest)}
    ).fetchone()[0]
    print(f"  {dest.relative_to(data_dir.parent.parent)}: {count:,} rows")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Sample NYC TLC trip data for CI")
    parser.add_argument("--month", default="2024-10", help="Month to sample (YYYY-MM)")
    parser.add_argument("--yellow-rows", type=int, default=50_000)
    parser.add_argument("--green-rows", type=int, default=20_000)
    args = parser.parse_args()

    write_sample("yellow", args.month, args.yellow_rows)
    write_sample("green", args.month, args.green_rows)


if __name__ == "__main__":
    main()
