"""Data quality assets."""

from pathlib import Path

import duckdb
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent
DUCKDB_PATH = PROJECT_ROOT / "data" / "taxiflow.duckdb"

# dbt-duckdb prefixes custom schemas with the target schema, so `+schema: marts`
# lands in `main_marts`
MARTS_SCHEMA = "main_marts"


@asset(
    group_name="quality",
    description="Data quality report on the final mart tables",
    compute_kind="duckdb",
    deps=[
        AssetKey(["marts", "mart_daily_summary"]),
        AssetKey(["marts", "mart_hourly_patterns"]),
        AssetKey(["marts", "mart_zone_performance"]),
    ],
)
def data_quality_report(context: AssetExecutionContext) -> MaterializeResult:
    """Generate data quality metrics for mart tables.

    Raises on any query failure rather than reporting an error status, so a
    schema drift in the marts fails the run instead of hiding in metadata.
    """
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        daily = conn.execute(f"""
            SELECT
                COUNT(*) AS row_count,
                MIN(pickup_date_key) AS min_date_key,
                MAX(pickup_date_key) AS max_date_key,
                COUNT(DISTINCT pickup_date_key) AS days_covered,
                SUM(CASE WHEN total_trips IS NULL THEN 1 ELSE 0 END) AS null_trips,
                SUM(CASE WHEN total_trips < 0 THEN 1 ELSE 0 END) AS negative_trips
            FROM {MARTS_SCHEMA}.mart_daily_summary
        """).fetchone()
        context.log.info(f"mart_daily_summary: {daily[0]} rows, {daily[3]} days")

        hourly = conn.execute(f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT hour_of_day) AS hours_covered,
                COUNT(DISTINCT day_of_week) AS days_of_week
            FROM {MARTS_SCHEMA}.mart_hourly_patterns
        """).fetchone()
        context.log.info(f"mart_hourly_patterns: {hourly[0]} rows")

        zones = conn.execute(f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT pickup_zone_name) AS zones_covered,
                AVG(avg_distance_miles) AS avg_distance
            FROM {MARTS_SCHEMA}.mart_zone_performance
        """).fetchone()
        context.log.info(f"mart_zone_performance: {zones[0]} rows, {zones[1]} zones")
    finally:
        conn.close()

    null_trips, negative_trips = daily[4] or 0, daily[5] or 0
    status = "WARNING" if null_trips > 0 or negative_trips > 0 else "PASSED"

    return MaterializeResult(
        metadata={
            "status": MetadataValue.text(status),
            "daily_summary_rows": MetadataValue.int(daily[0]),
            "date_key_range": MetadataValue.text(f"{daily[1]} to {daily[2]}"),
            "days_covered": MetadataValue.int(daily[3]),
            "null_trips": MetadataValue.int(null_trips),
            "negative_trips": MetadataValue.int(negative_trips),
            "hourly_patterns_rows": MetadataValue.int(hourly[0]),
            "hours_covered": MetadataValue.int(hourly[1]),
            "zone_performance_rows": MetadataValue.int(zones[0]),
            "zones_covered": MetadataValue.int(zones[1]),
            "avg_route_distance_miles": MetadataValue.float(
                round(zones[2], 2) if zones[2] is not None else 0.0
            ),
        }
    )
