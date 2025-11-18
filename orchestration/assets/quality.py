"""Data quality assets.

Runs quality checks after dbt models are built and generates reports.
"""

from pathlib import Path

import duckdb
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue, AssetIn


PROJECT_ROOT = Path(__file__).parent.parent.parent
DUCKDB_PATH = PROJECT_ROOT / "data" / "taxiflow.duckdb"


@asset(
    group_name="quality",
    description="Data quality report on the final mart tables",
    compute_kind="duckdb",
    deps=["mart_daily_summary", "mart_hourly_patterns", "mart_zone_performance"],
)
def data_quality_report(context: AssetExecutionContext) -> MaterializeResult:
    """Generate data quality metrics for mart tables.

    Checks:
    - Row counts for each mart
    - Null percentages for key columns
    - Date range coverage
    - Basic sanity checks (no negative values, etc.)
    """
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    metrics = {}

    # check mart_daily_summary
    try:
        result = conn.execute("""
            SELECT
                COUNT(*) as row_count,
                MIN(trip_date) as min_date,
                MAX(trip_date) as max_date,
                COUNT(DISTINCT trip_date) as days_covered,
                SUM(CASE WHEN total_trips IS NULL THEN 1 ELSE 0 END) as null_trips,
                SUM(CASE WHEN total_trips < 0 THEN 1 ELSE 0 END) as negative_trips
            FROM marts.mart_daily_summary
        """).fetchone()

        metrics["daily_summary"] = {
            "row_count": result[0],
            "date_range": f"{result[1]} to {result[2]}",
            "days_covered": result[3],
            "null_trips": result[4],
            "negative_trips": result[5],
        }
        context.log.info(f"mart_daily_summary: {result[0]} rows, {result[3]} days")
    except Exception as e:
        context.log.warning(f"Could not check mart_daily_summary: {e}")
        metrics["daily_summary"] = {"error": str(e)}

    # check mart_hourly_patterns
    try:
        result = conn.execute("""
            SELECT
                COUNT(*) as row_count,
                COUNT(DISTINCT hour_of_day) as hours_covered,
                COUNT(DISTINCT day_of_week) as days_of_week
            FROM marts.mart_hourly_patterns
        """).fetchone()

        metrics["hourly_patterns"] = {
            "row_count": result[0],
            "hours_covered": result[1],
            "days_of_week": result[2],
        }
        context.log.info(f"mart_hourly_patterns: {result[0]} rows")
    except Exception as e:
        context.log.warning(f"Could not check mart_hourly_patterns: {e}")
        metrics["hourly_patterns"] = {"error": str(e)}

    # check mart_zone_performance
    try:
        result = conn.execute("""
            SELECT
                COUNT(*) as row_count,
                COUNT(DISTINCT pickup_zone) as zones_covered,
                AVG(avg_trip_distance) as avg_distance
            FROM marts.mart_zone_performance
        """).fetchone()

        metrics["zone_performance"] = {
            "row_count": result[0],
            "zones_covered": result[1],
            "avg_distance": round(result[2], 2) if result[2] else None,
        }
        context.log.info(f"mart_zone_performance: {result[0]} rows, {result[1]} zones")
    except Exception as e:
        context.log.warning(f"Could not check mart_zone_performance: {e}")
        metrics["zone_performance"] = {"error": str(e)}

    conn.close()

    # determine overall status
    has_errors = any("error" in m for m in metrics.values())
    has_nulls = any(
        m.get("null_trips", 0) > 0 or m.get("negative_trips", 0) > 0
        for m in metrics.values()
    )

    if has_errors:
        status = "ERROR"
    elif has_nulls:
        status = "WARNING"
    else:
        status = "PASSED"

    return MaterializeResult(
        metadata={
            "status": MetadataValue.text(status),
            "daily_summary_rows": MetadataValue.int(
                metrics.get("daily_summary", {}).get("row_count", 0)
            ),
            "hourly_patterns_rows": MetadataValue.int(
                metrics.get("hourly_patterns", {}).get("row_count", 0)
            ),
            "zone_performance_rows": MetadataValue.int(
                metrics.get("zone_performance", {}).get("row_count", 0)
            ),
            "zones_covered": MetadataValue.int(
                metrics.get("zone_performance", {}).get("zones_covered", 0)
            ),
        }
    )
