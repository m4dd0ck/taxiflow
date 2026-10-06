# TaxiFlow

dbt + DuckDB pipeline for NYC taxi data. Downloads TLC trip records, builds a star schema, outputs analytics marts.

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager

### Setup

```bash
# Clone the repository
cd taxiflow

# Install dependencies
uv sync

# Install dbt packages
uv run dbt deps --profiles-dir .

# Download taxi data (2 months by default)
uv run python scripts/download_data.py --start 2024-10 --end 2024-11

# Run the full pipeline
uv run dbt build --profiles-dir .

# Generate documentation
uv run dbt docs generate --profiles-dir .

# Serve documentation locally
uv run dbt docs serve --profiles-dir .
```

## Data Model

### Layers

| Layer | Purpose | Materialization |
|-------|---------|-----------------|
| staging | Clean and standardize raw data | View |
| intermediate | Transform and union datasets | View |
| dimensions | Conformed dimension tables | Table |
| facts | Transaction-level records | Incremental |
| marts | Aggregated reports | Table |

### Models

**Dimensions:**
- `dim_date` - Calendar date attributes (2020-2026)
- `dim_time` - Time of day with rush hour and shift flags
- `dim_location` - NYC taxi zones with borough info
- `dim_rate_code` - Trip rate types (standard, JFK, Newark, etc.)
- `dim_payment_type` - Payment methods

**Facts:**
- `fct_trips` - One row per taxi trip with all dimension keys and metrics

**Marts:**
- `mart_daily_summary` - Daily aggregates by borough and taxi type
- `mart_hourly_patterns` - Demand patterns by hour and day of week
- `mart_zone_performance` - Zone-to-zone trip statistics

## Data Sources

This project uses official NYC TLC trip record data:
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Data includes:
- Yellow and green taxi trips (about 3.6M/month combined for Oct-Nov 2024; yellow is the large majority)
- Taxi zone lookup reference

## Orchestration with Dagster

Dagster handles scheduling and orchestration.

### Running with Dagster

```bash
# generate dbt manifest (required for dagster-dbt)
make manifest

# start dagster dev server
make dagster
# then open http://localhost:3000

# or run the full pipeline directly
make pipeline
```

### Pipeline Jobs

| Job | Description |
|-----|-------------|
| `taxiflow_full_pipeline` | Full ELT: download data → dbt build → quality checks |
| `dbt_only` | Run only dbt models (assumes data exists) |
| `ingestion_only` | Download raw data only |
| `marts_refresh` | Refresh marts and quality report |

### Schedules

- **Monthly full refresh**: Runs on the 5th of each month (after TLC publishes data)
- **Daily marts refresh**: Updates mart tables daily at 7 AM

## Project Structure

```
taxiflow/
├── models/
│   ├── staging/         # Clean raw data
│   ├── intermediate/    # Transform and union
│   ├── dimensions/      # Dimension tables
│   ├── facts/           # Fact tables
│   └── marts/           # Aggregations
├── orchestration/       # Dagster pipeline
│   ├── assets/          # Data assets
│   ├── resources/       # dbt resource config
│   ├── jobs.py          # Job definitions
│   ├── schedules.py     # Schedule definitions
│   └── definitions.py   # Dagster entry point
├── seeds/               # Reference data (CSV)
├── tests/               # Custom data tests
├── scripts/             # Python utilities
├── dbt_project.yml      # dbt configuration
├── profiles.yml         # Connection settings
└── packages.yml         # dbt package dependencies
```

`data/` (raw Parquet and the DuckDB file) is created by the download script and ignored by git.

## Configuration

Edit `dbt_project.yml` to change the date range:

```yaml
vars:
  start_date: '2024-10-01'
  end_date: '2024-11-30'
```

## Running Tests

```bash
# Run all tests
uv run dbt test --profiles-dir .

# Run tests for specific model
uv run dbt test --select fct_trips --profiles-dir .
```

## Key Metrics

The pipeline produces these analytics:

- **Trip Volume**: Total trips, trips by borough, trips by hour
- **Revenue**: Total fares, average fare, revenue by zone
- **Efficiency**: Average speed, trip duration, trips per hour
- **Customer Behavior**: Tip percentages, payment preferences

## Sample Queries & Output

With 2 months of data (Oct-Nov 2024), the pipeline processes **7.3 million trips**.

### Revenue by Borough

```sql
SELECT
    borough,
    SUM(total_trips) as trips,
    ROUND(SUM(total_revenue), 2) as revenue,
    ROUND(AVG(avg_fare_amount), 2) as avg_fare
FROM main_marts.mart_daily_summary
WHERE borough IS NOT NULL
GROUP BY borough
ORDER BY revenue DESC;
```

```
Borough              Trips         Revenue   Avg Fare
-------------------------------------------------------
Manhattan        6,505,045  $158,226,534.52    $16.14
Queens             669,244   $48,155,119.11    $37.19
Brooklyn           137,800    $4,253,816.11    $25.71
Bronx               25,353      $909,933.25    $29.82
Staten Island          371       $15,084.87    $28.26
```

Manhattan dominates with 88% of trips, but Queens generates disproportionate revenue due to airport traffic (avg fare $37 vs $16).

### Rush Hour vs Off-Peak

```sql
SELECT
    CASE
        WHEN hour_of_day BETWEEN 7 AND 9 THEN 'Morning Rush (7-9 AM)'
        WHEN hour_of_day BETWEEN 17 AND 19 THEN 'Evening Rush (5-7 PM)'
        ELSE 'Off-Peak'
    END as period,
    SUM(total_trips) as trips,
    ROUND(AVG(avg_speed_mph), 1) as avg_speed,
    ROUND(AVG(avg_fare_amount), 2) as avg_fare
FROM main_marts.mart_hourly_patterns
GROUP BY 1
ORDER BY trips DESC;
```

```
Period                        Trips   Avg Speed   Avg Fare
------------------------------------------------------------
Off-Peak                  5,798,933    34.2 mph    $31.06
Evening Rush (5-7 PM)     1,055,244    19.5 mph    $32.09
Morning Rush (7-9 AM)       503,801    28.4 mph    $31.77
```

Evening rush shows 43% slower speeds than off-peak due to congestion.

### Top 5 Most Popular Routes

```sql
SELECT
    pickup_zone_name,
    dropoff_zone_name,
    total_trips,
    ROUND(total_revenue, 2) as revenue
FROM main_marts.mart_zone_performance
ORDER BY total_trips DESC
LIMIT 5;
```

```
Pickup Zone                Dropoff Zone                 Trips      Revenue
---------------------------------------------------------------------------
Upper East Side South      Upper East Side North       54,984    $875,278
Upper East Side North      Upper East Side South       47,223    $769,822
Upper East Side South      Upper East Side South       36,698    $515,814
Upper East Side North      Upper East Side North       33,731    $444,928
Midtown Center             Upper East Side South       25,257    $448,052
```

The Upper East Side corridor is the busiest route pair in NYC.

### Airport Trip Analysis

```sql
SELECT
    pickup_zone_name,
    dropoff_zone_name,
    total_trips,
    ROUND(avg_fare_amount, 2) as avg_fare,
    ROUND(avg_tip_percentage, 1) as tip_pct
FROM main_marts.mart_zone_performance
WHERE pickup_zone_name LIKE '%Airport%'
   OR dropoff_zone_name LIKE '%Airport%'
ORDER BY total_trips DESC
LIMIT 5;
```

```
Pickup                     Dropoff                     Trips  Avg Fare  Tip %
--------------------------------------------------------------------------------
JFK Airport                Times Sq/Theatre District  17,460   $70.81   15.0%
LaGuardia Airport          Times Sq/Theatre District  14,780   $51.70   21.8%
JFK Airport                Outside of NYC             14,271  $109.81   61.0%
Times Sq/Theatre District  LaGuardia Airport          10,254   $51.75   20.5%
LaGuardia Airport          Midtown Center              9,351   $48.33   23.2%
```

JFK trips average $70+ (flat rate to Manhattan). LaGuardia passengers tip better (22% vs 15%).

### Connect and Query

```bash
# Using Python
uv run python -c "
import duckdb
con = duckdb.connect('data/taxiflow.duckdb', read_only=True)
result = con.execute('SELECT * FROM main_marts.mart_daily_summary LIMIT 5').df()
print(result)
"

# Using DuckDB CLI (if installed)
duckdb data/taxiflow.duckdb -c "SELECT * FROM main_marts.mart_daily_summary LIMIT 5"
```

## License

MIT
