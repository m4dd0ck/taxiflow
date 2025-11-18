# TaxiFlow: NYC Taxi Analytics Pipeline

A production-grade ELT pipeline that transforms NYC taxi trip records into actionable analytics using dbt-core, DuckDB, and dimensional modeling best practices.

## Features

- **Star Schema Design**: Dimensional model with 5 dimensions and a central fact table
- **Incremental Processing**: Efficient fact table updates using dbt incremental materialization
- **Data Quality**: 74 automated tests including uniqueness, referential integrity, and value validation
- **Full Documentation**: Auto-generated dbt docs with model descriptions and data lineage

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
| marts | Business-ready aggregations | Table |

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
- Yellow taxi trips (~7M/month)
- Green taxi trips (~500K/month)
- Taxi zone lookup reference

## Orchestration with Dagster

The pipeline includes Dagster orchestration for production scheduling and monitoring.

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
│   └── marts/           # Business aggregations
├── orchestration/       # Dagster pipeline
│   ├── assets/          # Data assets
│   ├── resources/       # dbt resource config
│   ├── jobs.py          # Job definitions
│   ├── schedules.py     # Schedule definitions
│   └── definitions.py   # Dagster entry point
├── seeds/               # Reference data (CSV)
├── macros/              # Reusable SQL
├── tests/               # Custom data tests
├── scripts/             # Python utilities
├── data/                # Local DuckDB + Parquet
├── dbt_project.yml      # dbt configuration
├── profiles.yml         # Connection settings
└── packages.yml         # dbt package dependencies
```

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

## License

MIT
