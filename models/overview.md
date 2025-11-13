{% docs __overview__ %}

# TaxiFlow: NYC Taxi Analytics Pipeline

Welcome to the TaxiFlow data documentation. This project transforms NYC Taxi & Limousine Commission (TLC) trip data into a well-structured star schema optimized for analytics.

## Quick Links

- **Fact Tables**: [fct_trips](#!/model/model.taxiflow.fct_trips)
- **Dimensions**:
  - [dim_date](#!/model/model.taxiflow.dim_date)
  - [dim_time](#!/model/model.taxiflow.dim_time)
  - [dim_location](#!/model/model.taxiflow.dim_location)
  - [dim_rate_code](#!/model/model.taxiflow.dim_rate_code)
  - [dim_payment_type](#!/model/model.taxiflow.dim_payment_type)
- **Marts**:
  - [mart_daily_summary](#!/model/model.taxiflow.mart_daily_summary)
  - [mart_hourly_patterns](#!/model/model.taxiflow.mart_hourly_patterns)
  - [mart_zone_performance](#!/model/model.taxiflow.mart_zone_performance)

## Data Sources

This project uses official NYC TLC trip record data:
- **Yellow Taxi**: Traditional NYC yellow cabs
- **Green Taxi**: Boro taxis serving outer boroughs

See [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) for more information.

## Model Layers

| Layer | Purpose | Materialization |
|-------|---------|-----------------|
| Staging | Clean and standardize raw data | View |
| Intermediate | Transform and union datasets | View |
| Dimensions | Conformed dimension tables | Table |
| Facts | Transaction-level records | Incremental |
| Marts | Business-ready aggregations | Table |

## Key Metrics

- **Trip Volume**: Total trips, trips by borough, trips by hour
- **Revenue**: Total fares, average fare, revenue by zone
- **Efficiency**: Average speed, trips per hour
- **Customer Behavior**: Tip percentages, payment preferences

{% enddocs %}
