/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Asset that aggregates staging trips for analytics
name: reports.trips_report

# running on DuckDB locally
type: duckdb.sql

depends:
  - staging.trips

# incrementally refresh based on pickup_datetime
materialization:
  type: table
  strategy: create+replace
 

# column definitions for the report output
columns:
  - name: report_date
    type: date
    description: "Calendar date of the pickup"
    primary_key: true
  - name: taxi_type
    type: string
    description: "Yellow/green taxi identifier"
    primary_key: true
  - name: payment_type_desc
    type: string
    description: "Readable payment method"
    primary_key: true
  - name: trip_count
    type: bigint
    description: "Number of trips in this group"
    checks:
      - name: non_negative
  - name: total_fare
    type: float
    description: "Sum of fare_amount"
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
    CAST(pickup_datetime AS DATE) AS report_date,
    taxi_type,
    payment_type_desc,
    COUNT(*) AS trip_count,
    SUM(fare_amount) AS total_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}' AND fare_Amount >= 0
GROUP BY
    report_date,
    taxi_type,
    payment_type_desc
