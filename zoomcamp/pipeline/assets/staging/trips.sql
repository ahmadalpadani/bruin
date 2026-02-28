/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

# column metadata for the staging output; includes the lookup description
columns:
  - name: vendorid
    type: integer
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: passenger_count
    type: float
  - name: trip_distance
    type: float
  - name: ratecodeid
    type: float
  - name: store_and_fwd_flag
    type: string
  - name: pulocationid
    type: integer
  - name: dolocationid
    type: integer
  - name: payment_type
    type: integer
  - name: payment_type_desc
    type: string
  - name: fare_amount
    type: float
  - name: extra
    type: float
  - name: mta_tax
    type: float
  - name: tip_amount
    type: float
  - name: tolls_amount
    type: float
  - name: improvement_surcharge
    type: float
  - name: total_amount
    type: float
  - name: congestion_surcharge
    type: float
  - name: airport_fee
    type: float
  - name: taxi_type
    type: string


# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: no_duplicate_trips
    description: Ensure no duplicate trips in staging
    query: |
      SELECT COUNT(*) FROM (
        SELECT
          vendorid,
          pickup_datetime,
          dropoff_datetime,
          pulocationid,
          dolocationid,
          fare_amount
        FROM staging.trips
        GROUP BY
          vendorid,
          pickup_datetime,
          dropoff_datetime,
          pulocationid,
          dolocationid,
          fare_amount
        HAVING COUNT(*) > 1
      )
    value: 0

@bruin */

-- staging query with deduplication and enrichment
WITH raw_window AS (
  SELECT *
  FROM ingestion.trips
  WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
    AND tpep_pickup_datetime < '{{ end_datetime }}'
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY vendorid,
                   tpep_pickup_datetime,
                   tpep_dropoff_datetime,
                   pulocationid,
                   dolocationid,
                   fare_amount
      ORDER BY tpep_pickup_datetime
    ) AS rn
  FROM raw_window
)
SELECT
    vendorid,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    ranked.payment_type,
    p.payment_type_name AS payment_type_desc,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    taxi_type
FROM ranked
LEFT JOIN ingestion.payment_lookup AS p
  ON ranked.payment_type = p.payment_type_id
WHERE rn = 1
