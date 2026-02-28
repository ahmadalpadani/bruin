"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendorid
    type: integer
  - name: tpep_pickup_datetime
    type: timestamp
  - name: tpep_dropoff_datetime
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

@bruin"""


def materialize():
    import os
    import json
    import pandas as pd
    from datetime import datetime

    # 🔥 FIX WINDOWS TIMEZONE ISSUE
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")

    if start_date is None or end_date is None:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")

    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    pipeline_vars = json.loads(bruin_vars)
    taxi_types = pipeline_vars.get("taxi_types", ["yellow", "green"])

    def month_range(start, end):
        cur = start.replace(day=1)
        while cur < end:
            yield cur.year, cur.month
            if cur.month == 12:
                cur = cur.replace(year=cur.year + 1, month=1)
            else:
                cur = cur.replace(month=cur.month + 1)

    start_dt = datetime.fromisoformat(start_date).date()
    end_dt = datetime.fromisoformat(end_date).date()

    dataframes = []

    for year, month in month_range(start_dt, end_dt):
        for taxi_type in taxi_types:
            url = (
                f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            )

            df = pd.read_parquet(url, engine="pyarrow")

            # Normalize column names to lowercase
            df.columns = df.columns.str.lower()

            df["taxi_type"] = taxi_type

            dataframes.append(df)

    if not dataframes:
        return pd.DataFrame(columns=[
            "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "ratecodeid",
            "store_and_fwd_flag", "pulocationid", "dolocationid",
            "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "improvement_surcharge",
            "total_amount", "congestion_surcharge", "airport_fee",
            "taxi_type",
        ])

    final_dataframe = pd.concat(dataframes, ignore_index=True)

    # 🔥 ENSURE airport_fee EXISTS (older months don't have it)
    if "airport_fee" not in final_dataframe.columns:
        final_dataframe["airport_fee"] = None

    # 🔥 FORCE UTC TIMESTAMP (prevents dlt crash)
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in final_dataframe.columns:
            final_dataframe[col] = pd.to_datetime(
                final_dataframe[col],
                utc=True,
                errors="coerce"
            )

    return final_dataframe