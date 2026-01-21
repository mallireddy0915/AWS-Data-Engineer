import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

PARQUET_PATH = os.getenv("TRIPS_PARQUET", "yellow_tripdata_2025-08.parquet")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

TABLE = "staging.yellow_trips"

COLS = [
    "VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count",
    "trip_distance","RatecodeID","store_and_fwd_flag","PULocationID","DOLocationID",
    "payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount",
    "improvement_surcharge","total_amount","congestion_surcharge","airport_fee","cbd_congestion_fee"
]

def main():
    df = pd.read_parquet(PARQUET_PATH)
    df = df[[c for c in COLS if c in df.columns]].copy()

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    rows = [tuple(x) for x in df.itertuples(index=False, name=None)]
    cols_sql = ",".join(df.columns)
    sql = f"INSERT INTO {TABLE} ({cols_sql}) VALUES %s"

    chunk = 5000
    for i in range(0, len(rows), chunk):
        execute_values(cur, sql, rows[i:i+chunk])
        conn.commit()
        print(f"Inserted {min(i+chunk, len(rows))}/{len(rows)}")

    cur.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()