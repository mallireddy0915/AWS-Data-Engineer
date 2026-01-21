import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

ZONES_CSV = os.getenv("ZONES_CSV", "taxi_zone_lookup.csv")
TRIPS_PARQUET = os.getenv("TRIPS_PARQUET", "yellow_tripdata_2025-08.parquet")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

CREATED_BY = os.getenv("CREATED_BY", "data_engineer")
APPROVED_BY = os.getenv("APPROVED_BY", "data_steward")

PAYMENT_TYPE_MAP = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

def connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

def upsert_zones(conn):
    zones = pd.read_csv(ZONES_CSV)
    zones = zones.rename(columns={"LocationID":"location_id","Borough":"borough","Zone":"zone","service_zone":"service_zone"})
    zones = zones[["location_id","borough","zone","service_zone"]].copy()

    rows = [(int(r.location_id), r.borough, r.zone, r.service_zone, CREATED_BY, APPROVED_BY)
            for r in zones.itertuples(index=False)]

    sql = """
    INSERT INTO mdm.dim_zone (location_id, borough, zone, service_zone, created_by, approved_by, approved_at, updated_by)
    VALUES %s
    ON CONFLICT (location_id) DO UPDATE SET
      borough = EXCLUDED.borough,
      zone = EXCLUDED.zone,
      service_zone = EXCLUDED.service_zone,
      version = mdm.dim_zone.version + 1,
      updated_at = NOW(),
      updated_by = EXCLUDED.updated_by,
      approved_by = EXCLUDED.approved_by,
      approved_at = NOW();
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    print(f"Upserted zones: {len(rows)}")

def upsert_code_set(conn, table, id_col, desc_col, pairs):
    # pairs: list of (id, desc)
    rows = [(int(i), d, CREATED_BY, APPROVED_BY) for i, d in pairs]

    sql = f"""
    INSERT INTO {table} ({id_col}, {desc_col}, created_by, approved_by, approved_at, updated_by)
    VALUES %s
    ON CONFLICT ({id_col}) DO UPDATE SET
      {desc_col} = EXCLUDED.{desc_col},
      version = {table}.version + 1,
      updated_at = NOW(),
      updated_by = EXCLUDED.updated_by,
      approved_by = EXCLUDED.approved_by,
      approved_at = NOW();
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    print(f"Upserted {table}: {len(rows)}")

def main():
    trips = pd.read_parquet(TRIPS_PARQUET)

    try:
        conn = connect()
    except psycopg2.OperationalError as e:
        print(f"⚠️  PostgreSQL not available: {e}")
        print("\n📋 Running in DEMO mode (no database)...")
        print(f"   Would load zones from: {ZONES_CSV}")
        print(f"   Would load trips from: {TRIPS_PARQUET}")
        
        # Demo: Show what would be loaded
        zones = pd.read_csv(ZONES_CSV)
        print(f"\n✅ Zones to load: {len(zones)}")
        
        if "VendorID" in trips.columns:
            vendor_ids = sorted(set(trips["VendorID"].dropna().astype(int).tolist()))
            print(f"✅ Vendors to load: {len(vendor_ids)} - {vendor_ids}")
        
        if "RatecodeID" in trips.columns:
            rate_ids = sorted(set(trips["RatecodeID"].dropna().astype(int).tolist()))
            print(f"✅ Rate codes to load: {len(rate_ids)} - {rate_ids}")
        
        if "payment_type" in trips.columns:
            pay_ids = sorted(set(trips["payment_type"].dropna().astype(int).tolist()))
            print(f"✅ Payment types to load: {len(pay_ids)} - {pay_ids}")
        
        print("\n📌 To run with PostgreSQL:")
        print("   1. Start PostgreSQL: brew services start postgresql")
        print("   2. Create MDM schema: psql -f sql/day4_mdm_schema.sql")
        print("   3. Re-run this script")
        return
    
    try:
        upsert_zones(conn)

        # Vendors (names unknown in dataset; store as "Vendor <id>")
        if "VendorID" in trips.columns:
            vendor_ids = sorted(set(trips["VendorID"].dropna().astype(int).tolist()))
            vendor_pairs = [(vid, f"Vendor {vid}") for vid in vendor_ids]
            upsert_code_set(conn, "mdm.dim_vendor", "vendor_id", "vendor_name", vendor_pairs)

        # Rate codes (descriptions unknown; store placeholder)
        if "RatecodeID" in trips.columns:
            rate_ids = sorted(set(trips["RatecodeID"].dropna().astype(int).tolist()))
            rate_pairs = [(rid, f"RateCode {rid}") for rid in rate_ids]
            upsert_code_set(conn, "mdm.dim_rate_code", "ratecode_id", "ratecode_desc", rate_pairs)

        # Payment types (use TLC mapping)
        if "payment_type" in trips.columns:
            pay_ids = sorted(set(trips["payment_type"].dropna().astype(int).tolist()))
            pay_pairs = [(pid, PAYMENT_TYPE_MAP.get(pid, f"PaymentType {pid}")) for pid in pay_ids]
            upsert_code_set(conn, "mdm.dim_payment_type", "payment_type_id", "payment_type_desc", pay_pairs)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
