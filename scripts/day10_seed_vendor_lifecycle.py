import os
import psycopg2

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

UPDATED_BY = os.getenv("UPDATED_BY", "seed_script")

def connect():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)

def main():
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT vendor_id FROM mdm.dim_vendor;")
            vids = [r[0] for r in cur.fetchall()]

            for vid in vids:
                cur.execute("""
                  INSERT INTO mdm.vendor_lifecycle(vendor_id, lifecycle_state, state_reason, updated_by)
                  VALUES (%s,'ACTIVE','Seed active state',%s)
                  ON CONFLICT (vendor_id) DO NOTHING;
                """, (vid, UPDATED_BY))
        conn.commit()
        print(f"Seeded lifecycle ACTIVE for {len(vids)} vendors.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
