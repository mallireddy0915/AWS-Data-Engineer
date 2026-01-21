import os, json
import psycopg2

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

AUTO_THRESHOLD = float(os.getenv("AUTO_THRESHOLD", "0.95"))
APPROVED_BY = os.getenv("APPROVED_BY", "auto_merge_bot")

def connect():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)

def longest(a, b):
    a = "" if a is None else str(a)
    b = "" if b is None else str(b)
    return a if len(a) >= len(b) else b

def main():
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
              SELECT review_id, left_vendor_id, right_vendor_id, confidence, rationale
              FROM mdm.vendor_review_queue
              WHERE status='OPEN' AND recommendation='AUTO_MERGE' AND confidence >= %s
              ORDER BY confidence DESC;
            """, (AUTO_THRESHOLD,))
            items = cur.fetchall()

        if not items:
            print("No AUTO_MERGE items found.")
            return

        with conn.cursor() as cur:
            for review_id, left_id, right_id, conf, rationale in items:
                # Choose survivor (simple): smaller id survives
                survivor = min(left_id, right_id)
                retired  = max(left_id, right_id)

                # Fetch names
                cur.execute("SELECT vendor_name FROM mdm.dim_vendor WHERE vendor_id=%s;", (survivor,))
                s_name = cur.fetchone()[0]
                cur.execute("SELECT vendor_name FROM mdm.dim_vendor WHERE vendor_id=%s;", (retired,))
                r_name = cur.fetchone()[0]

                new_name = longest(s_name, r_name)

                # Update survivor (causes audit trigger UPDATE)
                cur.execute("""
                  UPDATE mdm.dim_vendor
                  SET vendor_name=%s, updated_by=%s, approved_by=%s, approved_at=NOW(), version=version+1, updated_at=NOW()
                  WHERE vendor_id=%s;
                """, (new_name, "auto_merge", APPROVED_BY, survivor))

                # Deprecate retired in lifecycle table (do not delete for compliance)
                cur.execute("""
                  INSERT INTO mdm.vendor_lifecycle(vendor_id, lifecycle_state, state_reason, updated_by, approved_by, approved_at)
                  VALUES (%s,'DEPRECATED',%s,%s,%s,NOW())
                  ON CONFLICT (vendor_id) DO UPDATE SET
                    lifecycle_state='DEPRECATED',
                    state_reason=EXCLUDED.state_reason,
                    updated_at=NOW(),
                    updated_by=EXCLUDED.updated_by,
                    approved_by=EXCLUDED.approved_by,
                    approved_at=NOW();
                """, (retired, f"Auto-merged into vendor_id={survivor} (conf={conf})", "auto_merge", APPROVED_BY))

                # Mark queue row merged
                cur.execute("""
                  UPDATE mdm.vendor_review_queue
                  SET status='MERGED', reviewed_at=NOW(), reviewed_by=%s, decision_notes=%s
                  WHERE review_id=%s;
                """, (APPROVED_BY, f"Survivor={survivor}, Deprecated={retired}", review_id))

        conn.commit()
        print(f"Applied {len(items)} auto merges (audit logged via trigger).")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
