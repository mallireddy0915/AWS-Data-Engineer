import os
import psycopg2
import pandas as pd
from pathlib import Path
from datetime import datetime

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

def main():
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        df = pd.read_sql("""
          SELECT v.vendor_id, v.vendor_name, l.lifecycle_state
          FROM mdm.dim_vendor v
          LEFT JOIN mdm.vendor_lifecycle l ON v.vendor_id = l.vendor_id;
        """, conn)

        Path("docs").mkdir(exist_ok=True)
        out = Path("docs/day10_vendor_profile.md")

        lines = []
        lines.append("# Day 10 — Vendor Master Profiling\n")
        lines.append(f"- Generated UTC: {datetime.utcnow().isoformat()}Z\n\n")
        lines.append(f"- Total vendors: **{len(df)}**\n")
        lines.append(f"- Null names: **{df['vendor_name'].isna().sum()}**\n")
        lines.append("\n## Lifecycle distribution\n\n")
        dist = df["lifecycle_state"].fillna("UNKNOWN").value_counts()
        for k, v in dist.items():
            lines.append(f"- {k}: {v}\n")

        out.write_text("".join(lines), encoding="utf-8")
        print(f"Wrote {out}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
