import os, subprocess, time
from datetime import datetime
from pathlib import Path

PGHOST = os.getenv("PGHOST")
PGUSER = os.getenv("PGUSER")
PGDATABASE = os.getenv("PGDATABASE")
PGPORT = os.getenv("PGPORT", "5432")

SQL_FILES = [
    "sql/00_admin/001_create_analytics_schema.sql",
    "sql/30_quality/030_quality_functions.sql",
    "sql/10_transforms/010_trip_metrics_by_zone_day.sql",
    "sql/20_views/020_mv_revenue_by_borough_month.sql",
    "sql/20_views/021_refresh_materialized_views.sql",
    "sql/30_quality/031_run_all_tests_procedure.sql",
]

def run_psql(file_path: str):
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "-f", file_path]
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd)

def run_tests(run_id: str):
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "-c", f"CALL dq.run_all_tests('{run_id}');"]
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    for v in [PGHOST, PGUSER, PGDATABASE]:
        if not v:
            raise SystemExit("Set PGHOST, PGUSER, PGDATABASE (and PGPASSWORD if needed).")

    run_id = "sqlrun_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    for f in SQL_FILES:
        run_psql(f)

    run_tests(run_id)
    print(f"SQL workflow complete. run_id={run_id}")

if __name__ == "__main__":
    main()
