import sys, os
from pathlib import Path
from datetime import datetime
import psycopg2

def read_sql(path):
    return Path(path).read_text(encoding="utf-8")

def main():
    # These are passed as Glue job parameters
    args = dict(a.split("=", 1) for a in sys.argv[1:] if "=" in a)

    host = args["--PGHOST"]
    db   = args["--PGDATABASE"]
    user = args["--PGUSER"]
    pwd  = args["--PGPASSWORD"]

    sql_files = args["--SQL_FILES"].split(",")
    run_id = "glue_sqlrun_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    conn = psycopg2.connect(host=host, dbname=db, user=user, password=pwd, port=int(args.get("--PGPORT","5432")))
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for f in sql_files:
                cur.execute(read_sql(f))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(f"CALL dq.run_all_tests('{run_id}');")
        conn.commit()

        print(f"Glue SQL workflow succeeded. run_id={run_id}")

    except Exception as e:
        conn.rollback()
        raise

    finally:
        conn.close()

if __name__ == "__main__":
    main()
