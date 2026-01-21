import os
import pandas as pd
from pathlib import Path
from deltalake import write_deltalake, DeltaTable

CURATED_LOCAL = os.getenv("CURATED_LOCAL", "tmp/curated_yellow_2025-08_enriched.parquet")
DELTA_PATH = os.getenv("DELTA_PATH", "tmp/delta_yellow_curated")

def main():
    Path("tmp").mkdir(exist_ok=True)

    df = pd.read_parquet(CURATED_LOCAL)

    # Write initial version (v0)
    write_deltalake(DELTA_PATH, df, mode="overwrite")
    print(f"Delta written at: {DELTA_PATH} (version 0)")

    # Simulate a change: add a column (or update a few rows)
    df2 = df.copy()
    df2["audit_note"] = "day6_append"
    # Ensure columns match original schema for append
    df2 = df2[df.columns.tolist()]
    write_deltalake(DELTA_PATH, df2, mode="append")
    print("Appended data (creates new version)")

    dt = DeltaTable(DELTA_PATH)
    print("Current version:", dt.version())

    # Time travel reads
    dt_v0 = DeltaTable(DELTA_PATH, version=0)
    dt_v1 = DeltaTable(DELTA_PATH, version=1)

    print("Rows v0:", dt_v0.to_pandas().shape[0])
    print("Rows v1:", dt_v1.to_pandas().shape[0])

    # Show delta history (audit trail)
    hist = dt.history()
    print("History:")
    print(hist)

if __name__ == "__main__":
    main()
