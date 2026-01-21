import os
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---- CONFIG ----
PARQUET_PATH = os.getenv("TRIPS_PARQUET", "yellow_tripdata_2025-08.parquet")
OUTPUT_MD = os.getenv("DICT_OUT", "docs/governed_data_dictionary.md")

# Starter TLC-like meanings for common columns (you can extend anytime)
MEANINGS = {
    "VendorID": "Taxi vendor identifier",
    "tpep_pickup_datetime": "Datetime when the meter was engaged (pickup time)",
    "tpep_dropoff_datetime": "Datetime when the meter was disengaged (dropoff time)",
    "passenger_count": "Number of passengers (reported by driver)",
    "trip_distance": "Trip distance in miles reported by taximeter",
    "RatecodeID": "Rate code at end of trip",
    "store_and_fwd_flag": "Whether trip record was held in vehicle then forwarded",
    "PULocationID": "Pickup taxi zone ID",
    "DOLocationID": "Dropoff taxi zone ID",
    "payment_type": "Payment type code",
    "fare_amount": "Fare amount (time + distance)",
    "extra": "Extra charges (e.g., night, peak)",
    "mta_tax": "MTA tax",
    "tip_amount": "Tip amount",
    "tolls_amount": "Tolls amount",
    "improvement_surcharge": "Improvement surcharge",
    "total_amount": "Total amount charged",
    "congestion_surcharge": "Congestion surcharge",
    "airport_fee": "Airport fee",
    "cbd_congestion_fee": "CBD congestion fee (if present)",
}

DEFAULT_GOV = {
    # simple default governance; you can tune later
    "Sensitivity": "Internal",
    "Owner": "DataEngineering",
    "Steward": "DataEngineering",
    "Retention": "730 days",
}

QUALITY_RULES = {
    "tpep_pickup_datetime": "NOT NULL; must be <= tpep_dropoff_datetime",
    "tpep_dropoff_datetime": "NOT NULL; must be >= tpep_pickup_datetime",
    "trip_distance": ">= 0",
    "total_amount": ">= 0",
    "PULocationID": "Must exist in taxi_zone_lookup.LocationID",
    "DOLocationID": "Must exist in taxi_zone_lookup.LocationID",
}

def ensure_parent(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def main():
    ensure_parent(OUTPUT_MD)

    df = pd.read_parquet(PARQUET_PATH)
    cols = list(df.columns)
    dtypes = df.dtypes.astype(str).to_dict()

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = []
    lines.append(f"# Governed Data Dictionary (Generated)\n")
    lines.append(f"- Source parquet: `{PARQUET_PATH}`\n")
    lines.append(f"- Generated: {now}\n\n")

    lines.append("| Column | Data Type | Business Meaning | Sensitivity | Owner | Steward | Retention | Quality Rule |\n")
    lines.append("|---|---|---|---|---|---|---|---|\n")

    for c in cols:
        meaning = MEANINGS.get(c, "TODO: Add business meaning (from TLC dictionary)")
        sens = DEFAULT_GOV["Sensitivity"]
        owner = DEFAULT_GOV["Owner"]
        steward = DEFAULT_GOV["Steward"]
        retention = DEFAULT_GOV["Retention"]
        rule = QUALITY_RULES.get(c, "TODO: Add quality rule")
        lines.append(f"| `{c}` | `{dtypes.get(c,'')}` | {meaning} | {sens} | {owner} | {steward} | {retention} | {rule} |\n")

    Path(OUTPUT_MD).write_text("".join(lines), encoding="utf-8")
    print(f"Wrote governed dictionary: {OUTPUT_MD}")

if __name__ == "__main__":
    main()