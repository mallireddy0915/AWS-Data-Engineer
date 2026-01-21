import os
import re
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from difflib import SequenceMatcher

ZONES_CSV = os.getenv("ZONES_CSV", "taxi_zone_lookup.csv")
OUT_JSON = os.getenv("DUP_OUT", "docs/zone_duplicate_candidates.json")

def norm_text(s: str) -> str:
    s = "" if pd.isna(s) else str(s)
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    return s

def sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def main():
    Path("docs").mkdir(exist_ok=True)

    df = pd.read_csv(ZONES_CSV)
    df["borough_n"] = df["Borough"].map(norm_text)
    df["zone_n"] = df["Zone"].map(norm_text)
    df["service_n"] = df["service_zone"].map(norm_text)

    # 1) Exact duplicate candidates on normalized composite
    df["composite"] = df["borough_n"] + "|" + df["zone_n"] + "|" + df["service_n"]
    dup_groups = df[df.duplicated("composite", keep=False)].sort_values("composite")

    exact = []
    if not dup_groups.empty:
        for comp, g in dup_groups.groupby("composite"):
            exact.append({
                "type": "exact_composite_duplicate",
                "composite": comp,
                "records": g[["LocationID","Borough","Zone","service_zone"]].to_dict(orient="records")
            })

    # 2) Fuzzy: same borough+service_zone, zone strings very similar
    fuzzy = []
    threshold = 0.92
    for (b, s), g in df.groupby(["borough_n", "service_n"]):
        zones = g[["LocationID","Zone","zone_n","Borough","service_zone"]].to_dict(orient="records")
        n = len(zones)
        if n < 2:
            continue
        for i in range(n):
            for j in range(i+1, n):
                a = zones[i]["zone_n"]
                b2 = zones[j]["zone_n"]
                if not a or not b2:
                    continue
                score = sim(a, b2)
                if score >= threshold and a != b2:
                    fuzzy.append({
                        "type": "fuzzy_zone_duplicate",
                        "borough": zones[i]["Borough"],
                        "service_zone": zones[i]["service_zone"],
                        "score": round(score, 4),
                        "a": zones[i],
                        "b": zones[j]
                    })

    report = {
        "generated_utc": datetime.utcnow().isoformat() + "Z",
        "input": ZONES_CSV,
        "exact_candidates": exact,
        "fuzzy_candidates": fuzzy,
        "counts": {
            "exact_groups": len(exact),
            "fuzzy_pairs": len(fuzzy)
        }
    }

    Path(OUT_JSON).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote duplicate candidate report: {OUT_JSON}")
    print("Counts:", report["counts"])

if __name__ == "__main__":
    main()
