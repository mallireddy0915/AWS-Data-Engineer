import os, json
import yaml
import psycopg2
from psycopg2.extras import execute_values
from rapidfuzz.distance import JaroWinkler
from rapidfuzz.fuzz import ratio
import re

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

RULES_PATH = os.getenv("VENDOR_RULES", "governance/mdm/vendor_match_rules.yaml")
CREATED_BY = os.getenv("CREATED_BY", "data_engineer")

def norm(s: str, lowercase=True, strip_punct=True, strip_punctuation=None):
    # Accept both strip_punct and strip_punctuation for compatibility
    if strip_punctuation is not None:
        strip_punct = strip_punctuation
    s = "" if s is None else str(s)
    if lowercase:
        s = s.lower()
    s = s.strip()
    if strip_punct:
        s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def connect():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)

def load_rules():
    cfg = yaml.safe_load(open(RULES_PATH, "r", encoding="utf-8"))
    th = cfg["thresholds"]
    norm_cfg = cfg.get("normalization", {})
    weights = {f["name"]: float(f["weight"]) for f in cfg["fields"]}
    return cfg, th, norm_cfg, weights

def fetch_vendors(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT vendor_id, vendor_name FROM mdm.dim_vendor;")
        return cur.fetchall()  # [(id, name), ...]

def compute_confidence(a_name, b_name):
    # Two signals:
    # - ratio: normalized Levenshtein similarity (0..100)
    # - jaro-winkler: (0..1)
    lev = ratio(a_name, b_name) / 100.0
    jw  = JaroWinkler.normalized_similarity(a_name, b_name)
    # Combine (simple probabilistic-ish ensemble)
    conf = 0.55 * jw + 0.45 * lev
    return max(0.0, min(1.0, conf)), {"lev_ratio": lev, "jaro_winkler": jw}

def main():
    cfg, th, norm_cfg, weights = load_rules()
    auto_t = float(th["auto_merge"])
    review_t = float(th["steward_review"])

    conn = connect()
    try:
        vendors = fetch_vendors(conn)
        # Normalize once
        rows = []
        for vid, vname in vendors:
            rows.append({"vendor_id": int(vid), "vendor_name": vname})

        # Candidate generation (simple): compare all pairs if small set
        # If large, add blocking keys (e.g., first letter, token)
        candidates = []
        n = len(rows)
        for i in range(n):
            for j in range(i+1, n):
                a = rows[i]
                b = rows[j]
                a_n = norm(a["vendor_name"], **norm_cfg)
                b_n = norm(b["vendor_name"], **norm_cfg)
                if not a_n or not b_n:
                    continue

                conf, details = compute_confidence(a_n, b_n)

                if conf >= auto_t:
                    rec = "AUTO_MERGE"
                elif conf >= review_t:
                    rec = "STEWARD_REVIEW"
                else:
                    rec = "MANUAL"

                # Only keep review-worthy pairs (optional: keep everything)
                if conf >= review_t:
                    candidates.append((
                        a["vendor_id"], b["vendor_id"], conf, rec,
                        json.dumps({"a_name": a["vendor_name"], "b_name": b["vendor_name"], **details})
                    ))

        print(f"Found {len(candidates)} candidates with conf >= {review_t}")

        if candidates:
            with conn.cursor() as cur:
                # Avoid duplicates by clearing OPEN items first (optional). Keep simple:
                insert_sql = """
                INSERT INTO mdm.vendor_review_queue
                  (left_vendor_id, right_vendor_id, confidence, recommendation, rationale, created_by)
                VALUES %s;
                """
                execute_values(cur, insert_sql, candidates, page_size=500)
            conn.commit()
            print("Inserted review candidates into mdm.vendor_review_queue")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
