import json
import sys
from pathlib import Path

import yaml

def main():
    paths = [
        "governance/quality_rules.yaml",
        "governance/match_merge.yaml",
        "governance/workflows/approval_gates.yaml",
    ]
    for p in paths:
        data = yaml.safe_load(Path(p).read_text())
        assert isinstance(data, dict), f"{p} should parse to a dict"
        print(f"YAML ok: {p}")

    schema_path = Path("governance/metadata/metadata_schema.json")
    schema = json.loads(schema_path.read_text())
    assert "$schema" in schema, "metadata_schema.json missing $schema"
    print("JSON schema ok:", schema_path)

    # Optional: validate your existing manifest if you have it
    manifest_candidates = list(Path(".").glob("**/*manifest*.json"))
    if manifest_candidates:
        print("Found manifest candidates (not validating fully without jsonschema lib):")
        for m in manifest_candidates[:5]:
            print("  -", m)

    print("Governance-as-code validation passed")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Validation failed:", e)
        sys.exit(1)
