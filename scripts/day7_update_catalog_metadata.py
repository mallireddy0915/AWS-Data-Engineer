import os
import boto3

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
GLUE_DB = os.getenv("GLUE_DB", "nyc_taxi_db")
DOMAIN = os.getenv("DATA_DOMAIN", "NYC_Taxi")
OWNER = os.getenv("DATA_OWNER", "DataEngineering")
CLASSIFICATION = os.getenv("CLASSIFICATION", "Internal")

# Put the lineage S3 URI you wrote from Glue job
LINEAGE_S3_URI = os.getenv("LINEAGE_S3_URI", "")

def main():
    glue = boto3.client("glue", region_name=AWS_REGION)

    tables = []
    resp = glue.get_tables(DatabaseName=GLUE_DB)
    tables.extend(resp.get("TableList", []))

    # paginate
    while "NextToken" in resp:
        resp = glue.get_tables(DatabaseName=GLUE_DB, NextToken=resp["NextToken"])
        tables.extend(resp.get("TableList", []))

    print(f"Found {len(tables)} tables in {GLUE_DB}")

    for t in tables:
        name = t["Name"]

        # Only tag curated tables (crawler prefix)
        if not name.startswith("curated_"):
            continue

        params = t.get("Parameters", {})
        params.update({
            "data_owner": OWNER,
            "domain": DOMAIN,
            "classification": CLASSIFICATION,
            "quality_score": "0.98",   # example; later compute from reports
        })
        if LINEAGE_S3_URI:
            params["lineage_s3_uri"] = LINEAGE_S3_URI

        # Update table input (must include required fields)
        table_input = {
            "Name": t["Name"],
            "StorageDescriptor": t["StorageDescriptor"],
            "PartitionKeys": t.get("PartitionKeys", []),
            "TableType": t.get("TableType", "EXTERNAL_TABLE"),
            "Parameters": params
        }
        # Preserve optional fields
        if "Description" in t:
            table_input["Description"] = t["Description"]

        glue.update_table(DatabaseName=GLUE_DB, TableInput=table_input)
        print(f"Updated governance metadata for table: {name}")

if __name__ == "__main__":
    main()
