import boto3
import json
import os

s3 = boto3.client('s3')
bucket_name = 'arjun-s3-776312084600' # MUST match the bucket name from Script 2

def upload_and_tag():
 
    # 1. Upload to 'raw/' prefix (Simulating a folder)
    key = "NYC-Yellow-Trips-Taxi-Data/csv/taxi_zone_lookup.csv"

    print(f"Uploading {key}...")
    
    s3.upload_file("/Users/arjun/OUBT/Data Engineer/taxi_zone_lookup.csv", bucket_name, key)

    key = "NYC-Yellow-Trips-Taxi-Data/parquet/yellow_tripdata_2025-08.parquet"

    print(f"Uploading {key}...")
    
    s3.upload_file("/Users/arjun/OUBT/Data Engineer/yellow_tripdata_2025-08.parquet", bucket_name, key)
    
    # 3. Add Governance Tags (Owner, Classification)
    print("Applying tags...")
    s3.put_object_tagging(
        Bucket=bucket_name,
        Key="NYC-Yellow-Trips-Taxi-Data/" ,
        Tagging={
            'TagSet': [
                {'Key': 'Owner', 'Value': 'Arjun'},
                {'Key': 'Project', 'Value': 'OUBT'}
        
            ]
        }
    )
    print("File uploaded and tagged.")

    # 4. Create and Upload Metadata Manifest (YAML/JSON)
    manifest = {
        "dataset": "NYC-Yellow-Trips-Taxi-Data",
        "bucket": bucket_name,
        "owner": "Arjun",
        "schema_version": "1.0",
        "security_level": "high"
    }
    
    # We convert the Python dictionary directly to JSON and upload it
    manifest_json = json.dumps(manifest, indent=2)
    
    s3.put_object(
        Bucket=bucket_name,
        Key='metadata/manifest.json',
        Body=manifest_json,
        ContentType='application/json'
    )
    print("Manifest file generated and uploaded to 'metadata/' prefix.")
    

if __name__ == "__main__":
    upload_and_tag()