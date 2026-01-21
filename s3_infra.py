import boto3

s3 = boto3.client('s3')
region = 'us-east-2'  # Change this if you are in a different region
bucket_name = 'arjun-s3-776312084600' # CHANGE THIS to be unique

def setup_s3_infrastructure():
    # 1. Create Bucket
    print(f"Creating bucket: {bucket_name}...")
    try:
        if region == 'us-east-2':
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print("Bucket Created.")
    except Exception as e:
        print(f"Bucket might already exist: {e}")

    # 2. Enable Versioning
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={'Status': 'Enabled'}
    )
    print("Versioning Enabled.")

    # 3. Set Lifecycle Rule (Move 'raw/' data to Glacier after 30 days)
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'MoveRawToGlacier',
                'Prefix': 'raw/',  # Targets the 'raw' folder
                'Status': 'Enabled',
                'Transitions': [
                    {
                        'Days': 30,
                        'StorageClass': 'GLACIER'
                    }
                ]
            }
        ]
    }
    
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle_config
    )
    print("Lifecycle Policy Applied (Raw -> Glacier after 30 days).")

if __name__ == "__main__":
    setup_s3_infrastructure()