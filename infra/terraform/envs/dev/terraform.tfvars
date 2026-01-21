aws_region  = "us-east-2"
bucket_name = "arjun-s3-776312084600"

db_identifier = "nyc-taxi-dev"
db_name       = "nycyellowtaxi"
db_username   = "postgres"
db_password   = "Mallikarjun09"

vpc_id = "vpc-0e7514ad924835df6"
subnet_ids = [
  "subnet-032d4ee41a288e887",
  "subnet-003e550f47e6a9bcc",
  "subnet-0203298991bb279fb"
]
allowed_cidr_blocks = ["108.250.170.82/32"]

lambda_name = "nyc-taxi-dev-stub"
lambda_zip_path = "../../../../lambda_stub.zip"
