terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

module "s3" {
  source      = "../../modules/s3"
  bucket_name = var.bucket_name
  tags        = var.tags
}

module "rds" {
  source              = "../../modules/rds"
  db_identifier       = var.db_identifier
  db_name             = var.db_name
  db_username         = var.db_username
  db_password         = var.db_password
  vpc_id              = var.vpc_id
  subnet_ids          = var.subnet_ids
  allowed_cidr_blocks = var.allowed_cidr_blocks
  tags                = var.tags
}

module "lambda" {
  source        = "../../modules/lambda"
  function_name = var.lambda_name
  zip_path      = var.lambda_zip_path
  tags          = var.tags
}
