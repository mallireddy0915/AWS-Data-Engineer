variable "aws_region" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "db_identifier" {
  type = string
}

variable "db_name" {
  type = string
}

variable "db_username" {
  type = string
}

variable "db_password" {
  type      = string
  sensitive = true
}

variable "vpc_id" {
  type = string
}

variable "allowed_cidr_blocks" {
  type = list(string)
}

variable "subnet_ids" {
  type        = list(string)
  description = "Subnet IDs for RDS"
}

variable "lambda_name" {
  type = string
}

variable "lambda_zip_path" {
  type        = string
  description = "Path to Lambda deployment zip"
  default     = "lambda_stub.zip"
}

variable "tags" {
  type = map(string)
  default = {
    Project = "OUBT"
    Domain  = "NYC_Taxi"
    Env     = "dev"
  }
}
