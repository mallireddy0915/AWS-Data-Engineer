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
  description = "Subnets for RDS subnet group"
}

variable "instance_class" {
  type    = string
  default = "db.t3.micro"
}

variable "tags" {
  type = map(string)
}
