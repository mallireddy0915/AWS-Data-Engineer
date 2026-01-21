variable "region" {
  type    = string
  default = "us-east-2"
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "redshift_admin_user" {
  type    = string
  default = "adminuser"
}

variable "redshift_admin_password" {
  type = string
  # set via TF_VAR_redshift_admin_password
}

variable "s3_bucket" {
  type    = string
  default = "arjun-s3-776312084600"
}

resource "aws_redshift_subnet_group" "rs_subnets" {
  name       = "oobt-redshift-subnet-group"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "rs_sg" {
  name        = "oobt-redshift-sg"
  description = "Redshift access"
  vpc_id      = var.vpc_id
}

# IMPORTANT: For demo, you can open inbound from your IP. Better: VPN/bastion.
resource "aws_security_group_rule" "rs_inbound" {
  type              = "ingress"
  from_port         = 5439
  to_port           = 5439
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"] # tighten later
  security_group_id = aws_security_group.rs_sg.id
}

resource "aws_security_group_rule" "rs_outbound" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.rs_sg.id
}

# IAM role for COPY from S3
resource "aws_iam_role" "rs_copy_role" {
  name = "oobt-redshift-copy-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "redshift.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "rs_copy_policy" {
  name = "oobt-redshift-copy-policy"
  role = aws_iam_role.rs_copy_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:ListBucket"],
        Resource = ["arn:aws:s3:::${var.s3_bucket}"]
      },
      {
        Effect = "Allow",
        Action = ["s3:GetObject"],
        Resource = ["arn:aws:s3:::${var.s3_bucket}/*"]
      }
    ]
  })
}

resource "aws_redshift_cluster" "oobt_rs" {
  cluster_identifier = "oobt-redshift"
  node_type          = "ra3.xlplus"
  cluster_type       = "single-node"  # demo-friendly
  database_name      = "dev"
  master_username    = var.redshift_admin_user
  master_password    = var.redshift_admin_password

  iam_roles          = [aws_iam_role.rs_copy_role.arn]
  vpc_security_group_ids = [aws_security_group.rs_sg.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.rs_subnets.name

  publicly_accessible = true # demo-friendly; lock down later

  skip_final_snapshot = true
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.oobt_rs.endpoint
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.rs_copy_role.arn
}
