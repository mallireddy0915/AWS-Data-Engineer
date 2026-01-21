resource "aws_kms_key" "oobt_data_key" {
  description             = "OUBT data encryption key (S3/RDS/Redshift demo)"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "oobt_data_key_alias" {
  name          = "alias/oobt-data-key"
  target_key_id = aws_kms_key.oobt_data_key.key_id
}

output "oobt_kms_key_arn" {
  value = aws_kms_key.oobt_data_key.arn
}
