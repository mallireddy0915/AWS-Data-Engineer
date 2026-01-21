variable "bucket_name" { type = string default = "arjun-s3-776312084600" }

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket_sse" {
  bucket = var.bucket_name

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.oobt_data_key.arn
    }
  }
}

resource "aws_s3_bucket_policy" "enforce_kms" {
  bucket = var.bucket_name
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # Deny if not using SSE-KMS
      {
        Sid: "DenyIncorrectEncryptionHeader",
        Effect: "Deny",
        Principal: "*",
        Action: "s3:PutObject",
        Resource: "arn:aws:s3:::${var.bucket_name}/*",
        Condition: {
          StringNotEquals: { "s3:x-amz-server-side-encryption": "aws:kms" }
        }
      },
      # Deny if wrong KMS key
      {
        Sid: "DenyWrongKMSKey",
        Effect: "Deny",
        Principal: "*",
        Action: "s3:PutObject",
        Resource: "arn:aws:s3:::${var.bucket_name}/*",
        Condition: {
          StringNotEquals: { "s3:x-amz-server-side-encryption-aws-kms-key-id": aws_kms_key.oobt_data_key.arn }
        }
      }
    ]
  })
}
