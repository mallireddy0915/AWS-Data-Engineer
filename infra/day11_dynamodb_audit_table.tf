resource "aws_dynamodb_table" "pipeline_audit" {
  name         = "pipeline_audit_runs"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = {
    Project = "OUBT"
    Domain  = "NYC_Taxi"
  }
}

resource "aws_sns_topic" "steward_alerts" {
  name = "mdm-steward-alerts"
  tags = {
    Project = "OUBT"
    Domain  = "NYC_Taxi"
  }
}
