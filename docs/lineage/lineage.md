# NYC Taxi Lineage (Day 18)

```mermaid
flowchart LR
  A[S3 Raw: yellow_tripdata parquet/csv] --> B[Glue ETL: curated]
  Z[S3 Master: zones snapshots] --> B
  B --> C[S3 Curated: curated_yellow]
  C --> D[Redshift: fact_yellow_trip]
  Z --> E[Redshift: dim_zone]
  D --> Q[QuickSight Dashboard]
  E --> Q
  B --> L[S3 Lineage Artifacts]
  SF[Step Functions Orchestration] --> B
  SF --> DQ[Glue Data Quality Gate]
  SF --> AUD[DynamoDB Audit: pipeline_audit_runs]
  CT[CloudTrail] --> CWL[CloudWatch Logs]
  CWL --> AL[Alarms -> SNS]
