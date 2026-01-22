# Day 20 — End-to-End Governed Data Platform (NYC Yellow Taxi)

```mermaid
flowchart LR
  subgraph Ingestion
    SRC[Local Files: parquet/csv\nNYC Yellow Trips + Taxi Zones] --> S3RAW[S3 Raw Zone\ns3://arjun-s3-776312084600/raw/]
  end

  subgraph DataLake[S3 Data Lake Zones]
    S3RAW --> S3VAL[S3 Validated Zone\n.../validated/]
    S3VAL --> S3CUR[S3 Curated Zone\n.../curated/]
    S3MASTER[S3 Master Zone\n.../master/]
    S3ARCH[S3 Archive Zone\n.../archive/]
    S3LINE[S3 Lineage & DQ Results\n.../lineage/]
  end

  subgraph Orchestration
    SF[Step Functions\nGoverned Pipeline] --> GLUE[Glue ETL Job\nnyc-taxi-curated-job-fixed]
    SF --> DQ[Glue Data Quality Gate]
    SF --> AUD[DynamoDB Audit\npipeline_audit_runs]
    SF --> SNS[SNS Steward Alerts\nmdm-steward-alerts]
  end

  subgraph MDM[MDM Layer (RDS PostgreSQL)]
    ZSCD2[Zones SCD2\nmdm.dim_zone_scd2]
    VMDM[Vendor Master\n(dedup + lifecycle)]
    APPR[Approve/Rollback\nProcedures + Audit]
    ZSCD2 --> S3MASTER
    VMDM --> S3MASTER
  end

  subgraph Transform[SQL Transformations (Versioned)]
    GITSQL[Git: SQL scripts + headers\nquality tests] --> RDSA[Postgres Analytics Schema\nanalytics.*]
    RDSA --> DQSQL[dq.test_results]
  end

  subgraph Serving[Serving / Analytics]
    RS[Redshift Star Schema\nfact + dims] --> QS[QuickSight Dashboard\n+ Governance KPIs]
    ATH[Athena Workgroup\nCertified Views] --> QS
  end

  subgraph Monitoring[Observability + Governance Metrics]
    CW[CloudWatch Dashboard\nTech + Governance] --> AL[Alarms]
    CT[CloudTrail + Macie\nSecurity/PII Evidence] --> CW
  end

  S3RAW --> SF
  GLUE --> S3CUR
  GLUE --> S3LINE
  DQ --> S3LINE

  S3CUR --> RS
  S3MASTER --> RS
  RS --> ATH
