# NYC Yellow Taxi Data Platform — Architecture Overview

## Executive Summary

This document presents a comprehensive end-to-end **Governed Data Platform** built on AWS for NYC Yellow Taxi trip analytics. The platform implements enterprise-grade data engineering patterns including:

- **Multi-zone data lake** with governance controls
- **Master Data Management (MDM)** with deduplication and SCD Type 2
- **Automated orchestration** via AWS Step Functions
- **Data quality gates** with Great Expectations and AWS Glue DQ
- **Dimensional analytics** on Amazon Redshift and Athena
- **Real-time monitoring** with CloudWatch dashboards
- **CI/CD deployment** via CloudFormation

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    NYC YELLOW TAXI DATA PLATFORM                                                │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                 │
│  ┌──────────────┐    ┌────────────────────────────────────────────────────────────────────────┐                 │
│  │  INGESTION   │    │                        S3 DATA LAKE                                    │                 │
│  │              │    │  ┌─────────┐  ┌───────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │                 │
│  │ ┌──────────┐ │    │  │  raw/   │→→│ validated/│→→│ curated/ │→→│ master/  │  │ archive/ │ │                 │
│  │ │ Parquet  │ │───▶│  │(Landing)│  │(QA Gate) │  │(Enriched)│  │(Golden)  │  │(WORM)    │ │                 │
│  │ │ CSV      │ │    │  └─────────┘  └───────────┘  └──────────┘  └──────────┘  └──────────┘ │                 │
│  │ │ Batch    │ │    │                                   │              ▲                    │                 │
│  │ └──────────┘ │    │  ┌─────────────────────────────────────────────────┐                  │                 │
│  └──────────────┘    │  │          lineage/ (Audit Trail + DQ Results)   │                  │                 │
│                      │  └─────────────────────────────────────────────────┘                  │                 │
│                      └────────────────────────────────────────────────────────────────────────┘                 │
│                                                  │                                                              │
│    ┌───────────────────────────────────────────────────────────────────────────────────────────┐                │
│    │                              PROCESSING LAYER                                              │                │
│    │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │                │
│    │  │   AWS Glue ETL   │  │  AWS Lambda      │  │  Delta Lake      │  │  Spark Local     │  │                │
│    │  │  - Transform     │  │  - DQ Gate       │  │  - Time Travel   │  │  - Development   │  │                │
│    │  │  - Enrich        │  │  - Audit Logger  │  │  - Rollback      │  │  - Testing       │  │                │
│    │  │  - Partition     │  │  - Notifications │  │  - ACID          │  │                  │  │                │
│    │  └──────────────────┘  └──────────────────┘  └──────────────────┘  └──────────────────┘  │                │
│    └───────────────────────────────────────────────────────────────────────────────────────────┘                │
│                                                  │                                                              │
│    ┌───────────────────────────────────────────────────────────────────────────────────────────┐                │
│    │                            ORCHESTRATION (AWS STEP FUNCTIONS)                             │                │
│    │  ┌─────────┐  ┌───────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────────┐   │                │
│    │  │InitAudit│→→│MasterFreshness│→→│ RunGlueETL  │→→│  RunDQGate  │→→│FinalizeSuccess │   │                │
│    │  └─────────┘  └───────────────┘  └─────────────┘  └─────────────┘  └────────────────┘   │                │
│    │       │              │                  │               │                               │                │
│    │       └──────────────┴──────────────────┴───────────────┴──▶ DynamoDB Audit Table       │                │
│    │                                         │                                               │                │
│    │                           ┌─────────────┴─────────────┐                                 │                │
│    │                           │   SNS Steward Alerts      │                                 │                │
│    │                           │   (Failure Notifications) │                                 │                │
│    │                           └───────────────────────────┘                                 │                │
│    └───────────────────────────────────────────────────────────────────────────────────────────┘                │
│                                                  │                                                              │
│    ┌───────────────────────────────────────────────────────────────────────────────────────────┐                │
│    │                              MDM LAYER (RDS PostgreSQL)                                   │                │
│    │  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────────────────────┐  │                │
│    │  │   dim_zone_scd2    │  │   dim_vendor       │  │   vendor_review_queue              │  │                │
│    │  │   (SCD Type 2)     │  │   (Lifecycle Mgmt) │  │   (Match/Merge Candidates)         │  │                │
│    │  ├────────────────────┤  ├────────────────────┤  ├────────────────────────────────────┤  │                │
│    │  │ • Version Control  │  │ • Deduplication    │  │ • Confidence Scoring               │  │                │
│    │  │ • Effective Dating │  │ • State Machine    │  │ • Steward Review Workflow          │  │                │
│    │  │ • Audit Trail      │  │ • Survivorship     │  │ • Auto-Merge / Manual Review       │  │                │
│    │  └────────────────────┘  └────────────────────┘  └────────────────────────────────────┘  │                │
│    └───────────────────────────────────────────────────────────────────────────────────────────┘                │
│                                                  │                                                              │
│    ┌───────────────────────────────────────────────────────────────────────────────────────────┐                │
│    │                              SERVING LAYER                                                │                │
│    │  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐                │                │
│    │  │      Amazon Redshift            │  │      Amazon Athena              │                │                │
│    │  │  ┌───────────────────────────┐  │  │  ┌───────────────────────────┐  │                │                │
│    │  │  │ analytics.fact_yellow_trip│  │  │  │ Certified Views           │  │                │                │
│    │  │  │ analytics.dim_zone        │  │  │  │ Ad-hoc Queries            │  │                │                │
│    │  │  │ analytics.dim_vendor      │  │  │  │ Federated Queries         │  │                │                │
│    │  │  │ analytics.dim_date        │  │  │  │                           │  │                │                │
│    │  │  └───────────────────────────┘  │  │  └───────────────────────────┘  │                │                │
│    │  │  Star Schema (Kimball)          │  │  Workgroup: oubt-athena-wg      │                │                │
│    │  └─────────────────────────────────┘  └─────────────────────────────────┘                │                │
│    │                           │                                                              │                │
│    │                           ▼                                                              │                │
│    │               ┌─────────────────────────┐                                               │                │
│    │               │   Amazon QuickSight     │                                               │                │
│    │               │   - Revenue Dashboard   │                                               │                │
│    │               │   - Governance KPIs     │                                               │                │
│    │               │   - Data Quality Trends │                                               │                │
│    │               └─────────────────────────┘                                               │                │
│    └───────────────────────────────────────────────────────────────────────────────────────────┘                │
│                                                                                                                 │
│    ┌───────────────────────────────────────────────────────────────────────────────────────────┐                │
│    │                              MONITORING & GOVERNANCE                                      │                │
│    │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │                │
│    │  │ CloudWatch       │  │ CloudTrail       │  │ Custom Metrics   │  │ SNS Alerts       │  │                │
│    │  │ - Dashboard      │  │ - API Audit      │  │ - Governance KPIs│  │ - Steward Notif  │  │                │
│    │  │ - Alarms         │  │ - Security       │  │ - DQ Scores      │  │ - Failure Alerts │  │                │
│    │  │ - X-Ray Tracing  │  │ - Compliance     │  │ - Freshness      │  │                  │  │                │
│    │  └──────────────────┘  └──────────────────┘  └──────────────────┘  └──────────────────┘  │                │
│    └───────────────────────────────────────────────────────────────────────────────────────────┘                │
│                                                                                                                 │
│    ┌───────────────────────────────────────────────────────────────────────────────────────────┐                │
│    │                              CI/CD PIPELINE                                               │                │
│    │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │                │
│    │  │ Git Repository   │  │ Terraform        │  │ CloudFormation   │  │ Deployment       │  │                │
│    │  │ - SQL Scripts    │→→│ - Infrastructure │→→│ - Stack Deploy   │→→│ - Lambda         │  │                │
│    │  │ - Python Code    │  │ - Modules        │  │ - Change Sets    │  │ - Glue Jobs      │  │                │
│    │  │ - Config Files   │  │ - Environments   │  │ - Rollback       │  │ - Step Functions │  │                │
│    │  └──────────────────┘  └──────────────────┘  └──────────────────┘  └──────────────────┘  │                │
│    └───────────────────────────────────────────────────────────────────────────────────────────┘                │
│                                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Ingestion Layer (Batch via S3)

### Overview
Data arrives as batch files (Parquet, CSV) into the S3 landing zone.

### Implementation Details
| Component | Description |
|-----------|-------------|
| **Source Files** | NYC Yellow Taxi trip records (Parquet), Taxi Zone Lookup (CSV) |
| **Landing Zone** | `s3://arjun-s3-776312084600/raw/` |
| **File Formats** | Parquet (trips), CSV (reference data) |
| **Ingestion Pattern** | Batch upload via AWS CLI, boto3, or scheduled jobs |

### Key Scripts
- `data_ops.py` — S3 upload/download operations
- `s3_infra.py` — Bucket creation and lifecycle configuration

---

## 2. Storage Layer — S3 Data Lake with Zones

### Zone Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          S3 DATA LAKE ZONES                                 │
├──────────────┬──────────────┬──────────────┬──────────────┬───────────────┤
│    raw/      │  validated/  │   curated/   │   master/    │   archive/    │
│  (Landing)   │  (QA Gate)   │  (Enriched)  │  (Golden)    │   (WORM)      │
├──────────────┼──────────────┼──────────────┼──────────────┼───────────────┤
│ • Immutable  │ • Quality    │ • Business   │ • Golden     │ • Compliance  │
│ • Original   │   checks     │   rules      │   records    │   retention   │
│ • Source of  │   passed     │   applied    │ • Strict     │ • Immutable   │
│   truth      │ • Schema     │ • MDM        │   governance │ • 7+ years    │
│ • Open       │   validated  │   enriched   │ • Steward    │ • Legal hold  │
│   ingestion  │ • Auto-gated │ • Analytics  │   approval   │   enabled     │
│              │              │   ready      │   required   │               │
└──────────────┴──────────────┴──────────────┴──────────────┴───────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │         lineage/              │
                    │   (Audit Trail + DQ Results)  │
                    │   • Data provenance tracking  │
                    │   • Transformation history    │
                    │   • Governance audit logs     │
                    └───────────────────────────────┘
```

### Bucket Lifecycle Configuration
| Zone | Retention | Transition | Access Control |
|------|-----------|------------|----------------|
| raw/ | 90 days hot | → Glacier after 90d | Open ingestion |
| validated/ | 30 days hot | Archive after 30d | Auto-gated |
| curated/ | 1 year | Standard-IA after 60d | Governed transforms |
| master/ | Permanent | None | Strict access control |
| archive/ | 7+ years | Deep Archive | Legal hold, WORM |

---

## 3. Processing Layer — Spark/Glue/Lambda with Delta Lake

### AWS Glue ETL

```python
# Key transformation logic (day7_glue_taxi_curated.py)
# 1. Quality gates (validated zone rules)
trips_q = trips.filter(col("tpep_pickup_datetime").isNotNull()) \
               .filter(col("tpep_dropoff_datetime").isNotNull()) \
               .filter(col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime")) \
               .filter(col("trip_distance") >= 0) \
               .filter(col("total_amount") >= 0)

# 2. Add partitions
trips_q = trips_q.withColumn("year", year(col("tpep_pickup_datetime"))) \
                 .withColumn("month", month(col("tpep_pickup_datetime")))

# 3. Enrich with zone master data
trips_q = trips_q.join(pu_zones, "PULocationID", "left")
                 .join(do_zones, "DOLocationID", "left")
```

### AWS Lambda Functions
| Function | Purpose |
|----------|---------|
| `day11_audit_logger` | Records pipeline execution to DynamoDB |
| `day11_dq_gate` | Executes Glue Data Quality rulesets |
| `day11_master_freshness` | Validates MDM data freshness before ETL |
| `day11_notify_steward` | Sends SNS alerts on failures |

### Delta Lake Features
- **Time Travel**: Query historical data versions
- **ACID Transactions**: Consistent reads during writes
- **Rollback/Restore**: `day15_delta_rollback_restore.py`

---

## 4. Transformation Layer — SQL Scripts with Version Control

### SQL Script Organization
```
sql/
├── 00_admin/           # Admin DDL
├── 10_transforms/      # Core transformations
├── 20_views/           # Analytical views
├── 30_quality/         # DQ test queries
├── 40_tests/           # Unit tests for SQL
├── 50_scd/             # SCD Type 2 logic
├── 51_versioning/      # Version control metadata
├── athena/             # Athena-specific SQL
└── redshift/           # Redshift star schema
```

### Script Header Standard (Version Control)
```sql
-- ============================================================
-- Script:      day4_mdm_schema.sql
-- Author:      data_engineer
-- Created:     2026-01-15
-- Modified:    2026-01-20
-- Version:     1.2.0
-- Domain:      MDM
-- Description: Master data dimension tables with audit columns
-- ============================================================
```

### Key SQL Artifacts
| Script | Purpose |
|--------|---------|
| `day4_mdm_schema.sql` | MDM dimension tables with versioning |
| `510_proc_apply_zone_scd2.sql` | SCD Type 2 stored procedure |
| `day16_star_schema.sql` | Redshift dimensional model |
| `day17_dashboard_kpis_view.sql` | Governance KPI views |

---

## 5. Serving Layer — Redshift + Athena

### Redshift Star Schema

```
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS STAR SCHEMA                        │
│                                                                 │
│                      ┌──────────────────┐                       │
│                      │   dim_date       │                       │
│                      │   ─────────────  │                       │
│                      │ date_sk (PK)     │                       │
│                      │ full_date        │                       │
│                      │ year, month, day │                       │
│                      └────────┬─────────┘                       │
│                               │                                 │
│  ┌──────────────────┐   ┌─────┴───────────────┐   ┌───────────┐│
│  │   dim_zone       │   │ fact_yellow_trip    │   │ dim_vendor││
│  │   ───────────    │   │ ─────────────────── │   │ ────────  ││
│  │ zone_sk (PK)     │◄──┤ trip_id (PK)        ├──►│vendor_sk  ││
│  │ location_id      │   │ pickup_ts           │   │vendor_id  ││
│  │ borough          │   │ dropoff_ts          │   │vendor_name││
│  │ zone             │   │ date_sk (FK)        │   │lifecycle  ││
│  │ service_zone     │   │ vendor_sk (FK)      │   │ _state    ││
│  │ mdm_version      │   │ pu_zone_sk (FK)     │   └───────────┘│
│  │ mdm_approved_by  │   │ do_zone_sk (FK)     │                │
│  └──────────────────┘   │ passenger_count     │                │
│                         │ trip_distance       │                │
│                         │ fare_amount         │                │
│                         │ total_amount        │                │
│                         └─────────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
```

### Redshift Configuration
- **Cluster**: `oobt-redshift` (ra3.xlplus, single-node demo)
- **Distribution**: AUTO for fact, ALL for dimensions
- **Sort Keys**: `pickup_ts` on fact for time-series queries
- **IAM Role**: S3 COPY access for data loading

### Athena Configuration
- **Workgroup**: `oubt-athena-wg`
- **Catalog**: AWS Glue Data Catalog
- **Use Cases**: Ad-hoc queries, federated queries, certified views

---

## 6. Orchestration — AWS Step Functions

### Pipeline State Machine

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      GOVERNED PIPELINE STATE MACHINE                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐   ┌──────────────────┐   ┌────────────────┐   ┌────────────┐ │
│  │InitAudit │──▶│MasterFreshness   │──▶│   RunGlueETL   │──▶│ RunDQGate  │ │
│  │(Lambda)  │   │Check (Lambda)    │   │   (Glue Job)   │   │ (Lambda)   │ │
│  └──────────┘   └──────────────────┘   └────────────────┘   └────────────┘ │
│       │                  │                     │                    │       │
│       │                  ▼                     │                    │       │
│       │         ┌────────────────┐             │                    │       │
│       │         │ IsMasterFresh? │             │                    │       │
│       │         │   (Choice)     │             │                    │       │
│       │         └────────────────┘             │                    │       │
│       │          │           │                 │                    │       │
│       │       Yes│           │No               │                    │       │
│       │          │           ▼                 │                    │       │
│       │          │    ┌──────────────┐         │                    │       │
│       │          │    │NotifyMaster  │         │                    │       │
│       │          │    │Stale (SNS)   │────────▶│                    │       │
│       │          │    └──────────────┘         │                    │       │
│       │          │           │                 │                    │       │
│       │          └───────────┴─────────────────┘                    │       │
│       │                                                             │       │
│       │    ┌────────────────────────────────────────────────────────┘       │
│       │    │                                                                │
│       │    ▼                                                                │
│       │  ┌─────────────┐   ┌─────────────────┐   ┌─────────────────────┐   │
│       │  │  LogDQ      │──▶│FinalizeSuccess  │──▶│      Done           │   │
│       │  │  (Lambda)   │   │   (Lambda)      │   │    (Succeed)        │   │
│       │  └─────────────┘   └─────────────────┘   └─────────────────────┘   │
│       │                                                                     │
│       │    ┌─────────────────────────────────────────────────────────┐     │
│       └───▶│              ERROR HANDLING BRANCH                      │     │
│            │  NotifyFailure ──▶ FinalizeFail ──▶ FailState           │     │
│            └─────────────────────────────────────────────────────────┘     │
│                                                                             │
│  AUDIT: All steps logged to DynamoDB (pipeline_audit_runs)                 │
│  ALERTS: SNS notifications to mdm-steward-alerts topic                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step Functions Features
- **Retry Logic**: 3 retries with exponential backoff
- **Error Handling**: Catch all errors, notify stewards
- **Audit Trail**: Every step logged to DynamoDB
- **State Persistence**: Execution history for debugging

---

## 7. Master Data Management (MDM) Layer

### MDM Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      MASTER DATA MANAGEMENT HUB                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        DOMAIN OWNERSHIP                              │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │   │
│  │  │  Location   │  │   Vendor    │  │  RateCode   │  │ PaymentType │ │   │
│  │  │  (Zones)    │  │             │  │             │  │             │ │   │
│  │  ├─────────────┤  ├─────────────┤  ├─────────────┤  ├─────────────┤ │   │
│  │  │Owner: VP Ops│  │Owner:VPFin  │  │Owner:Comply │  │Owner: VP Fin│ │   │
│  │  │Steward: Ops │  │Steward: Fin │  │Steward:Compl│  │Steward: Fin │ │   │
│  │  │Custodian: DE│  │Custodian: DE│  │Custodian: DE│  │Custodian: DE│ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    GOLDEN RECORD MANAGEMENT                          │   │
│  │                                                                      │   │
│  │  ┌────────────────────────┐  ┌────────────────────────────────────┐ │   │
│  │  │   dim_zone_scd2        │  │   Match/Merge Engine               │ │   │
│  │  │   (SCD Type 2)         │  │   ─────────────────────            │ │   │
│  │  │   ─────────────        │  │   Thresholds:                      │ │   │
│  │  │   • zone_sk (PK)       │  │     Auto-Merge: ≥ 0.95 confidence  │ │   │
│  │  │   • location_id (NK)   │  │     Steward Review: 0.80 - 0.95    │ │   │
│  │  │   • version_number     │  │                                    │ │   │
│  │  │   • effective_from     │  │   Matching Methods:                │ │   │
│  │  │   • effective_to       │  │     • Jaro-Winkler similarity      │ │   │
│  │  │   • is_current         │  │     • Levenshtein ratio            │ │   │
│  │  │   • created_by         │  │     • Exact match (IDs)            │ │   │
│  │  │   • change_reason      │  │                                    │ │   │
│  │  │   • change_batch_id    │  │   Survivorship:                    │ │   │
│  │  │                        │  │     • Prefer active record         │ │   │
│  │  └────────────────────────┘  │     • Longest non-null value       │ │   │
│  │                              └────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    LIFECYCLE MANAGEMENT                              │   │
│  │                                                                      │   │
│  │    PROPOSED ───▶ ACTIVE ───▶ DEPRECATED ───▶ RETIRED                │   │
│  │        │            │             │              │                   │   │
│  │        │            │             │              │                   │   │
│  │    Steward      Golden        Soft            Hard                   │   │
│  │    Approval     Record        Delete          Delete                 │   │
│  │    Required     Published     (Cascade)       (Archive)              │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### SCD Type 2 Implementation

```sql
-- Stored procedure: apply_zone_scd2_from_stage
-- Key logic:
IF v_changed THEN
  -- Expire current row
  UPDATE mdm.dim_zone_scd2
  SET effective_to = v_now, is_current = FALSE
  WHERE zone_sk = v_current.zone_sk;

  -- Insert new version
  INSERT INTO mdm.dim_zone_scd2(
    location_id, borough, zone, service_zone,
    version_number, effective_from, is_current,
    created_by, change_reason, change_batch_id
  ) VALUES (...);
  
  -- Audit trail
  INSERT INTO mdm.master_version_audit(...);
END IF;
```

### Match/Merge Rules (vendor_match_rules.yaml)
```yaml
thresholds:
  auto_merge: 0.95      # Automatic merge
  steward_review: 0.80  # Human review required
fields:
  - name: vendor_name
    weight: 0.85
    method: "string_similarity"
  - name: vendor_id
    weight: 0.15
    method: "exact"
survivorship:
  auto_merge_strategy: "prefer_active_record"
  field_rules:
    vendor_name: "longest_non_null"
```

---

## 8. Monitoring Layer — CloudWatch Dashboards

### Observability Dashboard

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    OUBT OBSERVABILITY DASHBOARD                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  Domain: NYC_Taxi    Owner: Operations Team    Certified: Yes              │
├───────────────────────────────────┬─────────────────────────────────────────┤
│   STEP FUNCTIONS EXECUTIONS       │   GOVERNANCE KPIs (Custom Metrics)     │
│   ──────────────────────────      │   ────────────────────────────────     │
│   ┌──────────────────────────┐    │   ┌──────────────────────────────┐    │
│   │ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ │    │   │                              │    │
│   │ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ │    │   │ Pipeline Success Rate: 98.5% │    │
│   │ ░░░░░░░░░░░░░░░░░░░░░░░ │    │   │ Master Freshness: 2.3 hours  │    │
│   │ ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ │    │   │ Orphan Rate PU: 0.02%        │    │
│   └──────────────────────────┘    │   │ Orphan Rate DO: 0.01%        │    │
│   ■ Started  ■ Succeeded          │   └──────────────────────────────┘    │
│   ■ Failed   ■ TimedOut           │                                        │
├───────────────────────────────────┴─────────────────────────────────────────┤
│   DATA QUALITY SCORECARD                                                    │
│   ────────────────────────                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ Dimension    │ Metric                  │ Threshold     │ Status    │  │
│   ├──────────────┼─────────────────────────┼───────────────┼───────────┤  │
│   │ Completeness │ Non-null timestamps     │ ≥ 99.9%       │ ✅ PASS   │  │
│   │ Validity     │ Distances ≥ 0           │ 100%          │ ✅ PASS   │  │
│   │ Consistency  │ Known zones (no orphan) │ ≤ 0.1% orphan │ ✅ PASS   │  │
│   │ Timeliness   │ Curated within 24h      │ < 24 hours    │ ✅ PASS   │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### CloudWatch Alarms
| Alarm | Condition | Action |
|-------|-----------|--------|
| Pipeline Failure | ExecutionsFailed > 0 | SNS → Steward email |
| DQ Gate Failure | DQ score < threshold | Block pipeline, notify |
| Master Stale | Freshness > 24h | Alert for review |
| Lambda Errors | Error count > 5/5min | PagerDuty integration |

---

## 9. CI/CD Pipeline — CloudFormation

### Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CI/CD DEPLOYMENT PIPELINE                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
│  │   Git    │──▶│ Terraform│──▶│CloudForm │──▶│  Deploy  │──▶│ Validate │ │
│  │  Push    │   │  Plan    │   │  Stack   │   │ Resources│   │  & Test  │ │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
│       │              │              │              │              │        │
│       ▼              ▼              ▼              ▼              ▼        │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
│  │ SQL      │   │ Modules: │   │ Change   │   │ Lambda   │   │ DQ Tests │ │
│  │ Scripts  │   │ • S3     │   │ Sets     │   │ Glue     │   │ SQL      │ │
│  │ Python   │   │ • RDS    │   │ Review   │   │ Step Fn  │   │ Tests    │ │
│  │ YAML     │   │ • Lambda │   │ Rollback │   │ Redshift │   │          │ │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Terraform Modules
```
infra/terraform/
├── envs/
│   ├── dev.tf
│   ├── prod.tf
│   └── variables.tfvars
└── modules/
    ├── lambda/
    │   ├── main.tf
    │   └── variables.tf
    ├── rds/
    │   ├── main.tf
    │   ├── outputs.tf
    │   └── variables.tf
    └── s3/
        ├── main.tf
        ├── outputs.tf
        └── variables.tf
```

### Infrastructure Components (Terraform)
| Resource | Configuration |
|----------|---------------|
| S3 Buckets | Versioning, lifecycle, encryption |
| RDS PostgreSQL | MDM hub, private subnet |
| Redshift | ra3.xlplus, S3 COPY role |
| Lambda Functions | Python 3.11, VPC access |
| Step Functions | Pipeline orchestration |
| IAM Roles | Least privilege policies |

---

## 10. Live Demo Guide (20 Minutes)

### Demo 1: Master Data Operations (5 min)

```bash
# 1. Show current MDM state
psql -c "SELECT * FROM mdm.dim_zone WHERE location_id = 1;"

# 2. Stage an update
INSERT INTO mdm.stg_zone_updates (location_id, borough, zone, service_zone, change_reason)
VALUES (1, 'Manhattan', 'Newark Airport Updated', 'EWR', 'Demo update');

# 3. Apply SCD2 procedure
CALL mdm.apply_zone_scd2_from_stage('demo_user', 'Live demo', 'Manual');

# 4. Show version history
SELECT zone_sk, location_id, zone, version_number, is_current, effective_from
FROM mdm.dim_zone_scd2 WHERE location_id = 1 ORDER BY version_number;
```

### Demo 2: Data Quality Monitoring (4 min)

```bash
# 1. Show Great Expectations suite
cat governance/great_expectations/yellow_trips_suite.json

# 2. Run DQ evaluation
python scripts/day8_run_dq_evaluation.py

# 3. Show results in CloudWatch
aws cloudwatch get-metric-data --metric-name "DQRulesPassed" ...

# 4. Show quality scorecard
cat docs/quality_scorecard.md
```

### Demo 3: Analytics Dashboard (3 min)

```sql
-- Connect to Redshift
-- 1. Show star schema
SELECT * FROM analytics.fact_yellow_trip LIMIT 10;
SELECT * FROM analytics.dim_zone LIMIT 10;

-- 2. Run analytics query
SELECT 
  dz.borough,
  COUNT(*) as trips,
  SUM(f.total_amount) as revenue
FROM analytics.fact_yellow_trip f
JOIN analytics.dim_zone dz ON f.pu_zone_sk = dz.zone_sk
GROUP BY dz.borough
ORDER BY revenue DESC;
```

### Demo 4: Batch ETL with Step Functions (4 min)

```bash
# 1. Start pipeline execution
python scripts/day11_start_execution_manual.py

# 2. Monitor in AWS Console (Step Functions)
# Show state machine graph, execution progress

# 3. Check DynamoDB audit table
aws dynamodb scan --table-name pipeline_audit_runs

# 4. Show SNS notification (if failure)
```

### Demo 5: CI/CD Deployment (4 min)

```bash
# 1. Show Terraform plan
cd infra/terraform
terraform plan

# 2. Show module structure
ls -la modules/

# 3. Deploy change (Lambda update)
terraform apply -auto-approve

# 4. Verify deployment
aws lambda get-function --function-name day11_audit_logger
```

---

## Summary Table

| Layer | Technology | Key Features |
|-------|------------|--------------|
| **Ingestion** | S3, boto3 | Batch upload, Parquet/CSV |
| **Storage** | S3 Data Lake | 5 zones (raw → master), lifecycle |
| **Processing** | Glue, Lambda, Spark | ETL, DQ gates, Delta Lake |
| **Transformation** | SQL (versioned) | SCD2, quality tests |
| **Serving** | Redshift, Athena | Star schema, certified views |
| **Orchestration** | Step Functions | Governed pipeline, retry/catch |
| **MDM** | PostgreSQL | Dedup, match/merge, SCD2 |
| **Monitoring** | CloudWatch | Dashboards, alarms, X-Ray |
| **CI/CD** | Terraform, CloudFormation | IaC, modular deployment |

---

## Appendix: Key File References

| Category | File Path |
|----------|-----------|
| Architecture Doc | `docs/day20_architecture.md` |
| MDM Architecture | `docs/day19_mdm_architecture.md` |
| Lake Zone Design | `docs/day6_datalake_architecture.svg` |
| Governance Charter | `docs/governance_charter.md` |
| Step Functions | `stepfunctions/day11_pipeline.asl.json` |
| Glue ETL | `glue_jobs/day7_glue_taxi_curated.py` |
| MDM Schema | `sql/day4_mdm_schema.sql` |
| SCD2 Procedure | `sql/50_scd/510_proc_apply_zone_scd2.sql` |
| Redshift Schema | `sql/redshift/day16_star_schema.sql` |
| Match Rules | `governance/mdm/vendor_match_rules.yaml` |
| DQ Suite | `governance/great_expectations/yellow_trips_suite.json` |
| Dashboard | `dashboards/day18/oobt_observability_dashboard.json` |
| Terraform | `infra/terraform/modules/` |

---

*Document Version: 1.0.0*  
*Last Updated: 2026-01-21*  
*Author: Data Engineering Team*

