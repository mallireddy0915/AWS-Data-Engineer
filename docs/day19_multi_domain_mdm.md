# Day 19 — Multi-domain MDM Architecture (Central governance + federated stewardship)

## Domains
- Location/Zones (Owner: Operations VP, Steward: Ops Analyst)
- Vendor (Owner: Finance VP, Steward: Finance Analyst)
- Rate Codes (Owner: Compliance Lead, Steward: Compliance Analyst)

## Roles (RACI)
- Owner: accountable for domain policy + definitions
- Steward: approves changes, monitors quality + exceptions
- Custodian (you): implements pipelines, controls access, enforces quality gates
- Consumers: analysts, BI, ML models

## Hub pattern (publish golden records)
- Golden records live in RDS (versioned, SCD2)
- “Master zone” S3 contains approved snapshots for consumption
- Consumers derive analytics dims from master snapshots

```mermaid
flowchart LR
  SRC[Sources: raw NYC taxi] --> ETL[Glue ETL + DQ gates]
  REF[Reference: zone lookup] --> MDM[MDM Hub: RDS master tables (SCD2 + approvals)]
  ETL --> MDM

  MDM -->|Approved change| SNAP[S3 Master Snapshots]
  SNAP --> RS[Redshift dims (conformed)]
  SNAP --> ATH[Athena certified views]
  RS --> QS[QuickSight dashboards w/ governance labels]

  GOV[Central Governance Council] --> MDM
  STW[Federated Stewards] --> MDM
