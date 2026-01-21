# Day 8 — Data Quality Scorecard (Governance Context)

| Quality Dimension | Business Metric | Owner | Threshold | Action on Failure |
|---|---|---|---|---|
| Completeness | Can analytics trust pickup/dropoff timestamps? | Data Steward (Ops) | >= 99.9% non-null pickup/dropoff | Block promotion to validated; create ticket |
| Validity | Are distances/charges valid for billing/finance dashboards? | Data Owner (Finance) | 100% trip_distance >= 0 and total_amount >= 0 | Quarantine bad rows; alert owner |
| Consistency | Do trips match known zones (no orphan zones)? | Data Steward (Ops) | <= 0.1% orphan PU/DO | Manual review; steward approval required |
| Timeliness | Are monthly datasets available for reporting on time? | Data Custodian (DE) | Curated dataset published within 24h of ingest | On-call alert; rerun pipeline |
