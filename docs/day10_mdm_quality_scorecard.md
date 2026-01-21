# Day 10 — MDM Quality Scorecard (Business Impact)

| Dimension | Business Impact | Metric | Owner | Threshold | Action on Failure |
|---|---|---|---|---|---|
| Uniqueness | Avoid double-counting vendor KPIs | % duplicate vendors | Data Steward (Vendor Mgmt) | <= 0.5% | Steward review queue |
| Validity | Correct vendor attribution | % invalid vendor_id | Data Owner (Vendor Mgmt VP) | 0% | Block curated publish |
| Consistency | Consistent vendor naming | % name conflicts | Steward | <= 1% | Steward arbitration |
| Timeliness | Updated master within SLA | Update lag hours | Custodian (DE) | <= 24h | Alert + rerun |
| Auditability | Compliance & traceability | CDC log present | Custodian | 100% | Pipeline fails |
