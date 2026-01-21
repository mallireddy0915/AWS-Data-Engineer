# Day 4 — MDM Governance Model (Simulated)

## Roles
- Data Owner (business accountable): VP Operations (Location), VP Finance (Payment/Rate), VP Vendor Mgmt (Vendor)
- Data Steward (day-to-day): Ops Analyst / Finance Analyst
- Data Custodian (technical): Data Engineer (you)
- Data Consumer: Analysts, dashboards, ML models

## Operating Model (Golden Record)
- Golden record creation:
  - Location: ingestion from TLC + steward review for conflicts
- Change approval:
  - Steward approves routine changes; Owner approves structural changes (LocationID retirements/merges)
- Escalation:
  - Conflicts → Steward → Owner within 48 hours
- Exceptions:
  - Temporary override allowed with `exception_reason` + expiry date

## Data Quality Dimensions + thresholds
- Accuracy: 99% (manual sampling)
- Completeness: 99.9% mandatory fields
- Consistency: 99.5% across sources
- Timeliness: updates applied within 7 days of TLC release
- Validity: 100% allowed values
- Uniqueness: 100% primary keys
