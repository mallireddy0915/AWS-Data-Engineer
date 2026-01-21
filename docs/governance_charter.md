# Governance Charter (Draft)

## Purpose
Define decision rights, roles, escalation paths, and governance artifacts for NYC Taxi data products.

## Roles (RACI)
| Activity | Data Owner | Data Steward | Data Custodian (DE) | Consumers |
|---|---|---|---|---|
| Define domain rules | A | R | C | I |
| Approve golden record merges | A | R | C | I |
| Implement pipelines | I | C | R | I |
| Data quality monitoring | C | R | R | I |
| Policy changes (IAM/retention) | A | C | R | I |

## Decision Rights
- Owner: structural changes (merges/retirements, retention)
- Steward: routine updates and exception approvals
- Custodian: technical implementation + audit + lineage

## Escalation
Steward → Owner within 48 hours for conflicts.
Emergency changes require post-approval within 24 hours.

## Domains and Owners (Simulated)
- Location (Zones): VP Operations
- Vendor: VP Vendor Mgmt
- Rate Code + Payment Type: VP Finance
