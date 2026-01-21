# Multi-domain MDM Architecture (Central governance, federated stewardship)

## Domains + Ownership (example)
- Location (Zones): Owner=Operations VP, Steward=Ops Analyst, Custodian=Data Engineer
- Vendor: Owner=Finance VP, Steward=Finance Analyst, Custodian=Data Engineer
- RateCode: Owner=Policy/Compliance, Steward=Compliance Analyst, Custodian=Data Engineer

## Pattern: Master Data Hub (publish golden records)
```mermaid
flowchart LR
  subgraph Sources
    S1[Raw Sources: NYC taxi files]
    S2[Reference: taxi_zone_lookup]
  end

  subgraph MDM["MDM Hub (RDS) - Golden Records"]
    Z[dim_zone_scd2 (approved, versioned)]
    V[dim_vendor (dedup + lifecycle)]
    R[dim_rate_code]
    AQ[Audit + approvals]
  end

  subgraph Publish["Publish/Subscribe"]
    CDC[CDC/Event: change approved]
    PUB[S3 Master Zone snapshots]
  end

  subgraph Consumers
    RS[Redshift dims derived from golden]
    ATH[Athena certified views]
    QS[QuickSight dashboards]
  end

  S1 --> ETL[Glue/ETL + DQ gates]
  S2 --> ETL
  ETL --> MDM
  MDM --> CDC --> PUB --> RS
  PUB --> ATH
  RS --> QS

  Governance[Central Governance Council] --> MDM
  Stewards[Federated Stewards per domain] --> MDM
