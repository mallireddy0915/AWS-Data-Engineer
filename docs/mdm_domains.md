# Day 4 — MDM Domains (NYC Taxi)

## Domain: Location (Taxi Zone)
- Type: Master Data (Reference Master)
- Who cares: Ops, Analytics, Finance, Product
- Decisions: zone-level revenue, demand hotspots, congestion analysis
- If wrong: wrong aggregations, bad dashboards, incorrect modeling
- Source systems: TLC zone lookup (CSV)
- Update frequency: low (as TLC changes zones)
- Change triggers: TLC redefines boundaries, renames zones, changes service_zone
- Quality thresholds:
  - completeness: 99.9% (LocationID, Borough, Zone not null)
  - uniqueness: 100% LocationID unique
  - validity: 100% service_zone in allowed set

## Domain: Vendor
- Type: Master Data (Code set)
- Who cares: Compliance, Analytics
- Decisions: vendor KPIs, complaints, SLA checks
- If wrong: mis-attribution of trips/revenue
- Source: TLC codes (simulated)
- Update frequency: rare
- Quality thresholds: uniqueness 100%, validity 100%

## Domain: Rate Code
...

## Domain: Payment Type
...
