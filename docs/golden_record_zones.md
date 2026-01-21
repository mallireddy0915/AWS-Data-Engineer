# Golden Record Strategy — Taxi Zones (Survivorship Rules)

## Golden Record Key
- location_id is the master key (surrogate optional later)

## Survivorship rules (field-level)
1. location_id: must match (no survivorship; conflict = reject)
2. borough:
   - prefer value from source_priority=1 (TLC official)
   - else keep most frequent non-null
3. zone:
   - prefer TLC official
   - else longest non-null string
4. service_zone:
   - prefer TLC official
   - else allowed set validation

## Merge rules
- If duplicates found with same (borough, zone, service_zone) but different location_id:
  - DO NOT auto-merge (IDs are authoritative). Flag for steward review.

## Audit
- track created_by, updated_by, approved_by, version, effective timestamps
