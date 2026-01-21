# Day 7: Glue Catalog as a Governance Tool

## Overview

AWS Glue Data Catalog serves as a centralized metadata repository that enables data discovery, governance, and lineage tracking across our NYC Taxi data lake.

---

## Tables Crawled

| Database       | Table            | Source Location                                      | Format  |
|----------------|------------------|------------------------------------------------------|---------|
| `nyc_taxi_db`  | `curated_yellow` | `s3://arjun-s3-776312084600/curated/yellow/`         | Parquet |

---

## Custom Properties Added

The following governance metadata properties were added to catalog tables via `day7_update_catalog_metadata.py`:

| Property           | Value                                                        | Purpose                              |
|--------------------|--------------------------------------------------------------|--------------------------------------|
| `data_owner`       | `DataEngineering`                                            | Identifies accountable team          |
| `data_domain`      | `NYC_Taxi`                                                   | Business domain classification       |
| `classification`   | `Internal`                                                   | Data sensitivity level               |
| `quality_score`    | `validated`                                                  | Indicates data passed quality gates  |
| `lineage_s3_uri`   | `s3://arjun-s3-776312084600/lineage/glue/day7_run.json`      | Links to lineage artifact            |
| `last_updated`     | ISO 8601 timestamp                                           | Tracks freshness                     |

---

## Discovering Trusted Datasets

Users can identify "trusted" datasets in the Glue Catalog by checking:

### 1. Curated Tables with Quality Score

```sql
-- In Athena, query tables with validated quality
SELECT * FROM nyc_taxi_db.curated_yellow
WHERE year = 2025 AND month = 8;
```

Trusted tables have:
- `quality_score = validated` — passed all quality gates
- `lineage_s3_uri` — full provenance available

### 2. Classification and Ownership

Filter tables by governance properties:

```bash
aws glue get-table --database-name nyc_taxi_db --name curated_yellow \
  --query "Table.Parameters.{Owner:data_owner,Domain:data_domain,Classification:classification}"
```

### 3. Lineage Artifact

The `lineage_s3_uri` property points to a JSON file containing:
- Input sources (trips + zones)
- Quality gate results (rows in/out, rules applied)
- Transformations performed
- Timestamp of processing

```bash
aws s3 cp s3://arjun-s3-776312084600/lineage/glue/day7_run.json - | jq .
```

---

## Schema Evolution Strategy

### Crawler Configuration

The Glue Crawler is configured with:

| Setting                  | Value               | Effect                                           |
|--------------------------|---------------------|--------------------------------------------------|
| `SchemaChangePolicy`     | `UPDATE_IN_DATABASE`| New columns added automatically                  |
| `DeleteBehavior`         | `LOG`               | Removed columns logged, not deleted              |
| `RecrawlPolicy`          | `CRAWL_NEW_FOLDERS_ONLY` | Only new partitions scanned             |

### Partition Strategy

Data is partitioned by:

```
s3://bucket/curated/yellow/
  └── year=2025/
      └── month=8/
          └── *.parquet
```

| Partition Key | Type    | Purpose                        |
|---------------|---------|--------------------------------|
| `year`        | integer | Enables time-based filtering   |
| `month`       | integer | Reduces scan cost for queries  |

### Handling Schema Changes

1. **Additive changes** (new columns): Crawler adds columns with `UPDATE_IN_DATABASE`
2. **Type changes**: Require manual intervention; Glue logs incompatibilities
3. **Removed columns**: Logged but retained in schema for backward compatibility

---

## Summary

The Glue Catalog provides:

- **Discovery**: Users find tables by domain, owner, and classification
- **Trust**: `quality_score` and `lineage_s3_uri` indicate validated data
- **Governance**: Custom properties enforce accountability
- **Evolution**: `UPDATE_IN_DATABASE` handles schema growth gracefully

