CREATE SCHEMA IF NOT EXISTS analytics;

-- Track transformations (version control + lineage of SQL)
CREATE TABLE IF NOT EXISTS analytics.transform_registry (
  transform_name TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  purpose TEXT NOT NULL,
  version INT NOT NULL,
  sql_path TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT NOT NULL DEFAULT 'system'
);