CREATE SCHEMA IF NOT EXISTS mdm;

-- SCD2 dimension with surrogate key and versioning metadata
CREATE TABLE IF NOT EXISTS mdm.dim_zone_scd2 (
  zone_sk BIGSERIAL PRIMARY KEY,

  -- natural key
  location_id INT NOT NULL,

  -- attributes
  borough TEXT,
  zone TEXT,
  service_zone TEXT,

  -- versioning metadata
  version_number INT NOT NULL,
  effective_from TIMESTAMPTZ NOT NULL,
  effective_to TIMESTAMPTZ,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,

  -- governance metadata
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT NOT NULL DEFAULT 'system',
  approved_at TIMESTAMPTZ,
  approved_by TEXT,
  approval_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (approval_status IN ('PENDING','APPROVED','REJECTED')),
  change_reason TEXT,
  change_source TEXT,
  change_batch_id TEXT,

  -- rollback metadata
  is_rollback BOOLEAN NOT NULL DEFAULT FALSE,
  rollback_from_version INT
);

-- One-current-row constraint per natural key
CREATE UNIQUE INDEX IF NOT EXISTS ux_zone_current
ON mdm.dim_zone_scd2(location_id)
WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_zone_history
ON mdm.dim_zone_scd2(location_id, version_number);

CREATE INDEX IF NOT EXISTS idx_zone_effective
ON mdm.dim_zone_scd2(location_id, effective_from, effective_to);

-- Simple "current view" consumers should use
CREATE OR REPLACE VIEW mdm.vw_zone_current AS
SELECT *
FROM mdm.dim_zone_scd2
WHERE is_current = TRUE;

-- Staging table for incoming snapshots/changes (one row per LocationID)
CREATE TABLE IF NOT EXISTS mdm.stg_zone_updates (
  location_id INT NOT NULL,
  borough TEXT,
  zone TEXT,
  service_zone TEXT,
  change_reason TEXT,
  change_source TEXT,
  change_batch_id TEXT,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  loaded_by TEXT NOT NULL DEFAULT 'system',
  PRIMARY KEY (location_id)
);

-- Audit log for approvals/rollbacks (governance evidence)
CREATE TABLE IF NOT EXISTS mdm.master_version_audit (
  audit_id BIGSERIAL PRIMARY KEY,
  domain TEXT NOT NULL,               -- e.g. 'zone'
  natural_key TEXT NOT NULL,          -- e.g. location_id as text
  action TEXT NOT NULL CHECK (action IN ('UPSERT','APPROVE','ROLLBACK')),
  action_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  action_by TEXT NOT NULL DEFAULT 'system',
  details JSONB
);