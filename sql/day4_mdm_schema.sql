CREATE SCHEMA IF NOT EXISTS mdm;

-- Common audit columns pattern:
-- created_at, created_by, updated_at, updated_by, approved_at, approved_by, version

DROP TABLE IF EXISTS mdm.dim_zone;
CREATE TABLE mdm.dim_zone (
  zone_key BIGSERIAL PRIMARY KEY,
  location_id INT NOT NULL UNIQUE,
  borough TEXT NOT NULL,
  zone TEXT NOT NULL,
  service_zone TEXT NOT NULL,
  source_system TEXT NOT NULL DEFAULT 'TLC',
  source_priority INT NOT NULL DEFAULT 1,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,

  -- audit/version
  version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT NOT NULL DEFAULT 'system',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT NOT NULL DEFAULT 'system',
  approved_at TIMESTAMPTZ,
  approved_by TEXT
);

DROP TABLE IF EXISTS mdm.dim_vendor;
CREATE TABLE mdm.dim_vendor (
  vendor_key BIGSERIAL PRIMARY KEY,
  vendor_id INT NOT NULL UNIQUE,
  vendor_name TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,

  version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT NOT NULL DEFAULT 'system',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT NOT NULL DEFAULT 'system',
  approved_at TIMESTAMPTZ,
  approved_by TEXT
);

DROP TABLE IF EXISTS mdm.dim_rate_code;
CREATE TABLE mdm.dim_rate_code (
  rate_code_key BIGSERIAL PRIMARY KEY,
  ratecode_id INT NOT NULL UNIQUE,
  ratecode_desc TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,

  version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT NOT NULL DEFAULT 'system',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT NOT NULL DEFAULT 'system',
  approved_at TIMESTAMPTZ,
  approved_by TEXT
);

DROP TABLE IF EXISTS mdm.dim_payment_type;
CREATE TABLE mdm.dim_payment_type (
  payment_type_key BIGSERIAL PRIMARY KEY,
  payment_type_id INT NOT NULL UNIQUE,
  payment_type_desc TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,

  version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT NOT NULL DEFAULT 'system',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT NOT NULL DEFAULT 'system',
  approved_at TIMESTAMPTZ,
  approved_by TEXT
);
