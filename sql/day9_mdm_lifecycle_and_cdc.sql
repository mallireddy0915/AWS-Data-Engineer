CREATE SCHEMA IF NOT EXISTS mdm;

-- Create dim_vendor master table if not exists
DROP TABLE IF EXISTS mdm.dim_vendor CASCADE;
CREATE TABLE mdm.dim_vendor (
  vendor_id INT PRIMARY KEY,
  vendor_name TEXT NOT NULL,
  vendor_code TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Insert known NYC Taxi vendors
INSERT INTO mdm.dim_vendor (vendor_id, vendor_name, vendor_code) VALUES
  (1, 'Creative Mobile Technologies, LLC', 'CMT'),
  (2, 'VeriFone Inc.', 'VTS')
ON CONFLICT (vendor_id) DO NOTHING;

-- 2.1 Lifecycle state table (do not modify existing dim_vendor; keep lifecycle separate)
DROP TABLE IF EXISTS mdm.vendor_lifecycle;
CREATE TABLE mdm.vendor_lifecycle (
  vendor_id INT PRIMARY KEY,
  lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('PROPOSED','ACTIVE','DEPRECATED','RETIRED')),
  state_reason TEXT,
  effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  effective_to TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT NOT NULL DEFAULT 'system',
  approved_at TIMESTAMPTZ,
  approved_by TEXT
);

-- 2.2 Steward review queue for matches / dedup decisions
DROP TABLE IF EXISTS mdm.vendor_review_queue;
CREATE TABLE mdm.vendor_review_queue (
  review_id BIGSERIAL PRIMARY KEY,
  left_vendor_id INT NOT NULL,
  right_vendor_id INT NOT NULL,
  confidence NUMERIC NOT NULL,
  recommendation TEXT NOT NULL CHECK (recommendation IN ('AUTO_MERGE','STEWARD_REVIEW','MANUAL')),
  status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','APPROVED','REJECTED','MERGED')),
  rationale JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT NOT NULL DEFAULT 'system',
  reviewed_at TIMESTAMPTZ,
  reviewed_by TEXT,
  decision_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_vendor_review_status ON mdm.vendor_review_queue(status);
CREATE INDEX IF NOT EXISTS idx_vendor_review_conf ON mdm.vendor_review_queue(confidence);

-- 2.3 CDC/Audit log for master data changes (trigger-based)
DROP TABLE IF EXISTS mdm.dim_vendor_audit;
CREATE TABLE mdm.dim_vendor_audit (
  audit_id BIGSERIAL PRIMARY KEY,
  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
  changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  changed_by TEXT NOT NULL DEFAULT 'system',
  old_row JSONB,
  new_row JSONB
);

-- Trigger function (assumes mdm.dim_vendor exists from Day 4)
DROP FUNCTION IF EXISTS mdm.fn_audit_dim_vendor();
CREATE OR REPLACE FUNCTION mdm.fn_audit_dim_vendor()
RETURNS TRIGGER AS $$
BEGIN
  IF (TG_OP = 'INSERT') THEN
    INSERT INTO mdm.dim_vendor_audit(op, changed_by, old_row, new_row)
    VALUES ('INSERT', current_user, NULL, to_jsonb(NEW));
    RETURN NEW;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO mdm.dim_vendor_audit(op, changed_by, old_row, new_row)
    VALUES ('UPDATE', current_user, to_jsonb(OLD), to_jsonb(NEW));
    RETURN NEW;
  ELSIF (TG_OP = 'DELETE') THEN
    INSERT INTO mdm.dim_vendor_audit(op, changed_by, old_row, new_row)
    VALUES ('DELETE', current_user, to_jsonb(OLD), NULL);
    RETURN OLD;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_dim_vendor ON mdm.dim_vendor;
CREATE TRIGGER trg_audit_dim_vendor
AFTER INSERT OR UPDATE OR DELETE ON mdm.dim_vendor
FOR EACH ROW EXECUTE FUNCTION mdm.fn_audit_dim_vendor();
