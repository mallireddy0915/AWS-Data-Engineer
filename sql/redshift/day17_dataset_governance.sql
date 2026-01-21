CREATE SCHEMA IF NOT EXISTS governance;

CREATE TABLE IF NOT EXISTS governance.dataset_governance (
  dataset_name        varchar(200) PRIMARY KEY,
  owner_team          varchar(200),
  steward             varchar(200),
  classification      varchar(50),
  certified_flag      boolean,
  completeness_pct    double precision,
  last_refresh_ts     timestamp,
  notes               varchar(500)
);

-- Example rows (edit as you like)
DELETE FROM governance.dataset_governance WHERE dataset_name IN ('fact_yellow_trip','dim_zone','dim_vendor');

INSERT INTO governance.dataset_governance VALUES
('fact_yellow_trip', 'Operations Team', 'Data Steward - Ops', 'Internal', true, 0.992, GETDATE(), 'Certified for analytics'),
('dim_zone',         'Operations Team', 'MDM Steward - Zones', 'Internal', true, 1.000, GETDATE(), 'Derived from MDM golden records'),
('dim_vendor',       'Operations Team', 'MDM Steward - Vendors', 'Internal', true, 0.985, GETDATE(), 'Dedup workflow active');
