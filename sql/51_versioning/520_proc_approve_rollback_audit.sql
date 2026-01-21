-- Approve the CURRENT version for a given LocationID
CREATE OR REPLACE PROCEDURE mdm.approve_version(
  p_location_id INT,
  p_approver TEXT,
  p_reason TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_row mdm.dim_zone_scd2%ROWTYPE;
BEGIN
  SELECT * INTO v_row
  FROM mdm.dim_zone_scd2
  WHERE location_id = p_location_id AND is_current = TRUE
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No current version found for location_id=%', p_location_id;
  END IF;

  UPDATE mdm.dim_zone_scd2
  SET approved_by = p_approver,
      approved_at = NOW(),
      approval_status = 'APPROVED',
      change_reason = COALESCE(change_reason, p_reason)
  WHERE zone_sk = v_row.zone_sk;

  INSERT INTO mdm.master_version_audit(domain, natural_key, action, action_by, details)
  VALUES ('zone', p_location_id::text, 'APPROVE', p_approver,
    jsonb_build_object('approved_version', v_row.version_number, 'reason', p_reason)
  );
END;
$$;

-- Audit version history in a date range (point-in-time evidence)
CREATE OR REPLACE FUNCTION mdm.audit_version_history(
  p_location_id INT,
  p_from TIMESTAMPTZ,
  p_to TIMESTAMPTZ
)
RETURNS TABLE (
  location_id INT,
  version_number INT,
  effective_from TIMESTAMPTZ,
  effective_to TIMESTAMPTZ,
  is_current BOOLEAN,
  approval_status TEXT,
  approved_by TEXT,
  approved_at TIMESTAMPTZ,
  change_reason TEXT,
  change_source TEXT,
  change_batch_id TEXT,
  is_rollback BOOLEAN,
  rollback_from_version INT
)
LANGUAGE sql
AS $$
  SELECT
    location_id, version_number, effective_from, effective_to, is_current,
    approval_status, approved_by, approved_at,
    change_reason, change_source, change_batch_id,
    is_rollback, rollback_from_version
  FROM mdm.dim_zone_scd2
  WHERE location_id = p_location_id
    AND effective_from <= p_to
    AND (effective_to IS NULL OR effective_to >= p_from)
  ORDER BY version_number;
$$;

-- Rollback: make "truth now" match a target historical version, without deleting history.
-- Approach: expire current, then INSERT a NEW version that copies target attributes.
CREATE OR REPLACE PROCEDURE mdm.rollback_version(
  p_location_id INT,
  p_target_version INT,
  p_actor TEXT,
  p_reason TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_now TIMESTAMPTZ := NOW();
  v_current mdm.dim_zone_scd2%ROWTYPE;
  v_target mdm.dim_zone_scd2%ROWTYPE;
  v_new_version INT;
BEGIN
  SELECT * INTO v_current
  FROM mdm.dim_zone_scd2
  WHERE location_id = p_location_id AND is_current = TRUE
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No current version found for location_id=%', p_location_id;
  END IF;

  SELECT * INTO v_target
  FROM mdm.dim_zone_scd2
  WHERE location_id = p_location_id AND version_number = p_target_version
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Target version % not found for location_id=%', p_target_version, p_location_id;
  END IF;

  -- expire current
  UPDATE mdm.dim_zone_scd2
  SET effective_to = v_now,
      is_current = FALSE
  WHERE zone_sk = v_current.zone_sk;

  v_new_version := v_current.version_number + 1;

  -- insert new "rollback-applied" current record
  INSERT INTO mdm.dim_zone_scd2(
    location_id, borough, zone, service_zone,
    version_number, effective_from, effective_to, is_current,
    created_by, approved_by, approved_at, approval_status,
    change_reason, change_source, change_batch_id,
    is_rollback, rollback_from_version
  )
  VALUES (
    p_location_id, v_target.borough, v_target.zone, v_target.service_zone,
    v_new_version, v_now, NULL, TRUE,
    p_actor, p_actor, v_now, 'APPROVED',
    p_reason, 'ROLLBACK', NULL,
    TRUE, p_target_version
  );

  INSERT INTO mdm.master_version_audit(domain, natural_key, action, action_by, details)
  VALUES ('zone', p_location_id::text, 'ROLLBACK', p_actor,
    jsonb_build_object(
      'from_version', v_current.version_number,
      'to_copied_version', p_target_version,
      'new_version', v_new_version,
      'reason', p_reason
    )
  );
END;
$$;