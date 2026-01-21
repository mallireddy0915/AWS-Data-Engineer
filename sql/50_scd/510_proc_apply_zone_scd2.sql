
CREATE OR REPLACE PROCEDURE mdm.apply_zone_scd2_from_stage(
  p_changed_by TEXT,
  p_default_reason TEXT,
  p_default_source TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  r RECORD;
  v_current mdm.dim_zone_scd2%ROWTYPE;
  v_next_version INT;
  v_now TIMESTAMPTZ := NOW();
  v_changed BOOLEAN;
BEGIN
  FOR r IN
    SELECT * FROM mdm.stg_zone_updates
  LOOP
    -- Is there a current record?
    SELECT * INTO v_current
    FROM mdm.dim_zone_scd2
    WHERE location_id = r.location_id AND is_current = TRUE
    LIMIT 1;

    IF NOT FOUND THEN
      -- new natural key => version 1
      INSERT INTO mdm.dim_zone_scd2(
        location_id, borough, zone, service_zone,
        version_number, effective_from, effective_to, is_current,
        created_by, change_reason, change_source, change_batch_id
      )
      VALUES (
        r.location_id, r.borough, r.zone, r.service_zone,
        1, v_now, NULL, TRUE,
        COALESCE(p_changed_by,'system'),
        COALESCE(r.change_reason, p_default_reason),
        COALESCE(r.change_source, p_default_source),
        r.change_batch_id
      );

      INSERT INTO mdm.master_version_audit(domain, natural_key, action, action_by, details)
      VALUES ('zone', r.location_id::text, 'UPSERT', COALESCE(p_changed_by,'system'),
        jsonb_build_object('type','insert_new_key','version',1,'batch',r.change_batch_id)
      );

    ELSE
      -- Compare attributes (use IS DISTINCT FROM for NULL-safe compare)
      v_changed :=
        (v_current.borough IS DISTINCT FROM r.borough)
        OR (v_current.zone IS DISTINCT FROM r.zone)
        OR (v_current.service_zone IS DISTINCT FROM r.service_zone);

      IF v_changed THEN
        -- expire current row
        UPDATE mdm.dim_zone_scd2
        SET effective_to = v_now,
            is_current = FALSE
        WHERE zone_sk = v_current.zone_sk;

        -- next version number
        v_next_version := v_current.version_number + 1;

        -- insert new current version
        INSERT INTO mdm.dim_zone_scd2(
          location_id, borough, zone, service_zone,
          version_number, effective_from, effective_to, is_current,
          created_by, change_reason, change_source, change_batch_id
        )
        VALUES (
          r.location_id, r.borough, r.zone, r.service_zone,
          v_next_version, v_now, NULL, TRUE,
          COALESCE(p_changed_by,'system'),
          COALESCE(r.change_reason, p_default_reason),
          COALESCE(r.change_source, p_default_source),
          r.change_batch_id
        );

        INSERT INTO mdm.master_version_audit(domain, natural_key, action, action_by, details)
        VALUES ('zone', r.location_id::text, 'UPSERT', COALESCE(p_changed_by,'system'),
          jsonb_build_object(
            'type','attribute_change',
            'from_version', v_current.version_number,
            'to_version', v_next_version,
            'batch', r.change_batch_id
          )
        );
      END IF;
    END IF;
  END LOOP;

  -- Clear stage after apply (optional; keep if you want)
  TRUNCATE TABLE mdm.stg_zone_updates;

END;
$$;