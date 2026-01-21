CREATE SCHEMA IF NOT EXISTS dq;

-- If query returns any rows, raise exception
CREATE OR REPLACE FUNCTION dq.assert_zero_rows(failing_count BIGINT, test_name TEXT)
RETURNS VOID AS $$
BEGIN
  IF failing_count > 0 THEN
    RAISE EXCEPTION 'DQ_TEST_FAILED: % had % failing rows', test_name, failing_count;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Store test results (audit trail for SQL tests)
CREATE TABLE IF NOT EXISTS dq.test_results (
  run_id TEXT NOT NULL,
  test_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PASS','FAIL')),
  failing_rows BIGINT NOT NULL,
  details TEXT,
  ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (run_id, test_name)
);