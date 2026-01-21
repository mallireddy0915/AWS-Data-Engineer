CREATE OR REPLACE PROCEDURE dq.run_all_tests(p_run_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
  c BIGINT;
BEGIN
  -- Test 1: no negative revenue
  SELECT (
    WITH failures AS (
      SELECT 1 FROM analytics.trip_metrics_by_zone_day WHERE total_revenue < 0
    )
    SELECT COUNT(*) FROM failures
  ) INTO c;

  INSERT INTO dq.test_results(run_id, test_name, status, failing_rows, details)
  VALUES (p_run_id, 'test_no_negative_revenue', CASE WHEN c=0 THEN 'PASS' ELSE 'FAIL' END, c, 'total_revenue >= 0')
  ON CONFLICT (run_id, test_name) DO UPDATE SET status=EXCLUDED.status, failing_rows=EXCLUDED.failing_rows, details=EXCLUDED.details;

  PERFORM dq.assert_zero_rows(c, 'test_no_negative_revenue');

  -- Test 2: keys not null
  SELECT (
    WITH failures AS (
      SELECT 1 FROM analytics.trip_metrics_by_zone_day WHERE trip_date IS NULL OR pu_location_id IS NULL
    )
    SELECT COUNT(*) FROM failures
  ) INTO c;

  INSERT INTO dq.test_results(run_id, test_name, status, failing_rows, details)
  VALUES (p_run_id, 'test_not_null_keys', CASE WHEN c=0 THEN 'PASS' ELSE 'FAIL' END, c, 'PK columns not null')
  ON CONFLICT (run_id, test_name) DO UPDATE SET status=EXCLUDED.status, failing_rows=EXCLUDED.failing_rows, details=EXCLUDED.details;

  PERFORM dq.assert_zero_rows(c, 'test_not_null_keys');
END;
$$;