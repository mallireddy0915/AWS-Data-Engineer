CREATE OR REPLACE VIEW governance.vw_dashboard_kpis AS
SELECT
  g.dataset_name,
  g.owner_team,
  g.steward,
  g.classification,
  g.certified_flag,
  g.completeness_pct,
  g.last_refresh_ts,

  -- Example business KPI: revenue for last 30 days (adjust window)
  SUM(f.total_amount) AS revenue_30d,
  COUNT(*)            AS trips_30d
FROM governance.dataset_governance g
LEFT JOIN analytics.fact_yellow_trip f
  ON g.dataset_name = 'fact_yellow_trip'
 AND f.pickup_ts >= dateadd(day, -30, getdate())
GROUP BY 1,2,3,4,5,6,7;
