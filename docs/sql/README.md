# Governed SQL Transformations (Day 12–13)

## Standards
- Every SQL file must include a metadata header (author, owner, purpose, dependencies, quality expectations, version).
- Transforms use CTEs for modularity and readability.
- Quality tests follow "zero-row failure" pattern.

## Execution
- Local runner: `python scripts/day12_run_sql_workflow.py`
- Glue scheduling: Python Shell job runs `glue_jobs/day13_run_sql_workflow_glue.py`

## Audit / Evidence
- Test results are logged to `dq.test_results` with `run_id`
- Materialized views refreshed as part of workflow
