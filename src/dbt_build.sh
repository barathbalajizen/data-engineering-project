#!/usr/bin/env bash
# Prefect task wrapper for dbt.
# First attempt of a run  -> `dbt build` (everything).
# Retry after a failure   -> `dbt retry` (resumes from the failed node, skips models that already passed).
# Test results (pass or fail) are recorded in audit.dq_log, then `dbt source freshness` runs (warn-only:
# a quiet day without new data does not fail the pipeline). Only the build's exit code decides the task.
set -uo pipefail
DBT=/opt/dbt_venv/bin/dbt
PY=/opt/venv/bin/python
MARK=/tmp/dbt/last_failed_run
mkdir -p /tmp/dbt
cd /app/dbt_project

if [ "$(cat "$MARK" 2>/dev/null || true)" = "${RUN_ID:-none}" ]; then
  echo "Previous attempt of run ${RUN_ID} failed -> resuming with dbt retry"
  "$DBT" retry
  rc=$?
else
  "$DBT" build
  rc=$?
fi

"$PY" /app/src/dbt_results.py tests || echo "WARNING: could not record dbt test results in audit.dq_log"

if [ $rc -eq 0 ]; then
  rm -f "$MARK"
  "$DBT" source freshness || echo "WARNING: dbt source freshness reported an error (see above)"
  "$PY" /app/src/dbt_results.py freshness || echo "WARNING: could not record freshness results in audit.dq_log"
else
  echo "${RUN_ID:-none}" > "$MARK"
fi
exit $rc
