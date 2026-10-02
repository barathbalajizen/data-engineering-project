#!/usr/bin/env bash
# Prefect task wrapper for dbt.
# First attempt of a run  -> `dbt build` (everything).
# Retry after a failure   -> `dbt retry` (resumes from the failed node, skips models that already passed).
set -uo pipefail
DBT=/opt/dbt_venv/bin/dbt
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

if [ $rc -eq 0 ]; then rm -f "$MARK"; else echo "${RUN_ID:-none}" > "$MARK"; fi
exit $rc
