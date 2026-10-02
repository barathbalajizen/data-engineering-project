#!/usr/bin/env bash
# Runs the full pipeline once, without any orchestrator (handy for debugging).
# Usage: docker compose exec pipeline bash /app/src/run_pipeline.sh [--start 2017-03-01 --end 2017-04-01]
set -euo pipefail
export RUN_ID="${RUN_ID:-manual-$(date +%Y%m%dT%H%M%S)}"
PY=/opt/venv/bin/python
DBT=/opt/dbt_venv/bin/dbt
cd /app

$PY src/extract_bronze.py "$@"
$PY src/transform_silver.py
$PY src/load_warehouse.py
(cd dbt_project && $DBT build)
$PY src/checks.py
echo "Pipeline finished OK (run_id=$RUN_ID)"
