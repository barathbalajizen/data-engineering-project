#!/usr/bin/env bash
# End-to-end test of the whole stack on a FRESH checkout (used by CI, .github/workflows/ci.yml):
#   build images -> start services -> load a small dataset and run the full pipeline (setup flow) ->
#   run the incremental daily flow twice (idempotency) -> run every test inside the container
#   (unit + integration) -> require 0 failed data quality checks.
# Do not run it in a checkout whose stack you want to keep: it uses the same project and data folders.
set -euo pipefail
N_ORDERS="${N_ORDERS:-2000}"
cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env

exec_pipeline() { docker compose exec -T pipeline "$@"; }

echo "::group::build and start"
docker compose build
docker compose up -d
echo "::endgroup::"

echo "waiting for the deployments to be registered"
for _ in $(seq 1 60); do
  if exec_pipeline prefect deployment ls 2>/dev/null | grep -q "ecommerce-setup"; then break; fi
  sleep 5
done
exec_pipeline prefect deployment ls | grep -q "ecommerce-setup" || { echo "deployments not registered"; exit 1; }

run() {   # run a deployment and fail if the flow run does not complete
  echo "::group::$*"
  out=$(exec_pipeline prefect deployment run "$@" --watch 2>&1) || true
  echo "$out" | tail -5
  echo "::endgroup::"
  echo "$out" | grep -q "finished successfully" || { echo "flow run failed: $*"; exit 1; }
}

run 'ecommerce-setup/run' -p n_orders="$N_ORDERS"
run 'ecommerce-daily/daily'
run 'ecommerce-daily/daily'

echo "::group::tests (unit + integration)"
exec_pipeline pytest tests -q -p no:cacheprovider
echo "::endgroup::"

failed=$(docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "
  SELECT failed FROM audit.dq_scorecard ORDER BY run_ts DESC LIMIT 1"')
echo "data quality checks failed in the latest run: ${failed}"
[ "${failed}" = "0" ]
echo "end-to-end test passed"
