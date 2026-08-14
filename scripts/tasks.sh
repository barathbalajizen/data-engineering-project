#!/usr/bin/env bash
set -euo pipefail

TASK="${1:-help}"
PREFECT_PROFILE_NAME="${PREFECT_PROFILE:-ecommerce-cloud}"
PID_FILE=".prefect-server.pid"

run_step() {
  local cmd="$1"
  echo "> ${cmd}"
  eval "${cmd}"
}

is_pid_running() {
  local pid="$1"
  if [[ -z "${pid}" ]]; then
    return 1
  fi
  kill -0 "${pid}" >/dev/null 2>&1
}

service_start() {
  if [[ -f "${PID_FILE}" ]]; then
    local existing_pid
    existing_pid="$(cat "${PID_FILE}")"
    if is_pid_running "${existing_pid}"; then
      echo "Prefect local server is already running (PID: ${existing_pid})."
      echo "Local UI: http://127.0.0.1:4200"
      return 0
    fi
    rm -f "${PID_FILE}"
  fi

  nohup prefect server start > prefect-server.log 2>&1 &
  local new_pid=$!
  echo "${new_pid}" > "${PID_FILE}"
  echo "Started Prefect local server (PID: ${new_pid})."
  echo "Local UI: http://127.0.0.1:4200"
  echo "Logs: prefect-server.log"
}

service_stop() {
  if [[ -f "${PID_FILE}" ]]; then
    local pid
    pid="$(cat "${PID_FILE}")"
    if is_pid_running "${pid}"; then
      kill "${pid}" || true
      sleep 1
      if is_pid_running "${pid}"; then
        kill -9 "${pid}" || true
      fi
      echo "Stopped Prefect server PID ${pid}."
    else
      echo "No running process for PID ${pid}."
    fi
    rm -f "${PID_FILE}"
    return 0
  fi

  local found_pids
  found_pids="$(pgrep -f "prefect server start" || true)"
  if [[ -z "${found_pids}" ]]; then
    echo "No running Prefect local server process found."
    return 0
  fi

  while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    kill "${pid}" || true
    echo "Stopped Prefect server PID ${pid}."
  done <<< "${found_pids}"
}

service_status() {
  if [[ -f "${PID_FILE}" ]]; then
    local pid
    pid="$(cat "${PID_FILE}")"
    if is_pid_running "${pid}"; then
      echo "Prefect local server is running (PID: ${pid})."
      echo "Local UI: http://127.0.0.1:4200"
      return 0
    fi
    echo "Prefect local server PID file exists but process is not running."
    echo "Stale PID file: ${PID_FILE}"
    return 1
  fi

  local found_pids
  found_pids="$(pgrep -f "prefect server start" || true)"
  if [[ -n "${found_pids}" ]]; then
    echo "Prefect local server is running:"
    echo "${found_pids}"
    echo "Local UI: http://127.0.0.1:4200"
    return 0
  fi

  echo "Prefect local server is not running."
}

show_help() {
  echo "Available tasks:"
  echo "  ./scripts/tasks.sh sync         - Install runtime dependencies from uv.lock"
  echo "  ./scripts/tasks.sh sync-dev     - Install runtime + dev dependencies"
  echo "  ./scripts/tasks.sh lock         - Refresh uv.lock from pyproject.toml"
  echo "  ./scripts/tasks.sh test         - Run unit tests"
  echo "  ./scripts/tasks.sh test-cov     - Run tests with coverage report"
  echo "  ./scripts/tasks.sh flow         - Run full Prefect flow"
  echo "  ./scripts/tasks.sh cloud-status - Show Prefect profile and API configuration"
  echo "  ./scripts/tasks.sh cloud-login  - Login to Prefect Cloud"
  echo "  ./scripts/tasks.sh cloud-use    - Switch to PREFECT_PROFILE (default: ecommerce-cloud)"
  echo "  ./scripts/tasks.sh flow-cloud   - Use cloud profile, then run full Prefect flow"
  echo "  ./scripts/tasks.sh service-start  - Start local Prefect server in background"
  echo "  ./scripts/tasks.sh service-stop   - Stop local Prefect server background process"
  echo "  ./scripts/tasks.sh service-status - Show local Prefect server process status"
  echo "  ./scripts/tasks.sh ingest       - Run ingestion scripts"
  echo "  ./scripts/tasks.sh sentiment    - Run sentiment enrichment"
  echo "  ./scripts/tasks.sh silver       - Run Bronze to Silver pipeline"
  echo "  ./scripts/tasks.sh gold         - Run Silver to Gold pipeline"
  echo "  ./scripts/tasks.sh lint         - Run flake8"
  echo "  ./scripts/tasks.sh format       - Run black + isort"
  echo "  ./scripts/tasks.sh typecheck    - Run mypy"
}

case "${TASK}" in
  help)
    show_help
    ;;
  sync)
    run_step "uv sync"
    ;;
  sync-dev)
    run_step "uv sync --extra dev"
    ;;
  lock)
    run_step "uv lock"
    ;;
  test)
    run_step "uv run pytest tests/unit -q"
    ;;
  test-cov)
    run_step "uv run pytest tests --cov=src --cov-report=html"
    ;;
  flow)
    run_step "uv run python src/flows/prefect_flow.py"
    ;;
  cloud-status)
    run_step "prefect profile ls"
    run_step "prefect config view"
    ;;
  cloud-login)
    run_step "prefect cloud login"
    ;;
  cloud-use)
    run_step "prefect profile use ${PREFECT_PROFILE_NAME}"
    ;;
  flow-cloud)
    run_step "prefect profile use ${PREFECT_PROFILE_NAME}"
    run_step "uv run python src/flows/prefect_flow.py"
    ;;
  service-start)
    service_start
    ;;
  service-stop)
    service_stop
    ;;
  service-status)
    service_status
    ;;
  ingest)
    run_step "uv run python src/ingestion/orders_loader.py"
    run_step "uv run python src/ingestion/reviews_loader.py"
    run_step "uv run python src/ingestion/product_api_client.py"
    ;;
  sentiment)
    run_step "uv run python src/ingestion/sentiment_enrichment.py"
    ;;
  silver)
    run_step "uv run python src/transform/silver_layer_loader.py"
    ;;
  gold)
    run_step "uv run python src/transform/gold_layer_loader.py"
    ;;
  lint)
    run_step "uv run flake8 src tests"
    ;;
  format)
    run_step "uv run isort src tests"
    run_step "uv run black src tests"
    ;;
  typecheck)
    run_step "uv run mypy src"
    ;;
  *)
    echo "Unknown task: ${TASK}"
    show_help
    exit 1
    ;;
esac
