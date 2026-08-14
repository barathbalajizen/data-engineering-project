# Release Checklist

Use this checklist before cutting a release or merging major pipeline changes.

## 1. Dependency and Environment Hygiene

- Confirm [pyproject.toml](../pyproject.toml) and [uv.lock](../uv.lock) are in sync.
- Run dependency sync:
  - `uv sync --extra dev`
- If dependencies changed:
  - `uv lock`
  - commit both [pyproject.toml](../pyproject.toml) and [uv.lock](../uv.lock).

## 2. Static Quality Gates

- Lint:
  - `uv run flake8 src tests`
- Format check (or apply formatting):
  - `uv run isort src tests`
  - `uv run black src tests`
- Type check:
  - `uv run mypy src`

## 3. Test Gates

- Unit tests:
  - `uv run pytest tests/unit -q`
- Coverage report (optional but recommended):
  - `uv run pytest tests --cov=src --cov-report=html`

## 4. Pipeline Smoke Test

- Run end-to-end flow:
  - `uv run python src/flows/prefect_flow.py`
- Confirm final output includes:
  - `Pipeline flow completed successfully`
  - all phase statuses as `PASSED`.

## 5. Data Artifacts Sanity

- Confirm Silver and Gold row counts look reasonable in logs.
- Confirm no unexpected schema drift errors in Delta writes.
- If migrating schema paths, verify stale Delta table folders are handled safely.

## 6. Documentation and Ops

- Ensure setup commands in [README.md](../README.md) and [SETUP.md](../SETUP.md) match current workflow.
- Ensure CI passes in [.github/workflows/ci.yml](../.github/workflows/ci.yml).
- Update roadmap/report docs for completed phase milestones.

## 7. Final Release Steps

- Review git diff for accidental data/log artifacts.
- Tag release version and create release notes.
- Include key changes, migration notes, and rollback considerations.
