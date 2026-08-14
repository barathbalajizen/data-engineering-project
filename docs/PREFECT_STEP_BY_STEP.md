# Prefect Flow Step-by-Step Guide

Use this guide to run and view your Prefect flow in two modes:

- Prefect Cloud (recommended)
- Local Prefect Server UI

## Quick Decision

- Want runs in Prefect Cloud workspace: use Cloud mode.
- Want local UI at http://127.0.0.1:4200: use Local mode.

## Prerequisites

From project root:

```powershell
uv sync --extra dev
```

Flow command used by this project:

```powershell
uv run python src/flows/prefect_flow.py
```

---

## Mode A: Prefect Cloud (Recommended)

### Step 1. Ensure profile exists

```powershell
prefect profile create ecommerce-cloud
```

If it already exists, Prefect will tell you. That is fine.

### Step 2. Switch to Cloud profile

```powershell
prefect profile use ecommerce-cloud
```

### Step 3. Login to Prefect Cloud

```powershell
prefect cloud login
```

### Step 4. Verify profile and API target

```powershell
prefect profile ls
prefect config view
```

Confirm `PREFECT_API_URL` points to a cloud URL (`https://api.prefect.cloud/...`).

### Step 5. Run the flow

Single command shortcut:

```powershell
.\scripts\tasks.ps1 flow-cloud
```

Alternative direct run:

```powershell
uv run python src/flows/prefect_flow.py
```

### Step 5b. Backfill historical dates (optional)

Single historical day:

```powershell
uv run python src/flows/prefect_flow.py --run-date 2026-08-10
```

Date window backfill (inclusive):

```powershell
uv run python src/flows/prefect_flow.py --start-date 2026-08-01 --end-date 2026-08-07
```

Notes:
- `--run-date` executes one run date.
- `--start-date` and `--end-date` execute one flow cycle per day in the window.

### Step 6. View run in Cloud UI

1. Open https://app.prefect.cloud
2. Select your workspace
3. Open **Flow Runs**
4. Click the latest run

You should see task states for:
- Phase 1 Ingestion
- Phase 2 Sentiment Extraction
- Phase 3 Bronze to Silver
- Phase 4 Silver to Gold

---

## Mode B: Local Prefect Server UI

### Step 1. Start local Prefect server

```powershell
uv run prefect server start
```

Keep this terminal running.

### Step 2. In a new terminal, set local API URL

```powershell
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

### Step 3. Run the flow

```powershell
.\scripts\tasks.ps1 flow
```

Backfill examples work the same in local mode:

```powershell
uv run python src/flows/prefect_flow.py --run-date 2026-08-10
uv run python src/flows/prefect_flow.py --start-date 2026-08-01 --end-date 2026-08-07
```

### Step 4. View local UI

Open http://127.0.0.1:4200 and go to **Flow Runs**.

---

## Switch Back to Cloud After Local Mode

If you used local mode and want Cloud again:

```powershell
prefect profile use ecommerce-cloud
```

Then run:

```powershell
.\scripts\tasks.ps1 flow-cloud
```

---

## What Success Looks Like in Terminal

You should see lines similar to:

- `Task run 'Phase 1 Ingestion-0' - Finished in state Completed()`
- `Task run 'Phase 2 Sentiment Extraction-0' - Finished in state Completed()`
- `Task run 'Phase 3 Bronze to Silver-0' - Finished in state Completed()`
- `Task run 'Phase 4 Silver to Gold-0' - Finished in state Completed()`
- `Pipeline flow completed successfully`

---

## Quick Troubleshooting

### Runs not visible in expected UI

Check:

```powershell
prefect profile ls
prefect config view
```

- Cloud mode needs cloud API URL.
- Local mode needs `http://127.0.0.1:4200/api`.

### Profile mismatch

```powershell
prefect profile use ecommerce-cloud
```

### Fast status helper

```powershell
.\scripts\tasks.ps1 cloud-status
```
