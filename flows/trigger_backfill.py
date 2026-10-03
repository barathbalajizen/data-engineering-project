"""Start a backfill run from the command line (same as Deployments > backfill > Run in the UI).

  python flows/trigger_backfill.py --start 2017-03-01 --end 2017-04-01 [--chunk-days 31] [--skip-downstream]

Returns immediately; follow the run in the UI or with:  prefect flow-run watch <flow-run-id>
"""
import argparse

from prefect.deployments import run_deployment

ap = argparse.ArgumentParser()
ap.add_argument("--start", required=True, help="inclusive, e.g. 2017-03-01")
ap.add_argument("--end", required=True, help="exclusive, e.g. 2017-04-01")
ap.add_argument("--chunk-days", type=int, default=31)
ap.add_argument("--skip-downstream", action="store_true", help="only refill Bronze")
a = ap.parse_args()

run = run_deployment(
    name="ecommerce-backfill/03-backfill-date-range",
    parameters={"start": a.start, "end": a.end, "chunk_days": a.chunk_days,
                "rebuild_downstream": not a.skip_downstream},
    timeout=0,   # do not wait for completion
)
print(f"Backfill started: flow run {run.name} ({run.id})")
print(f"Watch it: prefect flow-run watch {run.id}   (or open http://localhost:4200)")
