"""Safe persistence of pipeline progress: the extract watermark (control.watermark).

The watermark only ever moves forward: the UPDATE itself carries `last_watermark < :new`, so a stale or
concurrent writer cannot move it back (a deliberate reset is a separate, explicit statement in load_source).
Every change is recorded by a database trigger in control.checkpoint_history; set_run_context() tags the
transaction with the pipeline run id so the history shows which run made it.
"""
import sqlalchemy as sa

from resilience import retry


def set_run_context(conn, run_id):
    """Tag the current transaction with the run id (read by the checkpoint history trigger)."""
    conn.execute(sa.text("SELECT set_config('app.run_id', :r, true)"), {"r": run_id or ""})


@retry(attempts=3, base_delay=2)
def get_watermark(eng, table):
    with eng.connect() as c:
        return c.execute(sa.text("SELECT last_watermark FROM control.watermark WHERE table_name = :t"),
                         {"t": table}).scalar()


@retry(attempts=3, base_delay=2)
def advance_watermark(eng, table, value, run_id):
    """Move the watermark to `value` if that is later than the stored one. Returns True if it moved."""
    with eng.begin() as c:
        set_run_context(c, run_id)
        moved = c.execute(sa.text(
            "UPDATE control.watermark SET last_watermark = :w, updated_at = now() "
            "WHERE table_name = :t AND last_watermark < :w"), {"w": value, "t": table}).rowcount
    return moved == 1
