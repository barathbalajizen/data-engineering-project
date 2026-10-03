"""Incremental Silver processing: which Bronze changes to read, and where the progress is stored.

Silver tracks the last Bronze Delta version it has merged (control.silver_checkpoint). Each run reads only
the Bronze commits after that version through Change Data Feed, or does a full pass when an incremental read
is not possible or not safe. decide_mode is pure Python so every case is unit-tested.
"""
import logging

from resilience import retry

log = logging.getLogger("incremental")

SKIP, INCREMENTAL, FULL = "skip", "incremental", "full"


def decide_mode(checkpoint, bronze_version, cdf_since, target_exists, force_full=False):
    """Return (mode, reason). mode is 'skip', 'incremental' or 'full'.

    checkpoint      last Bronze version merged into Silver (None = never)
    bronze_version  current Bronze version
    cdf_since       first Bronze version that has Change Data Feed data (None = CDF off)
    """
    if force_full:
        return FULL, "full refresh requested"
    if not target_exists:
        return FULL, "silver table does not exist yet"
    if checkpoint is None:
        return FULL, "no checkpoint yet"
    if bronze_version < checkpoint:
        return FULL, f"bronze version {bronze_version} is behind checkpoint {checkpoint} (table recreated?)"
    if bronze_version == checkpoint:
        return SKIP, f"no new bronze versions since {checkpoint}"
    if cdf_since is None:
        return FULL, "change data feed is not enabled on bronze"
    if checkpoint + 1 < cdf_since:
        return FULL, f"change data only exists from version {cdf_since}, checkpoint is {checkpoint}"
    return INCREMENTAL, f"bronze versions {checkpoint + 1}..{bronze_version}"


class PgCheckpointStore:
    """control.silver_checkpoint in Postgres."""

    def __init__(self, engine, run_id=None):
        self.eng, self.run_id = engine, run_id

    @retry(attempts=3, base_delay=2)
    def get(self, table):
        import sqlalchemy as sa
        with self.eng.connect() as c:
            return c.execute(sa.text("SELECT bronze_version FROM control.silver_checkpoint WHERE table_name = :t"),
                             {"t": table}).scalar()

    @retry(attempts=3, base_delay=2)
    def set(self, table, version, mode):
        import sqlalchemy as sa

        from checkpoints import set_run_context
        with self.eng.begin() as c:
            set_run_context(c, self.run_id)
            c.execute(sa.text(
                "INSERT INTO control.silver_checkpoint (table_name, bronze_version, mode, updated_at) "
                "VALUES (:t, :v, :m, now()) ON CONFLICT (table_name) DO UPDATE "
                "SET bronze_version = EXCLUDED.bronze_version, mode = EXCLUDED.mode, updated_at = now()"),
                {"t": table, "v": int(version), "m": mode})


class MemoryCheckpointStore:
    """In-memory store for tests."""

    def __init__(self, initial=None):
        self.data = dict(initial or {})

    def get(self, table):
        return self.data.get(table)

    def set(self, table, version, mode):
        self.data[table] = int(version)
