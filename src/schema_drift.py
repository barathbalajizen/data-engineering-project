"""Schema drift detection and controlled schema evolution (pure Python, unit-tested).

The baseline is the schema already accepted into the target (the Bronze Delta table, or a Postgres staging
table). Each load compares the incoming schema with it:

  added column       -> allowed: the target evolves (new column, NULL for older rows)
  narrower type      -> allowed: cast to the target type (e.g. int -> bigint), listed in SAFE_CASTS
  removed column     -> breaking: fail, unless allow_removed=True (then it is filled with NULL)
  other type change  -> breaking: fail (e.g. text -> int, bigint -> int, double -> int)

Without this check Delta MERGE silently drops new source columns and a type change fails deep inside a
write; here every change is detected up front, decided by an explicit rule and recorded in
audit.schema_changes.
"""
from dataclasses import dataclass, field

# Pipeline-added columns: not part of the source contract, never reported as drift
METADATA_COLUMNS = frozenset({"ingestion_ts", "batch_id", "source_system", "pipeline_run_id", "source_table"})

# (incoming type, target type) pairs that can be cast without losing information
SAFE_CASTS = frozenset({
    ("tinyint", "smallint"), ("tinyint", "int"), ("tinyint", "bigint"),
    ("smallint", "int"), ("smallint", "bigint"), ("int", "bigint"),
    ("float", "double"), ("int", "double"), ("smallint", "double"),
    ("date", "timestamp"),
})


class SchemaDriftError(Exception):
    """A breaking schema change that the policy does not allow."""


@dataclass
class DriftReport:
    added: list = field(default_factory=list)          # [(column, type)]
    removed: list = field(default_factory=list)        # [(column, type)]
    casts: list = field(default_factory=list)          # [(column, incoming type, target type)]
    incompatible: list = field(default_factory=list)   # [(column, target type, incoming type)]

    @property
    def has_changes(self):
        return bool(self.added or self.removed or self.casts or self.incompatible)

    def breaking(self, allow_removed=False):
        return bool(self.incompatible or (self.removed and not allow_removed))

    def changes(self, allow_removed=False):
        """Rows for audit.schema_changes: (change_type, column, old_type, new_type, action)."""
        rows = [("added", c, None, t, "evolved") for c, t in self.added]
        rows += [("removed", c, t, None, "filled_null" if allow_removed else "rejected") for c, t in self.removed]
        rows += [("type_changed", c, tgt, src, "cast") for c, src, tgt in self.casts]
        rows += [("type_changed", c, old, new, "rejected") for c, old, new in self.incompatible]
        return rows

    def summary(self):
        parts = []
        for name, items in (("added", self.added), ("removed", self.removed), ("cast", self.casts),
                            ("incompatible", self.incompatible)):
            if items:
                parts.append(f"{name}: " + ", ".join(":".join(str(x) for x in i) for i in items))
        return "; ".join(parts) or "no drift"


def normalize_type(t):
    """Spark simpleString type, lower-case; Postgres-style aliases mapped to Spark names."""
    t = str(t).strip().lower()
    return {"integer": "int", "long": "bigint", "short": "smallint", "byte": "tinyint", "real": "float",
            "double precision": "double", "boolean": "boolean", "text": "string"}.get(t, t)


def schema_dict(struct):
    """{column name (lower-case): type} for a pyspark StructType, skipping nothing."""
    return {f.name.lower(): normalize_type(f.dataType.simpleString()) for f in struct.fields}


def compare_schemas(existing, incoming, ignore=METADATA_COLUMNS):
    """Compare {column: type} dicts. existing=None (no target yet) means no drift."""
    report = DriftReport()
    if existing is None:
        return report
    old = {c.lower(): normalize_type(t) for c, t in existing.items() if c.lower() not in ignore}
    new = {c.lower(): normalize_type(t) for c, t in incoming.items() if c.lower() not in ignore}
    for col, t in new.items():
        if col not in old:
            report.added.append((col, t))
        elif t != old[col]:
            if (t, old[col]) in SAFE_CASTS:
                report.casts.append((col, t, old[col]))
            else:
                report.incompatible.append((col, old[col], t))
    report.removed = [(c, t) for c, t in old.items() if c not in new]
    return report


def enforce(report, table, allow_removed=False):
    """Raise SchemaDriftError for changes the policy does not allow."""
    if report.breaking(allow_removed):
        raise SchemaDriftError(
            f"{table}: breaking schema change ({report.summary()}). Bronze was not written. Fix the source, "
            "or evolve the target explicitly (see README > Schema drift) and rerun.")
