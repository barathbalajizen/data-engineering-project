"""Row-level validation rules for Silver (pure PySpark, unit-tested).

A rule is a name plus a SQL condition that VALID rows satisfy; NULL counts as a failure. A row that fails
any rule is quarantined with the list of every rule it failed (failed_rules) and a readable reason, so the
quarantine shows exactly why each row was rejected.

    rules = not_null("order_id", "customer_id") + [accepted("order_status", STATUSES)]
    valid, invalid = apply_rules(df, rules)
"""
from dataclasses import dataclass

from pyspark.sql import functions as F

FAILED_RULES = "failed_rules"
REASON = "reason"


@dataclass(frozen=True)
class Rule:
    name: str
    condition: str


def not_null(*cols):
    """Primary-key and required-column checks."""
    return [Rule(f"not_null:{c}", f"{c} IS NOT NULL") for c in cols]


def accepted(col, values):
    """Value must be one of `values` (NULL passes; combine with not_null to forbid it)."""
    # Spark SQL escapes quotes with a backslash ('it''s' would be read as two literals: 'its')
    quoted = ", ".join("'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'" for v in values)
    return Rule(f"accepted:{col}", f"{col} IS NULL OR {col} IN ({quoted})")


def castable(col, spark_type):
    """Data type check: the value can be read as `spark_type` (try_cast returns NULL when it cannot).
    Guards columns whose source type may drift, e.g. a numeric column arriving as text."""
    return Rule(f"type:{col}:{spark_type}", f"{col} IS NULL OR try_cast({col} AS {spark_type}) IS NOT NULL")


def check(name, condition):
    """Any other business rule, e.g. check("price_positive", "price > 0")."""
    return Rule(name, condition)


def apply_rules(df, rules):
    """Return (valid_df, invalid_df). invalid_df has failed_rules (array) and reason (string) columns.
    With no rules every row is valid."""
    if not rules:
        empty = df.limit(0).withColumn(FAILED_RULES, F.array().cast("array<string>")).withColumn(REASON, F.lit(""))
        return df, empty
    failures = F.filter(
        F.array(*[F.when(~F.coalesce(F.expr(r.condition), F.lit(False)), F.lit(r.name)) for r in rules]),
        lambda x: x.isNotNull())
    checked = df.withColumn(FAILED_RULES, failures)
    valid = checked.filter(F.size(FAILED_RULES) == 0).drop(FAILED_RULES)
    invalid = (checked.filter(F.size(FAILED_RULES) > 0)
               .withColumn(REASON, F.concat_ws(", ", F.col(FAILED_RULES))))
    return valid, invalid


def rule_failure_counts(invalid_df):
    """{rule name: rows failing it} for logging and the audit trail."""
    rows = invalid_df.select(F.explode(FAILED_RULES).alias("rule")).groupBy("rule").count().collect()
    return {r["rule"]: r["count"] for r in rows}
