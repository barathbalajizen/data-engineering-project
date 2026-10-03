"""Unit tests for the Silver validation rules."""
from validation import accepted, apply_rules, castable, check, not_null, rule_failure_counts

RULES = not_null("id", "status") + [
    accepted("status", ["ok", "done"]),
    castable("amount", "double"),
    check("amount_positive", "try_cast(amount AS double) > 0"),
]


def _df(spark):
    return spark.createDataFrame(
        [("1", "ok", "10.5"),       # valid
         ("2", "bad", "5"),         # accepted:status
         (None, "ok", "abc"),       # not_null:id, type:amount, amount_positive
         ("4", None, "-1"),         # not_null:status, amount_positive (accepted passes on NULL)
         ("5", "done", None)],      # amount_positive (NULL counts as failure)
        "id string, status string, amount string")


def test_valid_rows_pass_and_lose_no_columns(spark):
    valid, _ = apply_rules(_df(spark), RULES)
    assert [r["id"] for r in valid.collect()] == ["1"]
    assert valid.columns == ["id", "status", "amount"]


def test_every_failed_rule_is_listed(spark):
    _, invalid = apply_rules(_df(spark), RULES)
    got = {r["amount"]: r["failed_rules"] for r in invalid.collect()}
    assert got["5"] == ["accepted:status"]
    assert got["abc"] == ["not_null:id", "type:amount:double", "amount_positive"]
    assert got["-1"] == ["not_null:status", "amount_positive"]
    assert got[None] == ["amount_positive"]


def test_reason_is_readable(spark):
    _, invalid = apply_rules(_df(spark), RULES)
    reason = invalid.filter("amount = 'abc'").first()["reason"]
    assert reason == "not_null:id, type:amount:double, amount_positive"


def test_failure_counts_per_rule(spark):
    _, invalid = apply_rules(_df(spark), RULES)
    assert rule_failure_counts(invalid) == {"accepted:status": 1, "not_null:id": 1, "type:amount:double": 1,
                                            "amount_positive": 3, "not_null:status": 1}


def test_no_rules_means_all_valid(spark):
    valid, invalid = apply_rules(_df(spark), [])
    assert valid.count() == 5 and invalid.count() == 0 and "failed_rules" in invalid.columns


def test_quotes_in_accepted_values_are_escaped(spark):
    df = spark.createDataFrame([("it's",), ("x",)], "v string")
    valid, invalid = apply_rules(df, [accepted("v", ["it's"])])
    assert [r["v"] for r in valid.collect()] == ["it's"] and invalid.count() == 1
