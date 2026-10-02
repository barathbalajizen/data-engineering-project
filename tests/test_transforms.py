from transforms import clean_strings, dedupe_latest, split_valid_invalid


def test_dedupe_keeps_latest(spark):
    df = spark.createDataFrame(
        [("o1", "2024-01-01"), ("o1", "2024-01-02"), ("o2", "2024-01-01")], ["order_id", "updated_at"])
    out = {r.order_id: r.updated_at for r in dedupe_latest(df, ["order_id"], ["updated_at"]).collect()}
    assert out == {"o1": "2024-01-02", "o2": "2024-01-01"}


def test_split_valid_invalid(spark):
    df = spark.createDataFrame(
        [("o1", 10.0), (None, 5.0), ("o3", -1.0), ("o4", None)], "order_id string, price double")
    good, bad = split_valid_invalid(df, ["order_id"], "price > 0")
    assert [r.order_id for r in good.collect()] == ["o1"]
    assert bad.count() == 3


def test_clean_strings_trims(spark):
    df = spark.createDataFrame([("  abc  ", 1)], ["name", "n"])
    assert clean_strings(df).first().name == "abc"
