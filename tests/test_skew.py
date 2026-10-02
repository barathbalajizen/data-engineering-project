from transforms import key_distribution, salted_join


def test_salted_join_equals_plain_join(spark):
    big = spark.createDataFrame([("a", 1)] * 50 + [("b", 2)] * 5 + [("c", 3)], ["k", "v"])
    small = spark.createDataFrame([("a", "x"), ("b", "y"), ("c", "z")], ["k", "s"])
    assert sorted(big.join(small, "k").collect()) == sorted(salted_join(big, small, "k", 4).collect())


def test_salted_join_spreads_hot_key(spark):
    from pyspark.sql import functions as F
    big = spark.createDataFrame([("hot", i) for i in range(400)], ["k", "v"])
    small = spark.createDataFrame([("hot", "x")], ["k", "s"])
    salted = big.withColumn("_salt", (F.rand(42) * 4).cast("int"))
    assert salted.select("_salt").distinct().count() == 4      # hot key now has 4 sub-keys
    assert salted_join(big, small, "k", 4).count() == 400      # and no rows lost/duplicated


def test_key_distribution_finds_hot_key(spark):
    df = spark.createDataFrame([("hot",)] * 8 + [("a",), ("b",)], ["k"])
    top = key_distribution(df, "k", 1)[0]
    assert top[0] == "hot" and top[1] == 8 and abs(top[2] - 0.8) < 1e-9
