"""Unit tests for schema drift detection and the evolution policy (pure Python)."""
import pytest
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

from schema_drift import SchemaDriftError, compare_schemas, enforce, normalize_type, schema_dict

BRONZE = {"order_id": "string", "price": "double", "qty": "bigint", "updated_at": "timestamp",
          "ingestion_ts": "timestamp", "batch_id": "string"}


def test_no_target_yet_is_not_drift():
    assert not compare_schemas(None, {"a": "int"}).has_changes


def test_identical_schema_has_no_drift():
    incoming = {k: v for k, v in BRONZE.items() if k not in ("ingestion_ts", "batch_id")}
    report = compare_schemas(BRONZE, incoming)
    assert not report.has_changes and report.summary() == "no drift"


def test_metadata_columns_are_ignored():
    incoming = {"order_id": "string", "price": "double", "qty": "bigint", "updated_at": "timestamp",
                "pipeline_run_id": "string"}
    assert not compare_schemas(BRONZE, incoming).has_changes


def test_added_column_is_allowed_and_evolves():
    incoming = {"order_id": "string", "price": "double", "qty": "bigint", "updated_at": "timestamp",
                "coupon": "string"}
    report = compare_schemas(BRONZE, incoming)
    assert report.added == [("coupon", "string")] and not report.breaking()
    assert report.changes() == [("added", "coupon", None, "string", "evolved")]
    enforce(report, "bronze.orders")   # does not raise


def test_narrower_type_is_cast_not_rejected():
    incoming = {"order_id": "string", "price": "float", "qty": "int", "updated_at": "timestamp"}
    report = compare_schemas(BRONZE, incoming)
    assert sorted(report.casts) == [("price", "float", "double"), ("qty", "int", "bigint")]
    assert not report.breaking()


def test_incompatible_type_change_fails():
    incoming = {"order_id": "string", "price": "string", "qty": "bigint", "updated_at": "timestamp"}
    report = compare_schemas(BRONZE, incoming)
    assert report.incompatible == [("price", "double", "string")]
    with pytest.raises(SchemaDriftError, match="price"):
        enforce(report, "bronze.orders")


def test_wider_type_change_fails():
    # bigint data into an int column would lose information: not a safe cast
    report = compare_schemas({"qty": "int"}, {"qty": "bigint"})
    assert report.incompatible == [("qty", "int", "bigint")]


def test_removed_column_fails_unless_allowed():
    incoming = {"order_id": "string", "price": "double", "updated_at": "timestamp"}
    report = compare_schemas(BRONZE, incoming)
    assert report.removed == [("qty", "bigint")]
    with pytest.raises(SchemaDriftError, match="removed: qty"):
        enforce(report, "bronze.orders")
    enforce(report, "bronze.orders", allow_removed=True)
    assert report.changes(allow_removed=True) == [("removed", "qty", "bigint", None, "filled_null")]
    assert report.changes() == [("removed", "qty", "bigint", None, "rejected")]


def test_column_names_are_case_insensitive():
    assert not compare_schemas({"Order_ID": "string"}, {"order_id": "string"}).has_changes


def test_type_aliases_are_normalised():
    assert normalize_type("INTEGER") == "int" and normalize_type("long") == "bigint"
    assert not compare_schemas({"a": "integer"}, {"a": "int"}).has_changes


def test_schema_dict_from_spark_struct():
    struct = StructType([StructField("Order_Id", StringType()), StructField("qty", IntegerType()),
                         StructField("n", LongType()), StructField("p", DoubleType()),
                         StructField("ts", TimestampType())])
    assert schema_dict(struct) == {"order_id": "string", "qty": "int", "n": "bigint", "p": "double",
                                   "ts": "timestamp"}
