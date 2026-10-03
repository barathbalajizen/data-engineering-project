"""Unit tests for turning dbt artifacts into data quality log rows."""
from dbt_results import category_of, parse_freshness, parse_run_results, short_test_name


def test_short_test_name_from_unique_id():
    assert short_test_name("test.ecommerce.not_null_fact_orders_price.a1b2") == "not_null_fact_orders_price"
    assert short_test_name("weird") == "weird"


def test_category_from_generic_test():
    assert category_of("not_null_fact_orders_price") == "completeness"
    assert category_of("unique_fact_orders_order_item_key") == "uniqueness"
    assert category_of("unique_combination_stg_order_items_order_id__order_item_id") == "uniqueness"
    assert category_of("relationships_fact_orders_date_key__date_key__ref_dim_date_") == "referential_integrity"
    assert category_of("accepted_values_fact_orders_is_late__0__1") == "validity"
    assert category_of("source_unique_staging_orders_order_id") == "uniqueness"
    assert category_of("assert_revenue_reconciles") == "business_rule"


def test_parse_run_results_keeps_tests_only():
    doc = {"results": [
        {"unique_id": "model.ecommerce.fact_orders", "status": "success"},
        {"unique_id": "test.ecommerce.not_null_fact_orders_price.x", "status": "pass", "failures": 0},
        {"unique_id": "test.ecommerce.relationships_stg_order_items_order_id.y", "status": "warn", "failures": 3,
         "message": "Got 3 results, configured to warn if != 0"},
        {"unique_id": "test.ecommerce.assert_revenue_reconciles.z", "status": "fail", "failures": 1},
        {"unique_id": "test.ecommerce.unique_dim_seller_seller_key.w", "status": "skipped"},
        {"unique_id": "test.ecommerce.unique_dim_product_product_key.v", "status": "error", "failures": None,
         "message": "relation does not exist"},
    ]}
    rows = parse_run_results(doc, "run-1")
    assert [(r["name"], r["status"], r["failed"], r["category"]) for r in rows] == [
        ("dbt:not_null_fact_orders_price", "PASS", 0, "completeness"),
        ("dbt:relationships_stg_order_items_order_id", "WARN", 3, "referential_integrity"),
        ("dbt:assert_revenue_reconciles", "FAIL", 1, "business_rule"),
        ("dbt:unique_dim_product_product_key", "FAIL", 0, "uniqueness"),
    ]
    assert all(r["run_id"] == "run-1" and r["source"] == "dbt" for r in rows)


def test_parse_freshness():
    doc = {"results": [
        {"unique_id": "source.ecommerce.staging.orders", "status": "warn",
         "max_loaded_at": "2026-10-02T03:56:55", "max_loaded_at_time_ago_in_s": 108000.0},
        {"unique_id": "source.ecommerce.staging.customers", "status": "pass",
         "max_loaded_at": "2026-10-03T08:00:00", "max_loaded_at_time_ago_in_s": 3600.0},
        {"unique_id": "source.ecommerce.staging.sellers", "status": "runtime error", "error": "boom"},
    ]}
    rows = parse_freshness(doc, "run-1")
    assert [(r["name"], r["status"], r["category"]) for r in rows] == [
        ("dbt_freshness:staging.orders", "WARN", "freshness"),
        ("dbt_freshness:staging.customers", "PASS", "freshness"),
        ("dbt_freshness:staging.sellers", "FAIL", "freshness"),
    ]
    assert rows[0]["detail"].endswith("age_hours=30.0") and rows[2]["detail"] == "boom"
