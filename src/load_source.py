"""Load CSVs into Postgres schema `source` (simulates the operational system).
Adds updated_at columns used for incremental extraction (orders) and SCD2 (customers).
"""
import os

import pandas as pd
import sqlalchemy as sa

from common import DATA, get_engine, get_logger

log = get_logger("load_source")

SPEC = {
    "customers": ("olist_customers_dataset.csv",
                  ["customer_id", "customer_unique_id", "customer_city", "customer_state"], []),
    "products": ("olist_products_dataset.csv",
                 ["product_id", "product_category_name", "product_weight_g"], []),
    "sellers": ("olist_sellers_dataset.csv", ["seller_id", "seller_city", "seller_state"], []),
    "orders": ("olist_orders_dataset.csv",
               ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
                "order_delivered_customer_date", "order_estimated_delivery_date"],
               ["order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date"]),
    "order_items": ("olist_order_items_dataset.csv",
                    ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"], []),
    "order_payments": ("olist_order_payments_dataset.csv",
                       ["order_id", "payment_sequential", "payment_type", "payment_value"], []),
}


def main():
    eng = get_engine()
    for table, (fname, cols, dates) in SPEC.items():
        path = os.path.join(DATA, fname)
        if not os.path.exists(path):
            raise SystemExit(f"Missing {path}. Download Olist from Kaggle or run generate_sample_data.py")
        df = pd.read_csv(path, usecols=cols, parse_dates=dates)[cols]
        if table == "orders":
            df["updated_at"] = df["order_purchase_timestamp"]
        if table == "customers":
            df["updated_at"] = pd.Timestamp("2016-01-01")
        df.to_sql(table, eng, schema="source", if_exists="replace", index=False, chunksize=10000)
        log.info("loaded source.%s: %d rows", table, len(df))

    with eng.begin() as c:
        c.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_orders_updated_at ON source.orders(updated_at)"))
        c.execute(sa.text("UPDATE control.watermark SET last_watermark='1900-01-01', updated_at=now() WHERE table_name='orders'"))
    log.info("watermark reset to 1900-01-01")


if __name__ == "__main__":
    main()
