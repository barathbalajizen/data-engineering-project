"""Simulate a day of source-system activity so incremental loads + SCD2 have something to catch:
  - 200 open orders become 'delivered' (updates)
  - 100 customers move to a new city (SCD2 change)
  - 500 brand-new orders arrive (inserts), 100 of them from customers who moved
"""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import sqlalchemy as sa

from common import get_engine, get_logger

log = get_logger("simulate")
eng = get_engine()
now = datetime.utcnow().replace(microsecond=0)
rng = np.random.default_rng()
tag = int(now.timestamp())

with eng.begin() as c:
    r1 = c.execute(sa.text("""
        UPDATE source.orders SET order_status='delivered', updated_at=:now
        WHERE order_id IN (SELECT order_id FROM source.orders
                           WHERE order_status IN ('shipped','processing','approved')
                           ORDER BY random() LIMIT 200)"""), {"now": now})
    moved = [r[0] for r in c.execute(sa.text("""
        UPDATE source.customers SET customer_city='relocated city', customer_state='XX', updated_at=:now
        WHERE customer_id IN (SELECT customer_id FROM source.customers ORDER BY random() LIMIT 100)
        RETURNING customer_id"""), {"now": now})]
    others = [r[0] for r in c.execute(sa.text("SELECT customer_id FROM source.customers ORDER BY random() LIMIT 400"))]
    products = [r[0] for r in c.execute(sa.text("SELECT product_id FROM source.products LIMIT 500"))]
    sellers = [r[0] for r in c.execute(sa.text("SELECT seller_id FROM source.sellers LIMIT 100"))]

log.info("updated %d orders, moved %d customers", r1.rowcount, len(moved))

cust = (moved[:100] + others)[:500]
n = len(cust)
purchase = [now + timedelta(minutes=int(m)) for m in rng.integers(1, 120, n)]
ids = [f"new{tag}_{i:04d}" for i in range(n)]
orders = pd.DataFrame({
    "order_id": ids, "customer_id": cust, "order_status": "delivered",
    "order_purchase_timestamp": purchase,
    "order_delivered_customer_date": [p + timedelta(days=int(d)) for p, d in zip(purchase, rng.integers(3, 20, n))],
    "order_estimated_delivery_date": [p + timedelta(days=int(d)) for p, d in zip(purchase, rng.integers(10, 30, n))],
    "updated_at": now,
})
items = pd.DataFrame({
    "order_id": ids, "order_item_id": 1,
    "product_id": rng.choice(products, n), "seller_id": rng.choice(sellers, n),
    "price": np.round(rng.gamma(2.0, 45.0, n) + 5, 2), "freight_value": np.round(rng.uniform(5, 40, n), 2),
})
pay = pd.DataFrame({
    "order_id": ids, "payment_sequential": 1, "payment_type": "credit_card",
    "payment_value": np.round(items["price"] + items["freight_value"], 2),
})
orders.to_sql("orders", eng, schema="source", if_exists="append", index=False)
items.to_sql("order_items", eng, schema="source", if_exists="append", index=False)
pay.to_sql("order_payments", eng, schema="source", if_exists="append", index=False)
log.info("inserted %d new orders. Now re-run the pipeline.", n)
