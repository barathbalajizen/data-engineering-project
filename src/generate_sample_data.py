"""Generate synthetic Olist-style CSVs (same file names/columns as the Kaggle dataset).
Usage: python generate_sample_data.py [n_orders]
Skip this if you downloaded the real Olist dataset into data/raw/.
"""
import os
import sys

import numpy as np
import pandas as pd

from common import DATA, get_logger

log = get_logger("generate")
N = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
rng = np.random.default_rng(42)
os.makedirs(DATA, exist_ok=True)

CITIES = ["sao paulo", "rio de janeiro", "belo horizonte", "curitiba", "salvador", "recife", "brasilia", "porto alegre"]
STATES = ["SP", "RJ", "MG", "PR", "BA", "PE", "DF", "RS"]
CATS = ["health_beauty", "computers", "furniture", "toys", "sports_leisure", "housewares",
        "auto", "watches_gifts", "bed_bath_table", "garden_tools", "perfumery", "books", "pet_shop", "baby", "electronics"]
STATUS = ["delivered", "shipped", "processing", "canceled", "approved"]
STATUS_P = [0.90, 0.04, 0.02, 0.02, 0.02]

n_prod, n_sell = 2000, 200

customers = pd.DataFrame({"customer_id": [f"cus{i:08d}" for i in range(N)]})
customers["customer_unique_id"] = [f"uni{int(i * 0.8):08d}" for i in range(N)]
idx = rng.integers(0, len(CITIES), N)
customers["customer_city"] = [CITIES[i] for i in idx]
customers["customer_state"] = [STATES[i] for i in idx]

products = pd.DataFrame({
    "product_id": [f"prd{i:06d}" for i in range(n_prod)],
    "product_category_name": rng.choice(CATS, n_prod),
    "product_weight_g": rng.integers(50, 20000, n_prod),
})
products.loc[rng.choice(n_prod, 40, replace=False), "product_category_name"] = None  # some nulls

sellers = pd.DataFrame({"seller_id": [f"sel{i:05d}" for i in range(n_sell)]})
sidx = rng.integers(0, len(CITIES), n_sell)
sellers["seller_city"] = [CITIES[i] for i in sidx]
sellers["seller_state"] = [STATES[i] for i in sidx]

start, end = pd.Timestamp("2017-01-01"), pd.Timestamp("2018-08-31")
span = int((end - start).total_seconds())
purchase = start + pd.to_timedelta(rng.integers(0, span, N), unit="s")
status = rng.choice(STATUS, N, p=STATUS_P)
delivered = purchase + pd.to_timedelta(rng.integers(3, 26, N), unit="D")
estimated = purchase + pd.to_timedelta(rng.integers(10, 31, N), unit="D")
orders = pd.DataFrame({
    "order_id": [f"ord{i:08d}" for i in range(N)],
    "customer_id": customers["customer_id"],
    "order_status": status,
    "order_purchase_timestamp": purchase,
    "order_delivered_customer_date": pd.Series(delivered).where(status == "delivered"),
    "order_estimated_delivery_date": estimated,
})

n_items = rng.choice([1, 2, 3], N, p=[0.8, 0.15, 0.05])
rows = np.repeat(np.arange(N), n_items)
seq = np.concatenate([np.arange(1, k + 1) for k in n_items])
items = pd.DataFrame({
    "order_id": orders["order_id"].to_numpy()[rows],
    "order_item_id": seq,
    "product_id": products["product_id"].to_numpy()[rng.integers(0, n_prod, len(rows))],
    "seller_id": sellers["seller_id"].to_numpy()[rng.integers(0, n_sell, len(rows))],
    "price": np.round(rng.gamma(2.0, 45.0, len(rows)) + 5, 2),
    "freight_value": np.round(rng.uniform(5, 40, len(rows)), 2),
})

pay = items.groupby("order_id").agg(p=("price", "sum"), f=("freight_value", "sum")).reset_index()
payments = pd.DataFrame({
    "order_id": pay["order_id"],
    "payment_sequential": 1,
    "payment_type": rng.choice(["credit_card", "boleto", "voucher", "debit_card"], len(pay), p=[0.74, 0.19, 0.05, 0.02]),
    "payment_value": np.round(pay["p"] + pay["f"], 2),
})

files = {
    "olist_customers_dataset.csv": customers,
    "olist_products_dataset.csv": products,
    "olist_sellers_dataset.csv": sellers,
    "olist_orders_dataset.csv": orders,
    "olist_order_items_dataset.csv": items,
    "olist_order_payments_dataset.csv": payments,
}
for name, df in files.items():
    df.to_csv(os.path.join(DATA, name), index=False)
    log.info("wrote %s (%d rows)", name, len(df))
