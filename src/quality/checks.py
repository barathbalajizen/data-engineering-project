"""Data quality validation schemas for Silver layer using Polars."""

import polars as pl


def _to_polars(df):
    """Ensure a Polars DataFrame is provided."""
    if isinstance(df, pl.DataFrame):
        return df
    raise TypeError("Expected a polars.DataFrame")


def validate_silver_orders(df):
    """Validate Silver orders DataFrame using Polars schema."""
    df_pl = _to_polars(df)
    
    errors = []

    # Check required columns
    required_cols = {"order_id", "customer_id", "order_date", "total_price", "state"}
    missing = required_cols - set(df_pl.columns)
    if missing:
        errors.append(f"Missing columns: {missing}")

    # Check no null prices
    if df_pl["total_price"].null_count() > 0:
        errors.append("Null prices found")

    # Check no negative prices
    if (df_pl["total_price"] < 0).sum() > 0:
        errors.append("Negative prices found")

    # Check unique order IDs
    if df_pl["order_id"].n_unique() < len(df_pl):
        errors.append("Duplicate order IDs found")

    if errors:
        raise ValueError("; ".join(errors))

    return df


def validate_silver_reviews(df):
    """Validate Silver reviews DataFrame using Polars schema."""
    df_pl = _to_polars(df)
    
    errors = []

    # Check required columns
    required_cols = {
        "review_id",
        "order_id",
        "product_id",
        "sentiment_score",
        "sentiment_label",
    }
    missing = required_cols - set(df_pl.columns)
    if missing:
        errors.append(f"Missing columns: {missing}")

    # Check sentiment score range
    if "sentiment_score" in df_pl.columns:
        out_of_range = df_pl.filter(
            (pl.col("sentiment_score") < -1.0) | (pl.col("sentiment_score") > 1.0)
        )
        if len(out_of_range) > 0:
            errors.append("Sentiment scores out of range [-1.0, 1.0]")

    # Check sentiment label values
    if "sentiment_label" in df_pl.columns:
        valid_labels = {"positive", "neutral", "negative"}
        labels_set = set(df_pl["sentiment_label"].unique().to_list())
        invalid = labels_set - valid_labels - {None}
        if invalid:
            errors.append(f"Invalid sentiment labels: {invalid}")

    # Check unique review IDs
    if df_pl["review_id"].n_unique() < len(df_pl):
        errors.append("Duplicate review IDs found")

    if errors:
        raise ValueError("; ".join(errors))

    return df


def validate_silver_products(df):
    """Validate Silver products DataFrame using Polars schema."""
    df_pl = _to_polars(df)
    
    errors = []

    # Check required columns
    required_cols = {"product_id", "product_name", "product_price"}
    missing = required_cols - set(df_pl.columns)
    if missing:
        errors.append(f"Missing columns: {missing}")

    # Check no negative prices
    if "product_price" in df_pl.columns:
        if (df_pl["product_price"] < 0).sum() > 0:
            errors.append("Negative prices found")

    # Check unique product IDs
    if df_pl["product_id"].n_unique() < len(df_pl):
        errors.append("Duplicate product IDs found")

    if errors:
        raise ValueError("; ".join(errors))

    return df


def validate_silver_orders_reviews(df):
    """Validate Silver orders+reviews joined DataFrame using Polars schema."""
    df_pl = _to_polars(df)
    
    errors = []

    # Check required columns
    required_cols = {"order_id", "total_price"}
    missing = required_cols - set(df_pl.columns)
    if missing:
        errors.append(f"Missing columns: {missing}")

    # Check no null order IDs
    if "order_id" in df_pl.columns:
        if df_pl["order_id"].null_count() > 0:
            errors.append("Null order IDs found")

    # Check no negative prices
    if "total_price" in df_pl.columns:
        if (df_pl["total_price"] < 0).sum() > 0:
            errors.append("Negative prices found")

    # Check sentiment labels if present
    if "sentiment_label" in df_pl.columns:
        valid_labels = {"positive", "neutral", "negative"}
        labels_set = set(df_pl["sentiment_label"].unique().to_list())
        invalid = labels_set - valid_labels - {None}
        if invalid:
            errors.append(f"Invalid sentiment labels: {invalid}")

    if errors:
        raise ValueError("; ".join(errors))

    return df
