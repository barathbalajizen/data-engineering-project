"""Local filesystem storage for medallion layers."""

import os
from datetime import datetime
from pathlib import Path
from typing import Literal

import polars as pl
from deltalake.writer import write_deltalake

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

StorageLayer = Literal["bronze", "silver", "gold"]
StorageFormat = Literal["csv", "parquet", "delta"]
DEFAULT_LOCAL_DATA_LAKE_PATH = "C:/ecommerce_delta_lake" if os.name == "nt" else "./data/lake"


def write_local_layer(
    df: pl.DataFrame,
    layer: StorageLayer,
    source: str,
    base_path: str = DEFAULT_LOCAL_DATA_LAKE_PATH,
    file_format: StorageFormat = "delta",
) -> Path:
    """
    Write a DataFrame to a local medallion layer partitioned by source and date.

    Args:
        df: DataFrame to write.
        layer: Medallion layer name.
        source: Data source name.
        base_path: Root path for local data lake storage.
        file_format: Output file format. Use "delta" to write an appendable Delta table.

    Returns:
        Path to the written file or Delta table directory.
    """
    if df.is_empty():
        logger.warning(f"Skipping local {layer} write for empty DataFrame: {source}")
        return Path()

    if file_format == "delta":
        _validate_delta_table_path(base_path)
        output_path = Path(base_path) / layer / source
        output_path.parent.mkdir(parents=True, exist_ok=True)
        partition_by = ["_ingestion_date"] if "_ingestion_date" in df.columns else None
        write_deltalake(str(output_path), df.to_arrow(), mode="append", partition_by=partition_by)
    else:
        ingestion_date = _get_ingestion_date(df)
        output_dir = Path(base_path) / layer / source / f"date={ingestion_date}"
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
        output_path = output_dir / f"{source}_{timestamp}.{file_format}"

    if file_format == "csv":
        df.write_csv(output_path)
    elif file_format == "parquet":
        df.write_parquet(output_path)
    elif file_format == "delta":
        pass
    else:
        raise ValueError(f"Unsupported local storage format: {file_format}")

    logger.info(f"Wrote {len(df)} records to local {layer} storage: {output_path}")
    return output_path


def write_local_bronze(
    df: pl.DataFrame,
    source: str,
    base_path: str = DEFAULT_LOCAL_DATA_LAKE_PATH,
    file_format: StorageFormat = "delta",
) -> Path:
    """Write a DataFrame to the local Bronze layer."""
    return write_local_layer(df, "bronze", source, base_path, file_format)


def _get_ingestion_date(df: pl.DataFrame) -> str:
    if "_ingestion_date" in df.columns and len(df) > 0:
        return str(df.get_column("_ingestion_date")[0])

    return datetime.utcnow().date().isoformat()


def _validate_delta_table_path(base_path: str) -> None:
    if os.name == "nt" and " " in str(Path(base_path).resolve()):
        raise ValueError(
            "Delta Lake tables are not readable with deltalake 0.14.0 when the "
            "table path contains spaces on Windows. Set LOCAL_DATA_LAKE_PATH to "
            "a path without spaces, such as C:/ecommerce_delta_lake."
        )
