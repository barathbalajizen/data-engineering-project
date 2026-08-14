"""Unit tests for local storage helpers."""

import sys
from datetime import date
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.storage.local_storage import write_local_bronze


def test_write_local_bronze_creates_delta_table_by_default(tmp_path):
    """Test local Bronze writes an appendable Delta table by default."""
    df = pl.DataFrame({
        "order_id": ["ORD001"],
        "_ingestion_date": [date(2024, 1, 15)],
    })

    output_path = write_local_bronze(
        df,
        source="orders",
        base_path=str(tmp_path),
    )

    assert output_path.exists()
    assert output_path == tmp_path / "bronze" / "orders"
    assert (output_path / "_delta_log").exists()

    written_df = pl.from_arrow(DeltaTable(str(output_path)).to_pyarrow_table())
    assert written_df.get_column("order_id")[0] == "ORD001"