"""Silver Layer Pipeline: Orchestrates Bronze to Silver transformation."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.transform.bronze_to_silver import transform_bronze_to_silver
from src.utils.logger import setup_logger
from src.storage.local_storage import DEFAULT_LOCAL_DATA_LAKE_PATH

logger = setup_logger(__name__)


if __name__ == "__main__":
    local_lake_path = os.getenv("LOCAL_DATA_LAKE_PATH", DEFAULT_LOCAL_DATA_LAKE_PATH)

    try:
        print("\n" + "="*60)
        print("SILVER LAYER PIPELINE: BRONZE TO SILVER TRANSFORMATION")
        print("="*60)

        result = transform_bronze_to_silver(base_path=local_lake_path, validate=True)

        print("\nSilver Layer Pipeline Complete!")
        print("="*60)
        print("Silver tables created:")
        for table_name, path in result.items():
            print(f"  - {table_name}: {path}")
        print("="*60 + "\n")

    except Exception as error:
        logger.error(f"Silver layer pipeline failed: {str(error)}")
        raise
