"""Phase 2: Extract sentiment and keyword signals from reviews."""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import polars as pl

from src.extraction.review_sentiment import enrich_reviews_with_signals
from src.utils.logger import setup_logger
from src.storage.local_storage import DEFAULT_LOCAL_DATA_LAKE_PATH

logger = setup_logger(__name__)


def enrich_bronze_reviews_with_sentiment(
    base_path: str = DEFAULT_LOCAL_DATA_LAKE_PATH,
) -> dict:
    """
    Read Bronze reviews, enrich with sentiment signals, write back to Bronze.
    
    Args:
        base_path: Root path for data lake
        
    Returns:
        Dictionary with enrichment stats
    """
    logger.info("Starting Phase 2: Review sentiment extraction")
    
    # Read Bronze reviews
    bronze_path = Path(base_path) / "bronze" / "reviews"
    logger.info(f"Reading Bronze reviews from {bronze_path}")
    
    try:
        table = DeltaTable(str(bronze_path))
        df = pl.from_arrow(table.to_pyarrow_table())
        logger.info(f"Read {len(df)} reviews from Bronze")
    except Exception as e:
        logger.error(f"Failed to read Bronze reviews: {str(e)}")
        raise
    
    # Enrich with sentiment signals
    logger.info("Enriching reviews with sentiment signals...")
    enriched_df = enrich_reviews_with_signals(df, text_column="review_comment")
    logger.info(f"Enriched {len(enriched_df)} reviews")
    
    # Count sentiment distribution
    sentiment_counts_df = enriched_df.group_by("sentiment_label").count()
    sentiment_counts = {
        row["sentiment_label"]: int(row["count"]) for row in sentiment_counts_df.iter_rows(named=True)
    }
    flagged_count = int(enriched_df.get_column("has_flagged_keywords").cast(pl.Int64).sum())
    logger.info(f"Sentiment distribution: {sentiment_counts}")
    logger.info(f"Flagged reviews with keywords: {flagged_count}/{len(enriched_df)}")
    
    # Delete existing Bronze reviews table to avoid schema conflicts
    import shutil
    if bronze_path.exists():
        logger.info(f"Deleting existing Bronze reviews table: {bronze_path}")
        shutil.rmtree(bronze_path)
    
    # Write enriched reviews to Bronze with new schema
    logger.info("Writing enriched reviews to Bronze...")
    write_deltalake(
        str(bronze_path),
        enriched_df.to_arrow(),
        mode="append",
        partition_by=["_ingestion_date"],
    )
    logger.info(f"Wrote {len(enriched_df)} enriched reviews to Bronze")
    
    return {
        "total_reviews": len(enriched_df),
        "sentiment_distribution": sentiment_counts,
        "flagged_reviews": int(flagged_count),
        "bronze_path": str(bronze_path),
    }


if __name__ == "__main__":
    local_lake_path = os.getenv("LOCAL_DATA_LAKE_PATH", DEFAULT_LOCAL_DATA_LAKE_PATH)
    
    try:
        stats = enrich_bronze_reviews_with_sentiment(local_lake_path)
        
        print("\n" + "="*50)
        print("Phase 2 Review Sentiment Extraction Complete!")
        print("="*50)
        print(f"Total reviews enriched: {stats['total_reviews']}")
        print(f"Sentiment distribution: {stats['sentiment_distribution']}")
        print(f"Flagged reviews: {stats['flagged_reviews']}")
        print(f"Bronze path: {stats['bronze_path']}")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"Phase 2 failed: {str(e)}")
        raise
