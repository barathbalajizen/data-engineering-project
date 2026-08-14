"""Unit tests for review sentiment and keyword extraction."""

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.extraction.review_sentiment import (
    enrich_reviews_with_signals,
    extract_review_signals,
)


def test_extract_review_signals_positive_review():
    """Test positive review sentiment extraction."""
    result = extract_review_signals("Excellent product, fast delivery, works perfectly")

    assert result["sentiment_label"] == "positive"
    assert result["sentiment_score"] > 0
    assert result["flagged_keywords"] == ""
    assert result["has_flagged_keywords"] is False


def test_extract_review_signals_empty_review_is_neutral():
    """Test empty review text does not fail and returns neutral signals."""
    result = extract_review_signals("")

    assert result["sentiment_label"] == "neutral"
    assert result["sentiment_score"] == 0.0
    assert result["flagged_keywords"] == ""
    assert result["has_flagged_keywords"] is False


def test_extract_review_signals_non_english_text_does_not_fail():
    """Test non-English review text is handled without errors."""
    result = extract_review_signals("Produto chegou rapido e funciona bem")

    assert result["sentiment_label"] in {"positive", "neutral", "negative"}
    assert isinstance(result["sentiment_score"], float)


def test_extract_review_signals_all_caps_issue_review():
    """Test all-caps angry reviews are negative and keyword flagged."""
    result = extract_review_signals("BROKEN AND LATE, I WANT A REFUND!!!")

    assert result["sentiment_label"] == "negative"
    assert result["sentiment_score"] < 0
    assert result["flagged_keywords"] == "broken,late,refund"
    assert result["has_flagged_keywords"] is True


def test_enrich_reviews_with_signals_adds_extraction_columns():
    """Test DataFrame enrichment adds sentiment and keyword columns."""
    df = pl.DataFrame({
        "review_id": ["REV001", "REV002"],
        "review_comment": [
            "Great product",
            "Wrong item arrived damaged",
        ],
    })

    result = enrich_reviews_with_signals(df)

    assert "sentiment_score" in result.columns
    assert "sentiment_label" in result.columns
    assert "flagged_keywords" in result.columns
    assert "has_flagged_keywords" in result.columns
    assert result.get_column("sentiment_label")[0] == "positive"
    assert result.get_column("has_flagged_keywords")[1] is True
    assert result.get_column("flagged_keywords")[1] == "damaged,wrong"


def test_enrich_reviews_with_signals_requires_text_column():
    """Test DataFrame enrichment fails clearly when review text is missing."""
    df = pl.DataFrame({"review_id": ["REV001"]})

    with pytest.raises(ValueError, match="Missing review text column"):
        enrich_reviews_with_signals(df)