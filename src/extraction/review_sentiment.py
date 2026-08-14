"""Extract sentiment and flagged issue keywords from customer reviews."""

import re
import math
from typing import TypedDict

import polars as pl
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class ReviewSignals(TypedDict):
    sentiment_score: float
    sentiment_label: str
    flagged_keywords: str
    has_flagged_keywords: bool


FLAGGED_KEYWORDS = (
    "broken",
    "damaged",
    "defective",
    "late",
    "delayed",
    "delay",
    "refund",
    "missing",
    "wrong",
    "poor",
    "bad",
    "terrible",
    "awful",
    "not working",
)

POSITIVE_THRESHOLD = 0.05
NEGATIVE_THRESHOLD = -0.05

_ANALYZER = SentimentIntensityAnalyzer()


def extract_review_signals(review_text: object) -> ReviewSignals:
    """Extract sentiment score, label, and issue keywords from one review."""
    text = _normalize_review_text(review_text)
    sentiment_score = round(_ANALYZER.polarity_scores(text)["compound"], 4) if text else 0.0
    flagged_keywords = _extract_flagged_keywords(text)

    return {
        "sentiment_score": sentiment_score,
        "sentiment_label": _label_sentiment(sentiment_score),
        "flagged_keywords": ",".join(flagged_keywords),
        "has_flagged_keywords": bool(flagged_keywords),
    }


def enrich_reviews_with_signals(
    df: pl.DataFrame,
    text_column: str = "review_comment",
) -> pl.DataFrame:
    """Add sentiment and keyword extraction columns to a reviews DataFrame."""
    if text_column not in df.columns:
        raise ValueError(f"Missing review text column: {text_column}")

    signals = [extract_review_signals(value) for value in df.get_column(text_column).to_list()]
    result = df.hstack(pl.DataFrame(signals))

    logger.info(f"Extracted review signals for {len(result)} reviews")
    return result


def _normalize_review_text(review_text: object) -> str:
    if review_text is None:
        return ""

    if isinstance(review_text, float) and math.isnan(review_text):
        return ""

    return str(review_text).strip()


def _label_sentiment(sentiment_score: float) -> str:
    if sentiment_score >= POSITIVE_THRESHOLD:
        return "positive"
    if sentiment_score <= NEGATIVE_THRESHOLD:
        return "negative"
    return "neutral"


def _extract_flagged_keywords(text: str) -> list[str]:
    lowered_text = text.lower()
    matches = []

    for keyword in FLAGGED_KEYWORDS:
        pattern = rf"\b{re.escape(keyword)}\b"
        if re.search(pattern, lowered_text):
            matches.append(keyword)

    return matches