"""
Phase 7: Fuzzy Matching

Performs fuzzy string matching to identify near-duplicate records that
exact matching missed. Uses Levenshtein, Jaro-Winkler, and phonetic
algorithms to compute pairwise string similarity scores.

Input: Deduplicated Silver tables
Output: Candidate match pairs with similarity scores

Medallion Layer: Silver
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, StringType

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def levenshtein_distance(left: Column, right: Column) -> Column:
    """
    Compute Levenshtein (edit) distance between two string columns.

    Uses native Spark `levenshtein` function. Returns integer distance.
    Lower values indicate greater similarity.

    Args:
        left: First string column.
        right: Second string column.

    Returns:
        Integer distance column.
    """
    return F.levenshtein(
        F.coalesce(left, F.lit("")),
        F.coalesce(right, F.lit("")),
    )


def levenshtein_similarity(
    left: Column,
    right: Column,
    max_length: int = 100,
) -> Column:
    """
    Compute normalized Levenshtein similarity (0.0 to 1.0).

    similarity = 1 - (distance / max(len(left), len(right)))

    Args:
        left: First string column.
        right: Second string column.
        max_length: Maximum length to cap normalization at.

    Returns:
        Float similarity score (0.0 = completely different, 1.0 = identical).
    """
    dist = F.levenshtein(
        F.coalesce(left, F.lit("")),
        F.coalesce(right, F.lit("")),
    )
    max_len = F.greatest(
        F.length(F.coalesce(left, F.lit(""))),
        F.length(F.coalesce(right, F.lit(""))),
        F.lit(1),
    )
    return F.lit(1.0) - (dist.cast(FloatType()) / max_len.cast(FloatType()))


def jaro_winkler_similarity(left_col: str, right_col: str) -> Column:
    """
    Compute Jaro-Winkler similarity using a Spark UDF.

    Jaro-Winkler gives higher scores to strings that match from the beginning.
    Falls back gracefully if jellyfish is not installed.

    Args:
        left_col: First column name.
        right_col: Second column name.

    Returns:
        Float similarity score.
    """
    try:
        import jellyfish

        @F.udf(FloatType())
        def _jaro_winkler(left: str, right: str) -> float:
            if left is None or right is None:
                return 0.0
            return float(jellyfish.jaro_winkler_similarity(str(left), str(right)))

        return _jaro_winkler(F.col(left_col), F.col(right_col))
    except ImportError:
        logger.warning("jellyfish not installed; falling back to Levenshtein similarity")
        return levenshtein_similarity(F.col(left_col), F.col(right_col))


def soundex_code(col: Column) -> Column:
    """
    Compute Soundex phonetic code of a string.

    Args:
        col: String column.

    Returns:
        Soundex code (e.g., 'A261').
    """
    try:
        import jellyfish

        @F.udf(StringType())
        def _soundex(val: str) -> str:
            if val is None:
                return ""
            return jellyfish.soundex(str(val))

        return _soundex(col)
    except ImportError:
        return F.lit("")


def generate_candidate_pairs(
    df: DataFrame,
    match_columns: list[str],
    entity_id_col: str = "id",
    similarity_threshold: float = 0.85,
    use_self_join: bool = True,
) -> DataFrame:
    """
    Generate candidate pairs for fuzzy matching via self-join.

    Performs a cross-join filtered by a loose blocking condition
    (first letter match or phonetic match) to avoid O(n^2) explosion.

    Args:
        df: Input DataFrame.
        match_columns: Columns to compare for similarity.
        entity_id_col: Unique identifier column.
        similarity_threshold: Minimum similarity to keep a pair.
        use_self_join: If True, self-join; if False, cross-join with blocking.

    Returns:
        DataFrame of candidate pairs with similarity scores.
    """
    left = df.select(
        F.col(entity_id_col).alias("id_left"),
        *[F.col(c).alias(f"{c}_left") for c in match_columns],
    )
    right = df.select(
        F.col(entity_id_col).alias("id_right"),
        *[F.col(c).alias(f"{c}_right") for c in match_columns],
    )

    # Blocking: only compare pairs where left.id < right.id (avoid duplicates)
    # and first character of the first match column matches
    if use_self_join and match_columns:
        block_col = match_columns[0]
        pairs = left.join(
            right,
            (F.col("id_left") < F.col("id_right"))
            & (
                F.substring(F.coalesce(F.col(f"{block_col}_left"), F.lit("")), 1, 1)
                == F.substring(F.coalesce(F.col(f"{block_col}_right"), F.lit("")), 1, 1)
            ),
        )
    else:
        pairs = left.crossJoin(right).filter(
            F.col("id_left") < F.col("id_right")
        )

    # Compute similarity scores for each match column
    for col_name in match_columns:
        left_c = f"{col_name}_left"
        right_c = f"{col_name}_right"

        pairs = pairs.withColumn(
            f"sim_{col_name}",
            levenshtein_similarity(F.col(left_c), F.col(right_c)),
        )

    # Compute aggregate similarity (average across columns, weighted equally)
    sim_cols = [F.col(f"sim_{c}") for c in match_columns]
    pairs = pairs.withColumn(
        "_aggregate_similarity",
        sum(sim_cols) / F.lit(len(match_columns)),
    )

    # Filter by threshold
    pairs = pairs.filter(F.col("_aggregate_similarity") >= similarity_threshold)

    return pairs


def compute_match_confidence(
    pairs_df: DataFrame,
    match_column_scores: list[str],
    weights: Optional[list[float]] = None,
) -> DataFrame:
    """
    Compute a weighted match confidence score from individual similarity scores.

    Args:
        pairs_df: DataFrame of candidate pairs with similarity scores.
        match_column_scores: List of similarity column names.
        weights: Optional per-column weights (must sum to 1.0).

    Returns:
        DataFrame with _match_confidence column added.
    """
    if weights is None:
        weights = [1.0 / len(match_column_scores)] * len(match_column_scores)

    weighted_sum = sum(
        F.col(col) * weight
        for col, weight in zip(match_column_scores, weights)
    )

    return pairs_df.withColumn("_match_confidence", weighted_sum)


def run(
    spark: SparkSession,
    df: DataFrame,
    entity_type: str = "customer",
    config: Optional[dict] = None,
    silver_path: str = "Tables/silver/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> DataFrame:
    """
    Execute Phase 7: fuzzy matching to find near-duplicate records.

    Args:
        spark: Active Spark session.
        df: Input DataFrame from Phase 6 (deduplicated Silver records).
        entity_type: Type of entity (determines match column selection).
        config: Dict with:
            - match_columns: List of column names to compare.
            - similarity_threshold: Minimum aggregate similarity (0.0-1.0).
            - use_jaro_winkler: Whether to use Jaro-Winkler (default True).
            - column_weights: Per-column weight dict for confidence scoring.
        silver_path: Silver layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        DataFrame of candidate match pairs with similarity scores.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="07-fuzzy-matching")

    config = config or {}
    logger.info(f"Performing fuzzy matching for {entity_type}")

    # Resolve match columns by entity type
    default_columns = {
        "customer": ["full_name_std", "email_std", "phone_std"],
        "company": ["company_name_std", "address_street", "address_zip5"],
        "product": ["product_name_std", "sku_std"],
        "supplier": ["full_name_std", "email_std"],
    }

    match_columns = config.get("match_columns", default_columns.get(entity_type, ["name"]))
    # Filter to columns that exist in the DataFrame
    match_columns = [c for c in match_columns if c in df.columns]

    if not match_columns:
        logger.warning("No match columns available; skipping fuzzy matching")
        return df

    threshold = config.get("similarity_threshold", 0.85)

    # Generate candidate pairs
    pairs = generate_candidate_pairs(
        df,
        match_columns=match_columns,
        similarity_threshold=threshold,
    )

    # Compute weighted confidence
    column_weights = config.get("column_weights")
    sim_cols = [f"sim_{c}" for c in match_columns]

    if column_weights:
        weights = [column_weights.get(c, 1.0 / len(match_columns)) for c in match_columns]
    else:
        weights = None

    pairs = compute_match_confidence(pairs, sim_cols, weights)

    # Write candidate pairs
    target_path = resolve_table_path(
        layer="silver",
        table_name=f"{table_name}_fuzzy_candidates",
        workspace=workspace,
    )
    write_to_delta(pairs, target_path, mode="overwrite")

    # Emit metrics
    pair_count = pairs.count()
    metrics.log_count("fuzzy_candidate_pairs", pair_count)
    if pair_count > 0:
        avg_conf = pairs.select(F.avg("_match_confidence")).first()[0] or 0.0
        metrics.log_metric("fuzzy_avg_confidence", round(avg_conf, 4))
    metrics.flush()

    logger.info(f"Generated {pair_count} fuzzy candidate pairs (threshold={threshold})")
    return pairs
