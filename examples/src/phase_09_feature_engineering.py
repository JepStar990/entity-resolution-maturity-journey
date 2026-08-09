"""
Phase 9: Feature Engineering

Constructs feature vectors from candidate record pairs for ML-based
matching. Computes similarity features across multiple dimensions:
string similarity, numeric proximity, date proximity, and categorical
equality. Prepares training and inference datasets.

Input: Candidate record pairs from blocked or fuzzy-matched Silver tables
Output: Feature vectors (DataFrame) ready for ML model training/inference

Medallion Layer: Gold
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, DoubleType, IntegerType

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def string_similarity_features(
    df: DataFrame,
    left_col: str,
    right_col: str,
    feature_prefix: str = "",
) -> list[tuple[str, F.Column]]:
    """
    Generate string similarity features for a pair of columns.

    Features generated:
    - exact_match: 1 if strings are identical, 0 otherwise.
    - levenshtein_similarity: Normalized edit distance similarity.
    - shared_prefix_len: Length of common prefix.
    - length_difference: Absolute difference in string lengths.
    - word_count_difference: Absolute difference in word counts.

    Args:
        df: DataFrame with left/right column pairs.
        left_col: Left-side column name.
        right_col: Right-side column name.
        feature_prefix: Prefix for feature names (e.g., "name_").

    Returns:
        List of (feature_name, Column expression) tuples.
    """
    left = F.coalesce(F.col(left_col), F.lit(""))
    right = F.coalesce(F.col(right_col), F.lit(""))

    pfx = feature_prefix

    features = [
        (
            f"{pfx}exact_match",
            F.when(left == right, F.lit(1.0)).otherwise(F.lit(0.0)).cast(FloatType()),
        ),
        (
            f"{pfx}levenshtein_sim",
            levenshtein_similarity_expr(left, right),
        ),
        (
            f"{pfx}shared_prefix_len",
            shared_prefix_length(left, right).cast(FloatType()),
        ),
        (
            f"{pfx}length_diff",
            F.abs(F.length(left) - F.length(right)).cast(FloatType()),
        ),
        (
            f"{pfx}word_count_diff",
            F.abs(
                F.size(F.split(F.trim(left), r"\s+"))
                - F.size(F.split(F.trim(right), r"\s+"))
            ).cast(FloatType()),
        ),
    ]
    return features


def levenshtein_similarity_expr(left: F.Column, right: F.Column) -> F.Column:
    """Compute normalized Levenshtein similarity as a Column expression."""
    dist = F.levenshtein(left, right)
    max_len = F.greatest(F.length(left), F.length(right), F.lit(1))
    return F.when(
        F.col(left) == F.col(right),
        F.lit(1.0),
    ).otherwise(
        (F.lit(1.0) - dist.cast(FloatType()) / max_len.cast(FloatType())).cast(FloatType())
    )


def shared_prefix_length(left: F.Column, right: F.Column) -> F.Column:
    """
    Compute the length of the common prefix between two strings.

    Uses a SQL expression to compare characters one by one up to 20 chars.
    """
    left_upper = F.upper(left)
    right_upper = F.upper(right)

    # Build a CASE statement for prefix lengths 1-20
    prefix_len = F.lit(0)
    for i in range(20, 0, -1):
        prefix_len = F.when(
            F.substring(left_upper, 1, i) == F.substring(right_upper, 1, i),
            F.lit(i),
        ).otherwise(prefix_len)

    return prefix_len


def numeric_similarity_features(
    df: DataFrame,
    left_col: str,
    right_col: str,
    feature_prefix: str = "",
) -> list[tuple[str, F.Column]]:
    """
    Generate numeric similarity features for a pair of columns.

    Features:
    - exact_match: 1 if equal, 0 otherwise.
    - absolute_difference: |left - right|.
    - relative_difference: |left - right| / max(|left|, |right|, 1).

    Args:
        df: DataFrame with left/right column pairs.
        left_col: Left-side column name.
        right_col: Right-side column name.
        feature_prefix: Prefix for feature names.

    Returns:
        List of (feature_name, Column expression) tuples.
    """
    pfx = feature_prefix
    left = F.col(left_col).cast(DoubleType())
    right = F.col(right_col).cast(DoubleType())

    abs_diff = F.abs(left - right)
    max_abs = F.greatest(F.abs(left), F.abs(right), F.lit(1.0))

    features = [
        (
            f"{pfx}exact_match",
            F.when(left == right, F.lit(1.0)).otherwise(F.lit(0.0)).cast(FloatType()),
        ),
        (
            f"{pfx}abs_diff",
            abs_diff.cast(FloatType()),
        ),
        (
            f"{pfx}rel_diff",
            (abs_diff / max_abs).cast(FloatType()),
        ),
    ]
    return features


def date_similarity_features(
    df: DataFrame,
    left_col: str,
    right_col: str,
    feature_prefix: str = "",
) -> list[tuple[str, F.Column]]:
    """
    Generate date similarity features.

    Features:
    - exact_match: 1 if dates are equal.
    - days_difference: Absolute difference in days.
    - same_year: 1 if same year, 0 otherwise.
    - same_month: 1 if same month, 0 otherwise.

    Args:
        df: DataFrame with left/right date column pairs.
        left_col: Left-side date column.
        right_col: Right-side date column.
        feature_prefix: Prefix for feature names.

    Returns:
        List of (feature_name, Column expression) tuples.
    """
    pfx = feature_prefix
    left_date = F.to_date(F.col(left_col))
    right_date = F.to_date(F.col(right_col))

    days_diff = F.abs(F.datediff(left_date, right_date))

    features = [
        (
            f"{pfx}exact_match",
            F.when(left_date == right_date, F.lit(1.0)).otherwise(F.lit(0.0)).cast(FloatType()),
        ),
        (
            f"{pfx}days_diff",
            days_diff.cast(FloatType()),
        ),
        (
            f"{pfx}same_year",
            F.when(F.year(left_date) == F.year(right_date), F.lit(1.0)).otherwise(F.lit(0.0)).cast(FloatType()),
        ),
        (
            f"{pfx}same_month",
            F.when(
                (F.year(left_date) == F.year(right_date))
                & (F.month(left_date) == F.month(right_date)),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)).cast(FloatType()),
        ),
    ]
    return features


def categorical_equality_features(
    df: DataFrame,
    left_col: str,
    right_col: str,
    feature_prefix: str = "",
) -> list[tuple[str, F.Column]]:
    """
    Generate categorical equality features.

    Args:
        df: DataFrame with left/right categorical column pairs.
        left_col: Left-side column.
        right_col: Right-side column.
        feature_prefix: Prefix for feature names.

    Returns:
        List with one feature: exact match.
    """
    pfx = feature_prefix
    return [
        (
            f"{pfx}exact_match",
            F.when(
                F.col(left_col).cast("string") == F.col(right_col).cast("string"),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)).cast(FloatType()),
        ),
    ]


def build_feature_vector(
    df: DataFrame,
    feature_config: list[dict],
    pair_id_col: str = "id_left",
) -> DataFrame:
    """
    Build a complete feature vector from a feature configuration.

    Args:
        df: DataFrame with candidate record pairs.
        feature_config: List of feature definitions, each with:
            - type: 'string', 'numeric', 'date', or 'categorical'.
            - left_col: Left-side column name.
            - right_col: Right-side column name.
            - prefix: Feature name prefix (optional).
        pair_id_col: Column identifying the record pair.

    Returns:
        DataFrame with all features as columns, plus pair identifiers.
    """
    result = df
    feature_cols: list[str] = []

    for fc in feature_config:
        ftype = fc["type"]
        left_c = fc["left_col"]
        right_c = fc["right_col"]
        prefix = fc.get("prefix", f"{left_c}_")

        if left_c not in df.columns or right_c not in df.columns:
            continue

        if ftype == "string":
            feats = string_similarity_features(df, left_c, right_c, prefix)
        elif ftype == "numeric":
            feats = numeric_similarity_features(df, left_c, right_c, prefix)
        elif ftype == "date":
            feats = date_similarity_features(df, left_c, right_c, prefix)
        elif ftype == "categorical":
            feats = categorical_equality_features(df, left_c, right_c, prefix)
        else:
            logger.warning(f"Unknown feature type: {ftype}; skipping")
            continue

        for feat_name, feat_expr in feats:
            result = result.withColumn(feat_name, feat_expr)
            feature_cols.append(feat_name)

    # Select final columns: identifiers + features
    id_cols = [pair_id_col, "id_right", "_aggregate_similarity"]
    id_cols = [c for c in id_cols if c in result.columns]
    final_cols = id_cols + feature_cols
    result = result.select(*final_cols)

    logger.info(f"Built feature vector with {len(feature_cols)} features")
    return result


def compute_feature_correlations(
    spark: SparkSession,
    df: DataFrame,
    feature_cols: list[str],
) -> DataFrame:
    """
    Compute pairwise correlations between features.

    Useful for identifying redundant features and debugging model behavior.

    Args:
        spark: Active Spark session.
        df: Feature vector DataFrame.
        feature_cols: List of feature column names.

    Returns:
        DataFrame with correlation matrix entries.
    """
    rows = []
    numeric_cols = [
        c for c in feature_cols
        if c in df.columns and dict(df.dtypes).get(c, "") in ("float", "double", "int")
    ]

    for c1 in numeric_cols:
        for c2 in numeric_cols:
            corr_val = df.stat.corr(c1, c2)
            rows.append({
                "feature_a": c1,
                "feature_b": c2,
                "correlation": corr_val or 0.0,
            })

    return spark.createDataFrame(rows)


def run(
    spark: SparkSession,
    pairs_df: DataFrame,
    entity_type: str = "customer",
    config: Optional[dict] = None,
    gold_path: str = "Tables/gold/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> DataFrame:
    """
    Execute Phase 9: build feature vectors from candidate pairs.

    Args:
        spark: Active Spark session.
        pairs_df: DataFrame of candidate record pairs (from Phase 7 or 8).
        entity_type: Type of entity for feature config selection.
        config: Dict with:
            - feature_config: List of feature definitions.
            - label_col: Column with ground-truth labels (if available).
        gold_path: Gold layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        DataFrame with feature vectors.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="09-feature-engineering")

    config = config or {}
    logger.info(f"Building feature vectors for {entity_type}")

    # Default feature configs by entity type
    default_configs = {
        "customer": [
            {"type": "string", "left_col": "full_name_std_left", "right_col": "full_name_std_right", "prefix": "name_"},
            {"type": "string", "left_col": "email_std_left", "right_col": "email_std_right", "prefix": "email_"},
            {"type": "string", "left_col": "address_street_left", "right_col": "address_street_right", "prefix": "street_"},
            {"type": "categorical", "left_col": "address_state_left", "right_col": "address_state_right", "prefix": "state_"},
            {"type": "categorical", "left_col": "address_zip5_left", "right_col": "address_zip5_right", "prefix": "zip_"},
            {"type": "date", "left_col": "created_date_left", "right_col": "created_date_right", "prefix": "created_"},
        ],
        "company": [
            {"type": "string", "left_col": "company_name_std_left", "right_col": "company_name_std_right", "prefix": "name_"},
            {"type": "string", "left_col": "address_street_left", "right_col": "address_street_right", "prefix": "street_"},
            {"type": "categorical", "left_col": "address_state_left", "right_col": "address_state_right", "prefix": "state_"},
            {"type": "categorical", "left_col": "sic_code_left", "right_col": "sic_code_right", "prefix": "sic_"},
        ],
        "product": [
            {"type": "string", "left_col": "product_name_std_left", "right_col": "product_name_std_right", "prefix": "name_"},
            {"type": "categorical", "left_col": "sku_std_left", "right_col": "sku_std_right", "prefix": "sku_"},
        ],
    }

    feature_config = config.get(
        "feature_config",
        default_configs.get(entity_type, default_configs["customer"]),
    )

    # Build features
    features_df = build_feature_vector(pairs_df, feature_config)

    # Write feature vectors to Gold
    target_path = resolve_table_path(
        layer="gold",
        table_name=f"{table_name}_features",
        workspace=workspace,
    )
    write_to_delta(features_df, target_path, mode="overwrite")

    # Emit metrics
    feature_cols = [c for c in features_df.columns if c not in ("id_left", "id_right", "_aggregate_similarity")]
    pair_count = features_df.count()
    metrics.log_count("feature_vector_count", pair_count)
    metrics.log_count("feature_count", len(feature_cols))
    metrics.flush()

    logger.info(
        f"Built {len(feature_cols)} features for {pair_count} candidate pairs"
    )
    return features_df
