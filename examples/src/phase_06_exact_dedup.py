"""
Phase 6: Exact Deduplication

Removes exact duplicate records using key-based matching and window functions.
Implements multi-strategy dedup: composite key matching, hash-based matching,
and configurable keep strategies (first, last, best-quality).

Input: Enriched Silver tables
Output: Deduplicated Silver tables with _dedup_key and _duplicate_rank

Medallion Layer: Silver
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def generate_dedup_key(
    df: DataFrame,
    key_columns: list[str],
    key_name: str = "_dedup_key",
) -> DataFrame:
    """
    Generate a deterministic deduplication key from one or more columns.

    Uses MD5 hash for compact, deterministic keys. Null values in key
    columns are coalesced to empty string to enable key generation.

    Args:
        df: Input DataFrame.
        key_columns: List of column names to include in the dedup key.
        key_name: Name for the generated key column.

    Returns:
        DataFrame with dedup key column added.
    """
    coalesced_cols = [
        F.coalesce(F.col(c).cast("string"), F.lit(""))
        for c in key_columns
    ]
    # Concatenate with delimiter to avoid cross-column collisions
    concat_expr = F.concat_ws("|", *coalesced_cols)
    return df.withColumn(key_name, F.md5(concat_expr))


def deduplicate_exact(
    df: DataFrame,
    dedup_key_col: str = "_dedup_key",
    keep_strategy: str = "first",
    order_by_col: Optional[str] = None,
    quality_col: Optional[str] = None,
) -> DataFrame:
    """
    Remove exact duplicates using a dedup key and a keep strategy.

    Args:
        df: DataFrame with dedup key column.
        dedup_key_col: Column to use for identifying duplicates.
        keep_strategy: Which record to keep:
            - 'first': Keep the first occurrence (deterministic with ordering).
            - 'last': Keep the last occurrence.
            - 'best_quality': Keep the record with the highest quality score.
            - 'most_recent': Keep the most recently updated record.
        order_by_col: Column for deterministic ordering (for first/last).
        quality_col: Column for best_quality strategy.
    Returns:
        DataFrame with _duplicate_rank and _is_duplicate columns; deduplicated.
    """
    valid_strategies = {"first", "last", "best_quality", "most_recent"}
    if keep_strategy not in valid_strategies:
        raise ValueError(
            f"Invalid keep strategy: {keep_strategy}. Choose from {valid_strategies}"
        )

    # Build window spec: partition by dedup key, order by strategy
    if keep_strategy == "best_quality" and quality_col:
        order_col = F.col(quality_col).desc_nulls_last()
    elif keep_strategy == "most_recent":
        order_col = F.col(order_by_col or "updated_at").desc_nulls_last()
    elif keep_strategy == "last":
        order_col = F.col(order_by_col or "_ingested_at").desc_nulls_last()
    else:  # first
        order_col = F.col(order_by_col or "_ingested_at").asc_nulls_last()

    window_spec = Window.partitionBy(dedup_key_col).orderBy(order_col)

    ranked = df.withColumn("_duplicate_rank", F.row_number().over(window_spec))
    ranked = ranked.withColumn(
        "_is_duplicate",
        F.when(F.col("_duplicate_rank") > 1, F.lit(True)).otherwise(F.lit(False)),
    )

    # Keep only rank 1 records
    deduplicated = ranked.filter(F.col("_duplicate_rank") == 1)

    return deduplicated


def deduplicate_composite(
    df: DataFrame,
    key_sets: list[list[str]],
    keep_strategy: str = "first",
) -> DataFrame:
    """
    Multi-pass deduplication using increasingly strict key sets.

    Pass 1: Dedup on strict keys (e.g., tax_id, email).
    Pass 2: Dedup remaining on medium keys (e.g., name + phone).
    Pass 3: Dedup remaining on loose keys (e.g., name + zip).

    Args:
        df: Input DataFrame.
        key_sets: Ordered list of key column lists (strictest first).
        keep_strategy: Keep strategy for each pass.

    Returns:
        Deduplicated DataFrame.
    """
    result = df
    for i, key_set in enumerate(key_sets):
        pass_name = f"_dedup_pass_{i + 1}"
        result = generate_dedup_key(result, key_set, key_name=pass_name)
        result = deduplicate_exact(
            result,
            dedup_key_col=pass_name,
            keep_strategy=keep_strategy,
        )

    # Keep only the final keep records and drop intermediate keys
    final_cols = [
        c for c in result.columns
        if not c.startswith("_dedup_pass_")
    ]
    return result.select(*final_cols)


def compute_dedup_metrics(
    df_before: DataFrame,
    df_after: DataFrame,
) -> dict:
    """
    Compute deduplication effectiveness metrics.

    Args:
        df_before: DataFrame before deduplication.
        df_after: DataFrame after deduplication.

    Returns:
        Dict with dedup metrics.
    """
    count_before = df_before.count()
    count_after = df_after.count()
    duplicates_removed = count_before - count_after
    dedup_rate = (duplicates_removed / count_before * 100) if count_before > 0 else 0.0

    return {
        "records_before": count_before,
        "records_after": count_after,
        "duplicates_removed": duplicates_removed,
        "dedup_rate_pct": round(dedup_rate, 2),
    }


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
    Execute Phase 6: remove exact duplicates.

    Args:
        spark: Active Spark session.
        df: Input DataFrame from Phase 5 (enriched Silver records).
        entity_type: Type of entity for key selection.
        config: Dict with:
            - dedup_keys: List of columns for single-pass dedup.
            - key_sets: List of column lists for multi-pass dedup.
            - keep_strategy: How to choose survivor among duplicates.
        silver_path: Silver layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        Deduplicated DataFrame.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="06-exact-dedup")

    config = config or {}
    logger.info(f"Executing exact deduplication for {entity_type}")

    count_before = df.count()

    keep_strategy = config.get("keep_strategy", "first")
    quality_col = config.get("quality_col", "_quality_score")

    # Choose dedup strategy based on config
    key_sets = config.get("key_sets")
    dedup_keys = config.get("dedup_keys")

    if key_sets:
        result = deduplicate_composite(df, key_sets, keep_strategy)
    elif dedup_keys:
        result = generate_dedup_key(df, dedup_keys)
        result = deduplicate_exact(
            result,
            keep_strategy=keep_strategy,
            quality_col=quality_col,
        )
    else:
        # Default: dedup on all non-metadata columns
        metadata_cols = {
            "_ingested_at", "_source_system", "_batch_id", "_source_file",
            "_schema_valid", "_schema_errors",
        }
        data_cols = [c for c in df.columns if c not in metadata_cols]
        result = generate_dedup_key(df, data_cols[:10])  # Cap at 10 columns
        result = deduplicate_exact(result, keep_strategy=keep_strategy)

    # Write deduplicated data
    target_path = resolve_table_path(
        layer="silver",
        table_name=f"{table_name}_dedup",
        workspace=workspace,
    )
    write_to_delta(result, target_path, mode="overwrite")

    # Emit metrics
    dedup_metrics = compute_dedup_metrics(df, result)
    for key, value in dedup_metrics.items():
        if isinstance(value, int):
            metrics.log_count(key, value)
        else:
            metrics.log_metric(key, value)
    metrics.flush()

    logger.info(
        f"Dedup: {dedup_metrics['duplicates_removed']} duplicates removed "
        f"({dedup_metrics['dedup_rate_pct']}% dedup rate)"
    )
    return result
