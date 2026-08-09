"""
Phase 8: Record Blocking

Implements record blocking strategies to scale entity matching to millions
of records. Blocks partition the search space so that only records within
the same block are compared, reducing pairwise comparisons from O(n^2) to
O(b * (n/b)^2) where b = number of blocks.

Input: Deduplicated Silver tables
Output: Blocked record groups ready for pairwise comparison

Medallion Layer: Silver
"""

from __future__ import annotations

from typing import Optional, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def sorted_neighborhood_blocking(
    df: DataFrame,
    sort_key: str,
    window_size: int = 100,
    block_key_col: str = "_block_key",
) -> DataFrame:
    """
    Sorted Neighborhood Method (SNM) blocking.

    Sorts records by a key, then slides a window of size `window_size`
    over the sorted list. Only records within the same window are
    considered potential matches.

    This reduces comparisons from O(n^2) to O(n * w) where w = window_size.

    Args:
        df: Input DataFrame.
        sort_key: Column to sort by for windowing.
        window_size: Number of records per comparison window.
        block_key_col: Name for the generated block key column.

    Returns:
        DataFrame with block key assigned (each record may appear in 1-2 blocks).
    """
    # Sort and assign row number
    window_spec = F.orderBy(sort_key)
    ranked = df.withColumn("_row_num", F.row_number().over(window_spec))

    # Assign block key = row_num // window_size (integer division)
    blocked = ranked.withColumn(
        block_key_col,
        (F.col("_row_num") / F.lit(window_size)).cast("int"),
    )

    # Create sliding overlap: each record also belongs to the next block
    # This ensures records near block boundaries are compared
    overlap = ranked.withColumn(
        block_key_col,
        ((F.col("_row_num") - F.lit(window_size // 2)) / F.lit(window_size)).cast("int"),
    )

    # Union base blocking with overlap
    all_blocks = blocked.select(*df.columns, block_key_col).union(
        overlap.select(*df.columns, block_key_col)
    ).filter(F.col(block_key_col) >= 0)

    return all_blocks


def canopy_clustering_blocking(
    df: DataFrame,
    cluster_key_columns: list[str],
    loose_threshold: float = 0.5,
    block_key_col: str = "_block_key",
) -> DataFrame:
    """
    Canopy clustering blocking.

    Creates overlapping clusters based on cheap similarity measures.
    Records within the same canopy are candidates for expensive matching.

    Algorithm:
    1. Pick a random record as a canopy center.
    2. Assign all records within loose_threshold to that canopy.
    3. Records within a tight threshold are removed from consideration.
    4. Repeat until all records are assigned.

    This implementation uses a simplified approach with string prefix
    as a proxy for cheap similarity.

    Args:
        df: Input DataFrame.
        cluster_key_columns: Columns to use for canopy assignment.
        loose_threshold: Not used in simplified version (placeholder).
        block_key_col: Name for the generated block key column.

    Returns:
        DataFrame with block key assigned.
    """
    # Use n-gram prefix of the first cluster key as a cheap blocking proxy
    primary_key = cluster_key_columns[0]

    # Multi-length prefixes for overlapping canopies
    blocked = df

    # Prefix length 3 block
    blocked = blocked.withColumn(
        f"{block_key_col}_p3",
        F.substring(
            F.coalesce(F.upper(F.col(primary_key)), F.lit("")),
            1,
            3,
        ),
    )
    # Prefix length 5 block
    blocked = blocked.withColumn(
        f"{block_key_col}_p5",
        F.substring(
            F.coalesce(F.upper(F.col(primary_key)), F.lit("")),
            1,
            5,
        ),
    )

    # Combine prefix blocks into a single array for explosion
    blocked = blocked.withColumn(
        block_key_col,
        F.array(F.col(f"{block_key_col}_p3"), F.col(f"{block_key_col}_p5")),
    )

    # Explode: each record belongs to both its p3 and p5 block
    blocked = blocked.withColumn(
        block_key_col,
        F.explode(F.col(block_key_col)),
    )

    # Drop intermediate columns
    blocked = blocked.drop(f"{block_key_col}_p3", f"{block_key_col}_p5")

    return blocked


def standard_blocking(
    df: DataFrame,
    block_key_columns: list[str],
    block_key_col: str = "_block_key",
) -> DataFrame:
    """
    Standard blocking: group records by exact match on blocking key columns.

    Generate a composite block key from one or more columns (e.g., zip code,
    state, first letter of name). Records with the same block key are
    compared pairwise.

    Args:
        df: Input DataFrame.
        block_key_columns: Columns to hash into the block key.
        block_key_col: Name for the generated block key column.

    Returns:
        DataFrame with block key assigned.
    """
    # Generate a composite block key by concatenating and hashing
    key_expr = F.concat_ws(
        "|",
        *[
            F.coalesce(F.col(c).cast("string"), F.lit(""))
            for c in block_key_columns
        ],
    )
    return df.withColumn(block_key_col, F.md5(key_expr))


def compute_blocking_statistics(
    df: DataFrame,
    block_key_col: str = "_block_key",
) -> dict:
    """
    Compute blocking effectiveness statistics.

    Args:
        df: Blocked DataFrame.
        block_key_col: Block key column name.

    Returns:
        Dict with block count, size distribution, and reduction ratio.
    """
    n = df.count()
    stats = df.groupBy(block_key_col).agg(F.count("*").alias("block_size"))

    num_blocks = stats.count()
    avg_block_size = stats.select(F.avg("block_size")).first()[0] if num_blocks > 0 else 0.0
    max_block_size = stats.select(F.max("block_size")).first()[0] if num_blocks > 0 else 0
    min_block_size = stats.select(F.min("block_size")).first()[0] if num_blocks > 0 else 0

    # Reduction ratio: how much did we reduce the comparison space?
    # Original comparisons: n * (n-1) / 2
    # Blocked comparisons: sum over blocks of (b_i * (b_i-1) / 2)
    original_comparisons = n * (n - 1) / 2 if n > 1 else 0

    row = stats.select(
        F.sum(F.col("block_size") * (F.col("block_size") - 1) / 2),
    ).first()
    blocked_comparisons = row[0] if row and row[0] else 0

    reduction_ratio = (
        (1 - blocked_comparisons / original_comparisons) * 100
        if original_comparisons > 0
        else 0.0
    )

    return {
        "total_records": n,
        "num_blocks": num_blocks,
        "avg_block_size": round(avg_block_size, 2) if avg_block_size else 0,
        "max_block_size": max_block_size or 0,
        "min_block_size": min_block_size or 0,
        "original_comparisons": int(original_comparisons),
        "blocked_comparisons": int(blocked_comparisons),
        "reduction_ratio_pct": round(reduction_ratio, 2),
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
    Execute Phase 8: apply record blocking for scalable matching.

    Args:
        spark: Active Spark session.
        df: Input DataFrame from Phase 6/7 (deduplicated Silver records).
        entity_type: Type of entity for blocking strategy selection.
        config: Dict with:
            - strategy: 'standard', 'sorted_neighborhood', or 'canopy'.
            - block_columns: List of columns for standard blocking.
            - sort_key: Sort column for sorted neighborhood.
            - window_size: Window size for sorted neighborhood.
        silver_path: Silver layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        DataFrame with block key assigned.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="08-record-blocking")

    config = config or {}
    strategy = config.get("strategy", "standard")
    logger.info(f"Applying {strategy} blocking strategy for {entity_type}")

    # Apply blocking strategy
    if strategy == "sorted_neighborhood":
        sort_key = config.get("sort_key", "full_name_std")
        if sort_key not in df.columns:
            sort_key = df.columns[0]
        window_size = config.get("window_size", 100)
        result = sorted_neighborhood_blocking(df, sort_key, window_size)

    elif strategy == "canopy":
        cluster_keys = config.get("cluster_key_columns", ["full_name_std"])
        cluster_keys = [k for k in cluster_keys if k in df.columns]
        if not cluster_keys:
            cluster_keys = [df.columns[0]]
        result = canopy_clustering_blocking(df, cluster_keys)

    else:  # standard blocking
        block_columns = config.get("block_columns", ["address_zip5", "address_state"])
        block_columns = [c for c in block_columns if c in df.columns]
        if not block_columns:
            # Fallback: block by first character of name
            if "full_name_std" in df.columns:
                result = df.withColumn(
                    "_block_key",
                    F.substring(F.col("full_name_std"), 1, 3),
                )
            else:
                # Hash the entire row
                result = df.withColumn(
                    "_block_key",
                    F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns[:5]])),
                )
        else:
            result = standard_blocking(df, block_columns)

    # Compute blocking statistics
    stats = compute_blocking_statistics(result)

    # Write blocked data
    target_path = resolve_table_path(
        layer="silver",
        table_name=f"{table_name}_blocked",
        workspace=workspace,
    )
    write_to_delta(result, target_path, mode="overwrite")

    # Emit metrics
    for key, value in stats.items():
        if isinstance(value, int):
            metrics.log_count(key, value)
        else:
            metrics.log_metric(key, value)
    metrics.flush()

    logger.info(
        f"Blocking: {stats['num_blocks']} blocks, "
        f"{stats['reduction_ratio_pct']}% reduction in comparisons "
        f"({stats['original_comparisons']} -> {stats['blocked_comparisons']})"
    )
    return result
