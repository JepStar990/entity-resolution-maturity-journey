"""
Phase 13: Golden Record Creation

Merges matched entity records into single, authoritative golden records
using configurable survivorship rules. A golden record represents the
best-known version of an entity, composed of the best attribute values
from all matched source records.

Input: Matched record clusters from Phases 10/11/12
Output: Golden records (one per entity cluster)

Medallion Layer: Gold
"""

from __future__ import annotations

from typing import Optional, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


class SurvivorshipRule:
    """
    Defines how to select the best value for an attribute from multiple
    source records within a matched entity cluster.

    Attributes:
        attribute: Target attribute name.
        strategy: Selection strategy ('most_recent', 'longest', 'most_complete',
                  'prefer_source', 'most_frequent', 'custom').
        source_priority: List of source systems in priority order.
        fallback_value: Default value if no source provides the attribute.
    """

    def __init__(
        self,
        attribute: str,
        strategy: str = "most_recent",
        source_priority: Optional[list[str]] = None,
        fallback_value: Optional[str] = None,
        custom_udf: Optional[Callable] = None,
    ):
        self.attribute = attribute
        self.strategy = strategy
        self.source_priority = source_priority or []
        self.fallback_value = fallback_value
        self.custom_udf = custom_udf


def apply_survivorship_rules(
    df: DataFrame,
    cluster_id_col: str,
    rules: list[SurvivorshipRule],
) -> DataFrame:
    """
    Apply survivorship rules to create golden records from entity clusters.

    For each cluster and each attribute, selects the best value based on
    the configured survivorship strategy.

    Args:
        df: DataFrame of entity records with cluster_id assigned.
        cluster_id_col: Column identifying the entity cluster (e.g., _cluster_id).
        rules: List of SurvivorshipRule objects defining attribute selection.

    Returns:
        DataFrame with one golden record per cluster.
    """
    result_rows = []
    clusters = df.select(cluster_id_col).distinct().collect()

    for row in clusters:
        cluster_id = row[cluster_id_col]
        cluster_df = df.filter(F.col(cluster_id_col) == cluster_id)

        golden_attrs: dict = {cluster_id_col: cluster_id}
        golden_attrs["_source_record_count"] = cluster_df.count()

        for rule in rules:
            attr = rule.attribute
            if attr not in cluster_df.columns:
                continue

            if rule.strategy == "most_recent":
                best = select_most_recent(cluster_df, attr)

            elif rule.strategy == "longest":
                best = select_longest(cluster_df, attr)

            elif rule.strategy == "most_complete":
                best = select_most_complete(cluster_df, attr)

            elif rule.strategy == "prefer_source":
                best = select_prefer_source(cluster_df, attr, rule.source_priority)

            elif rule.strategy == "most_frequent":
                best = select_most_frequent(cluster_df, attr)

            elif rule.strategy == "custom" and rule.custom_udf:
                best = rule.custom_udf(cluster_df, attr)

            else:
                best = select_most_recent(cluster_df, attr)

            golden_attrs[attr] = best if best is not None else rule.fallback_value
            golden_attrs[f"{attr}_source"] = get_source_for_value(cluster_df, attr, best)

        result_rows.append(golden_attrs)

    if not result_rows:
        schema = StructType([
            StructField(cluster_id_col, StringType(), True),
            StructField("_source_record_count", StringType(), True),
        ])
        return df.sparkSession.createDataFrame([], schema)

    return df.sparkSession.createDataFrame(result_rows)


def select_most_recent(df: DataFrame, attr: str) -> Optional[str]:
    """Select the most recently updated non-null value."""
    order_col = "updated_at" if "updated_at" in df.columns else "_ingested_at"
    if order_col not in df.columns:
        order_col = df.columns[0]

    row = df.filter(F.col(attr).isNotNull()) \
        .orderBy(F.col(order_col).desc_nulls_last()) \
        .select(attr) \
        .first()
    return row[attr] if row else None


def select_longest(df: DataFrame, attr: str) -> Optional[str]:
    """Select the longest non-null string value."""
    row = df.filter(F.col(attr).isNotNull()) \
        .withColumn("_str_len", F.length(F.col(attr).cast("string"))) \
        .orderBy(F.col("_str_len").desc_nulls_last()) \
        .select(attr) \
        .first()
    return row[attr] if row else None


def select_most_complete(df: DataFrame, attr: str) -> Optional[str]:
    """
    Select the record with the fewest null values across all attributes.
    Returns the value of the specified attribute from the most complete record.
    """
    non_metadata_cols = [
        c for c in df.columns
        if not c.startswith("_") and c != "id"
    ]
    # Count non-null values per row
    null_counts = [
        F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0))
        for c in non_metadata_cols
    ]
    scored = df.withColumn("_null_count", sum(null_counts))

    row = scored.orderBy("_null_count").select(attr).first()
    return row[attr] if row else None


def select_prefer_source(
    df: DataFrame,
    attr: str,
    source_priority: list[str],
) -> Optional[str]:
    """Select the value from the highest-priority source system."""
    source_col = "_source_system"
    if source_col not in df.columns:
        return select_most_recent(df, attr)

    for source in source_priority:
        row = df.filter(
            (F.col(source_col) == source) & F.col(attr).isNotNull()
        ).select(attr).first()
        if row and row[attr] is not None:
            return row[attr]

    # Fallback: any non-null value
    return select_most_recent(df, attr)


def select_most_frequent(df: DataFrame, attr: str) -> Optional[str]:
    """Select the most frequently occurring non-null value."""
    freq = df.filter(F.col(attr).isNotNull()) \
        .groupBy(attr) \
        .count() \
        .orderBy(F.col("count").desc()) \
        .first()
    return freq[attr] if freq else None


def get_source_for_value(df: DataFrame, attr: str, value: object) -> Optional[str]:
    """Get the source system that provided a given value."""
    if value is None:
        return None
    source_col = "_source_system"
    if source_col not in df.columns:
        return "unknown"

    row = df.filter(F.col(attr) == value).select(source_col).first()
    return row[source_col] if row else "unknown"


def assign_cluster_ids(
    match_pairs: DataFrame,
    entity_id_col_left: str = "id_left",
    entity_id_col_right: str = "id_right",
    cluster_id_col: str = "_cluster_id",
) -> DataFrame:
    """
    Assign cluster IDs to entities based on match pairs using connected
    components. Simple iterative transitive closure approach.

    Args:
        match_pairs: DataFrame of matched record pairs.
        entity_id_col_left: Left entity ID column.
        entity_id_col_right: Right entity ID column.
        cluster_id_col: Output cluster ID column name.

    Returns:
        DataFrame mapping entity_id to cluster_id.
    """
    # Extract unique entity IDs from both sides of match pairs
    left = match_pairs.select(F.col(entity_id_col_left).alias("entity_id"))
    right = match_pairs.select(F.col(entity_id_col_right).alias("entity_id"))
    all_entities = left.union(right).distinct()

    # Initialize: each entity is its own cluster
    clusters = all_entities.withColumn(cluster_id_col, F.col("entity_id"))

    # Iterative transitive closure (up to 10 iterations)
    for iteration in range(10):
        # Join matches to find connections
        joined = match_pairs.alias("m").join(
            clusters.alias("c"),
            F.col("m." + entity_id_col_left) == F.col("c.entity_id"),
            "inner",
        ).select(
            F.col("m." + entity_id_col_right).alias("entity_id"),
            F.col("c." + cluster_id_col).alias("new_cluster"),
        )

        # For each entity, find the minimum cluster ID it connects to
        current_with_new = clusters.alias("c").join(
            joined.alias("j"),
            F.col("c.entity_id") == F.col("j.entity_id"),
            "left",
        ).select(
            F.col("c.entity_id"),
            F.least(
                F.col("c." + cluster_id_col),
                F.coalesce(F.col("j.new_cluster"), F.col("c." + cluster_id_col)),
            ).alias(cluster_id_col),
        ).distinct()

        new_count = current_with_new.count()
        old_count = clusters.count()
        clusters = current_with_new

        # Stop if stable
        if new_count == old_count:
            logger.info(f"Transitive closure converged after {iteration + 1} iterations")
            break

    return clusters


def generate_golden_record_schema() -> StructType:
    """Generate the standard golden record schema."""
    return StructType([
        StructField("golden_id", StringType(), True),
        StructField("_cluster_id", StringType(), True),
        StructField("_source_record_count", StringType(), True),
        StructField("_created_at", TimestampType(), True),
        StructField("_updated_at", TimestampType(), True),
        StructField("_confidence_score", StringType(), True),
    ])


def run(
    spark: SparkSession,
    entity_df: DataFrame,
    match_pairs: Optional[DataFrame] = None,
    entity_type: str = "customer",
    config: Optional[dict] = None,
    gold_path: str = "Tables/gold/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> DataFrame:
    """
    Execute Phase 13: create golden records from matched entity clusters.

    Args:
        spark: Active Spark session.
        entity_df: Entity DataFrame with all attributes.
        match_pairs: Matched record pairs from previous phases.
        entity_type: Type of entity.
        config: Dict with:
            - survivorship_rules: List of SurvivorshipRule definitions.
            - source_priorities: Dict mapping source names to priority numbers.
        gold_path: Gold layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        DataFrame of golden records (one per entity cluster).
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="13-golden-records")

    config = config or {}
    logger.info(f"Creating golden records for {entity_type}")

    # Assign cluster IDs from match pairs
    if match_pairs is not None:
        clusters = assign_cluster_ids(match_pairs)
        entity_df = entity_df.join(
            clusters,
            entity_df["id"] == clusters["entity_id"],
            "left",
        ).drop("entity_id")

        # Entities without matches are their own cluster
        entity_df = entity_df.withColumn(
            "_cluster_id",
            F.coalesce(F.col("_cluster_id"), F.col("id")),
        )
    else:
        # No match pairs: each entity is its own cluster
        entity_df = entity_df.withColumn("_cluster_id", F.col("id"))

    # Define survivorship rules
    default_rules = {
        "customer": [
            SurvivorshipRule("full_name_std", strategy="longest"),
            SurvivorshipRule("email_std", strategy="most_recent"),
            SurvivorshipRule("phone_std", strategy="most_recent"),
            SurvivorshipRule("address_street", strategy="longest"),
            SurvivorshipRule("address_city", strategy="most_frequent"),
            SurvivorshipRule("address_state", strategy="most_frequent"),
            SurvivorshipRule("address_zip5", strategy="most_frequent"),
            SurvivorshipRule("address_country", strategy="most_frequent"),
        ],
        "company": [
            SurvivorshipRule("company_name_std", strategy="longest"),
            SurvivorshipRule("ticker_std", strategy="most_recent"),
            SurvivorshipRule("sic_description", strategy="most_recent"),
        ],
        "product": [
            SurvivorshipRule("product_name_std", strategy="longest"),
            SurvivorshipRule("sku_std", strategy="most_recent"),
        ],
    }

    rule_configs = config.get(
        "survivorship_rules",
        default_rules.get(entity_type, default_rules["customer"]),
    )

    # Convert config dicts to SurvivorshipRule objects if needed
    rules = []
    for r in rule_configs:
        if isinstance(r, SurvivorshipRule):
            rules.append(r)
        elif isinstance(r, dict):
            rules.append(SurvivorshipRule(
                attribute=r["attribute"],
                strategy=r.get("strategy", "most_recent"),
                source_priority=r.get("source_priority", []),
                fallback_value=r.get("fallback_value"),
            ))

    # Apply survivorship rules
    golden = apply_survivorship_rules(entity_df, "_cluster_id", rules)

    # Add golden record metadata
    import time
    golden = golden.withColumn("golden_id", F.col("_cluster_id"))
    golden = golden.withColumn("_created_at", F.current_timestamp())
    golden = golden.withColumn("_updated_at", F.current_timestamp())
    golden = golden.withColumn("_confidence_score", F.lit(1.0))

    # Write golden records to Gold
    target_path = resolve_table_path(
        layer="gold",
        table_name=f"{table_name}_golden",
        workspace=workspace,
    )
    write_to_delta(golden, target_path, mode="overwrite")

    # Emit metrics
    golden_count = golden.count()
    total_entities = entity_df.count()
    compression_ratio = (1 - golden_count / max(total_entities, 1)) * 100

    metrics.log_count("golden_records", golden_count)
    metrics.log_count("source_entities", total_entities)
    metrics.log_metric("compression_ratio_pct", round(compression_ratio, 2))
    metrics.flush()

    logger.info(
        f"Created {golden_count} golden records from {total_entities} entities "
        f"({compression_ratio:.1f}% reduction)"
    )
    return golden
