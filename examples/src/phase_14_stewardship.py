"""
Phase 14: Data Stewardship

Provides a human-in-the-loop review system for uncertain entity matches.
Data stewards can accept, reject, or flag matches for further review.
Steward decisions feed back into model retraining to continuously improve
matching accuracy.

Input: Uncertain matches from Phases 10/11; golden records from Phase 13
Output: Reviewed match decisions; stewardship audit log; training labels

Medallion Layer: Gold
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, TimestampType, BooleanType,
)

from utils.delta_helpers import write_to_delta, resolve_table_path, read_delta
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


# Standard review actions
REVIEW_ACTION_APPROVE = "approve"
REVIEW_ACTION_REJECT = "reject"
REVIEW_ACTION_FLAG = "flag"
REVIEW_ACTION_SPLIT = "split"
VALID_ACTIONS = {REVIEW_ACTION_APPROVE, REVIEW_ACTION_REJECT, REVIEW_ACTION_FLAG, REVIEW_ACTION_SPLIT}


def create_review_queue(
    match_df: DataFrame,
    priority_rules: Optional[list[dict]] = None,
    max_queue_size: Optional[int] = None,
) -> DataFrame:
    """
    Create a prioritized review queue from uncertain matches.

    Priority is determined by:
    1. Match probability (closer to 0.5 = more uncertain = higher priority).
    2. Business impact (configurable rules).
    3. Entity type importance.

    Args:
        match_df: DataFrame of match pairs with match_probability.
        priority_rules: List of rules defining priority boosts.
        max_queue_size: Cap queue size (None = unlimited).

    Returns:
        DataFrame of review tasks ordered by priority.
    """
    queue = match_df.filter(
        (F.col("_needs_llm_review") == True) |  # noqa: E712
        (F.col("match_probability").between(0.3, 0.7))
    )

    # Calculate review priority
    # Closer to 0.5 = more uncertain = higher priority
    queue = queue.withColumn(
        "_review_priority",
        F.lit(1.0) - F.abs(F.col("match_probability") - F.lit(0.5)) * 2,
    )

    # Apply business priority boosts
    if priority_rules:
        for rule in priority_rules:
            condition = rule.get("condition")
            boost = rule.get("boost", 0.1)
            if condition:
                queue = queue.withColumn(
                    "_review_priority",
                    F.when(condition, F.col("_review_priority") + boost)
                    .otherwise(F.col("_review_priority")),
                )

    # Order by priority
    queue = queue.orderBy(F.col("_review_priority").desc())

    if max_queue_size:
        queue = queue.limit(max_queue_size)

    # Add review metadata
    queue = queue.withColumn("_review_status", F.lit("pending"))
    queue = queue.withColumn("_reviewed_by", F.lit(None).cast(StringType()))
    queue = queue.withColumn("_reviewed_at", F.lit(None).cast(TimestampType()))
    queue = queue.withColumn("_review_action", F.lit(None).cast(StringType()))
    queue = queue.withColumn("_review_notes", F.lit(None).cast(StringType()))

    return queue


def record_steward_decision(
    review_queue: DataFrame,
    match_id_left: str,
    match_id_right: str,
    action: str,
    steward_id: str,
    notes: Optional[str] = None,
) -> DataFrame:
    """
    Record a steward's decision on a specific match pair.

    Args:
        review_queue: Current review queue DataFrame.
        match_id_left: Left entity ID of the pair being reviewed.
        match_id_right: Right entity ID of the pair being reviewed.
        action: One of 'approve', 'reject', 'flag', 'split'.
        steward_id: Identifier of the steward making the decision.
        notes: Optional steward notes.

    Returns:
        Updated review queue DataFrame.

    Raises:
        ValueError: If action is not one of the valid review actions.
    """
    if action not in VALID_ACTIONS:
        raise ValueError(
            f"Invalid review action: {action}. Must be one of {VALID_ACTIONS}"
        )

    from datetime import datetime, timezone

    updated = review_queue.withColumn(
        "_review_status",
        F.when(
            (F.col("id_left") == match_id_left)
            & (F.col("id_right") == match_id_right),
            F.lit("reviewed"),
        ).otherwise(F.col("_review_status")),
    ).withColumn(
        "_review_action",
        F.when(
            (F.col("id_left") == match_id_left)
            & (F.col("id_right") == match_id_right),
            F.lit(action),
        ).otherwise(F.col("_review_action")),
    ).withColumn(
        "_reviewed_by",
        F.when(
            (F.col("id_left") == match_id_left)
            & (F.col("id_right") == match_id_right),
            F.lit(steward_id),
        ).otherwise(F.col("_reviewed_by")),
    ).withColumn(
        "_reviewed_at",
        F.when(
            (F.col("id_left") == match_id_left)
            & (F.col("id_right") == match_id_right),
            F.lit(datetime.now(timezone.utc).isoformat()).cast(TimestampType()),
        ).otherwise(F.col("_reviewed_at")),
    ).withColumn(
        "_review_notes",
        F.when(
            (F.col("id_left") == match_id_left)
            & (F.col("id_right") == match_id_right),
            F.lit(notes or ""),
        ).otherwise(F.col("_review_notes")),
    )

    return updated


def generate_training_labels(
    review_queue: DataFrame,
    action_to_label: Optional[dict[str, int]] = None,
) -> DataFrame:
    """
    Convert steward decisions into training labels for model retraining.

    Args:
        review_queue: Review queue with steward decisions.
        action_to_label: Mapping from review action to binary label
                         (default: approve=1, reject=0, flag=None, split=0).

    Returns:
        DataFrame with id_left, id_right, is_match columns for training.
    """
    if action_to_label is None:
        action_to_label = {
            REVIEW_ACTION_APPROVE: 1,
            REVIEW_ACTION_REJECT: 0,
            REVIEW_ACTION_SPLIT: 0,
            # FLAG is excluded (not labeled)
        }

    # Filter to reviewed items with labelable actions
    labeled = review_queue.filter(
        (F.col("_review_status") == "reviewed")
        & F.col("_review_action").isin(*action_to_label.keys())
    )

    # Map action to label
    label_expr = F.lit(None).cast("int")
    for action, label in action_to_label.items():
        label_expr = F.when(
            F.col("_review_action") == action,
            F.lit(label),
        ).otherwise(label_expr)

    training_data = labeled.select(
        F.col("id_left"),
        F.col("id_right"),
        label_expr.alias("is_match"),
        F.col("_reviewed_by").alias("labeled_by"),
        F.col("_reviewed_at").alias("labeled_at"),
    )

    return training_data


def compute_stewardship_metrics(
    review_queue: DataFrame,
) -> dict:
    """
    Compute stewardship effectiveness metrics.

    Args:
        review_queue: Review queue with decisions.

    Returns:
        Dict of stewardship metrics.
    """
    total = review_queue.count()
    reviewed = review_queue.filter(F.col("_review_status") == "reviewed").count()
    pending = total - reviewed

    action_counts = {}
    for action in VALID_ACTIONS:
        count = review_queue.filter(F.col("_review_action") == action).count()
        action_counts[f"action_{action}"] = count

    completion_rate = (reviewed / total * 100) if total > 0 else 0.0

    return {
        "total_review_tasks": total,
        "reviewed_tasks": reviewed,
        "pending_tasks": pending,
        "completion_rate_pct": round(completion_rate, 2),
        **action_counts,
    }


def create_audit_log_schema() -> StructType:
    """Create the schema for the stewardship audit log table."""
    return StructType([
        StructField("audit_id", StringType(), True),
        StructField("id_left", StringType(), True),
        StructField("id_right", StringType(), True),
        StructField("action", StringType(), True),
        StructField("steward_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("notes", StringType(), True),
        StructField("match_probability", FloatType(), True),
    ])


def run(
    spark: SparkSession,
    match_df: Optional[DataFrame] = None,
    entity_type: str = "customer",
    config: Optional[dict] = None,
    gold_path: str = "Tables/gold/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> dict:
    """
    Execute Phase 14: create review queue and manage stewardship workflow.

    Args:
        spark: Active Spark session.
        match_df: Match scored pairs DataFrame from Phase 10/11.
        entity_type: Type of entity.
        config: Dict with:
            - priority_rules: Custom rules for review prioritization.
            - max_queue_size: Maximum review queue size.
            - auto_approve_threshold: Probability above which auto-approve.
            - auto_reject_threshold: Probability below which auto-reject.
        gold_path: Gold layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        Dict with review_queue DataFrame and stewardship metrics.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="14-stewardship")

    config = config or {}
    logger.info(f"Setting up stewardship review for {entity_type}")

    if match_df is None:
        logger.warning("No match data provided; skipping stewardship setup")
        return {"review_queue": None, "metrics": {}}

    priority_rules = config.get("priority_rules")
    max_queue_size = config.get("max_queue_size")

    # Create review queue
    review_queue = create_review_queue(match_df, priority_rules, max_queue_size)

    # Write review queue to Gold
    queue_path = resolve_table_path(
        layer="gold",
        table_name=f"{table_name}_review_queue",
        workspace=workspace,
    )
    write_to_delta(review_queue, queue_path, mode="overwrite")

    # Generate training labels from existing decisions (if any)
    labeled = review_queue.filter(F.col("_review_status") == "reviewed")
    label_count = labeled.count()
    if label_count > 0:
        training_data = generate_training_labels(review_queue)
        train_path = resolve_table_path(
            layer="gold",
            table_name=f"{table_name}_training_labels",
            workspace=workspace,
        )
        write_to_delta(training_data, train_path, mode="append")
        logger.info(f"Generated {training_data.count()} training labels from steward decisions")

    # Compute stewardship metrics
    stew_metrics = compute_stewardship_metrics(review_queue)
    for key, value in stew_metrics.items():
        if isinstance(value, int):
            metrics.log_count(key, value)
        elif isinstance(value, float):
            metrics.log_metric(key, value)
    metrics.flush()

    logger.info(
        f"Review queue: {stew_metrics['total_review_tasks']} tasks, "
        f"{stew_metrics['reviewed_tasks']} reviewed "
        f"({stew_metrics['completion_rate_pct']:.1f}% complete)"
    )
    return {
        "review_queue": review_queue,
        "training_labels": labeled,
        "metrics": stew_metrics,
    }
