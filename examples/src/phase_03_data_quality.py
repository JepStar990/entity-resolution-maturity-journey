"""
Phase 3: Data Quality Rules

Applies data quality rules to validated Bronze data using a rules engine.
Records that pass quality checks are promoted to the Silver layer.
Records that fail are flagged for review.

Input: Validated Bronze tables
Output: Quality-scored records promoted to Silver; quality report

Medallion Layer: Bronze -> Silver transition gate
"""

from __future__ import annotations

from typing import Optional, Callable

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


class QualityRule:
    """
    A declarative data quality rule.

    Attributes:
        name: Unique rule identifier.
        description: Human-readable description.
        condition: Spark Column expression that evaluates to True for valid records.
        severity: 'error' (blocking) or 'warning' (non-blocking).
        category: Rule category (completeness, uniqueness, validity, consistency, timeliness).
    """

    def __init__(
        self,
        name: str,
        description: str,
        condition: Column,
        severity: str = "error",
        category: str = "validity",
    ):
        self.name = name
        self.description = description
        self.condition = condition
        self.severity = severity
        self.category = category


def apply_quality_rules(
    df: DataFrame,
    rules: list[QualityRule],
) -> DataFrame:
    """
    Apply data quality rules and add per-record quality metadata.

    Adds columns:
    - _quality_score: Fraction of rules passed (0.0 to 1.0).
    - _quality_errors: Array of failed rule names.
    - _quality_warnings: Array of warning rule names.
    - _quality_passed: Boolean (True if all error-severity rules passed).

    Args:
        df: Input DataFrame.
        rules: List of QualityRule objects.

    Returns:
        DataFrame with quality evaluation columns.
    """
    result = df
    total_rules = len(rules)
    error_count = sum(1 for r in rules if r.severity == "error")

    # Build arrays of passed/failed rules
    failed_names: list[Column] = []
    warning_names: list[Column] = []

    for rule in rules:
        result = result.withColumn(
            f"_qr_{rule.name}",
            F.when(rule.condition, True).otherwise(False),
        )
        if rule.severity == "error":
            failed_names.append(
                F.when(~F.col(f"_qr_{rule.name}"), F.lit(rule.name))
            )
        else:
            warning_names.append(
                F.when(~F.col(f"_qr_{rule.name}"), F.lit(rule.name))
            )

    # Compute quality score
    score_cols = [F.col(f"_qr_{rule.name}").cast("int") for rule in rules]
    result = result.withColumn(
        "_quality_score",
        sum(score_cols) / F.lit(total_rules),
    )

    # Collect failures and warnings
    if failed_names:
        result = result.withColumn(
            "_quality_errors",
            F.filter(
                F.array(*failed_names),
                lambda x: x.isNotNull(),
            ),
        )
    else:
        result = result.withColumn("_quality_errors", F.array())

    if warning_names:
        result = result.withColumn(
            "_quality_warnings",
            F.filter(
                F.array(*warning_names),
                lambda x: x.isNotNull(),
            ),
        )
    else:
        result = result.withColumn("_quality_warnings", F.array())

    result = result.withColumn(
        "_quality_passed",
        F.size(F.col("_quality_errors")) == 0,
    )

    # Drop intermediate rule result columns
    for rule in rules:
        result = result.drop(f"_qr_{rule.name}")

    return result


def build_common_rules() -> list[QualityRule]:
    """
    Build a set of common data quality rules applicable to most entity types.

    Returns:
        List of QualityRule objects.
    """
    return [
        QualityRule(
            name="id_not_null",
            description="Entity ID must not be null",
            condition=F.col("id").isNotNull(),
            severity="error",
            category="completeness",
        ),
        QualityRule(
            name="id_not_blank",
            description="Entity ID must not be blank when string",
            condition=F.col("id").cast("string") != F.lit(""),
            severity="error",
            category="completeness",
        ),
        QualityRule(
            name="name_not_null",
            description="Entity name must not be null",
            condition=F.col("name").isNotNull(),
            severity="warning",
            category="completeness",
        ),
        QualityRule(
            name="name_min_length",
            description="Entity name must be at least 2 characters",
            condition=F.length(F.col("name").cast("string")) >= 2,
            severity="warning",
            category="validity",
        ),
        QualityRule(
            name="created_date_valid",
            description="Created date must be a valid date in the past",
            condition=(
                F.col("created_date").isNull()
                | (F.col("created_date").cast("date") <= F.current_date())
            ),
            severity="error",
            category="validity",
        ),
    ]


def generate_quality_report(
    df: DataFrame,
    rules: list[QualityRule],
) -> DataFrame:
    """
    Generate a per-rule quality report.

    Args:
        df: DataFrame with quality evaluation.
        rules: List of applied quality rules.

    Returns:
        DataFrame with one row per rule showing pass/fail counts and rates.
    """
    report_rows = []
    for rule in rules:
        total = df.count()
        passed = df.filter(
            F.size(F.col("_quality_errors")) > 0
        ).count()
        report_rows.append({
            "rule_name": rule.name,
            "description": rule.description,
            "severity": rule.severity,
            "category": rule.category,
            "total_records": total,
            "passed_records": total - passed if total > 0 else 0,
            "failed_records": passed,
            "pass_rate_pct": round((total - passed) / total * 100, 2) if total > 0 else 0.0,
        })

    report_df = df.sparkSession.createDataFrame(report_rows)
    return report_df


def run(
    spark: SparkSession,
    df: DataFrame,
    quality_rules: Optional[list[QualityRule]] = None,
    silver_path: str = "Tables/silver/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
    gateway_threshold: float = 1.0,
) -> DataFrame:
    """
    Execute Phase 3: apply quality rules and gate progression to Silver.

    Args:
        spark: Active Spark session.
        df: Input DataFrame from Phase 2 (validated Bronze).
        quality_rules: Custom quality rules (defaults to common rules).
        silver_path: Base path for Silver layer output.
        table_name: Target table name for Silver promotion.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.
        gateway_threshold: Minimum quality score to pass to Silver (0.0-1.0).

    Returns:
        DataFrame of records promoted to Silver (quality passed + score >= threshold).
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="03-data-quality")

    if quality_rules is None:
        quality_rules = build_common_rules()

    logger.info(f"Applying {len(quality_rules)} quality rules to {table_name}")

    # Apply quality rules
    validated = apply_quality_rules(df, quality_rules)

    # Gate: records must pass all error rules AND meet threshold
    silver_df = validated.filter(
        (F.col("_quality_passed") == True)  # noqa: E712
        & (F.col("_quality_score") >= gateway_threshold)
    )

    failed_df = validated.filter(
        (F.col("_quality_passed") == False)  # noqa: E712
        | (F.col("_quality_score") < gateway_threshold)
    )

    # Write quality-passed records to Silver
    silver_target = resolve_table_path(
        layer="silver",
        table_name=table_name,
        workspace=workspace,
    )
    write_to_delta(silver_df, silver_target, mode="overwrite")

    # Generate and log quality report
    report = generate_quality_report(validated, quality_rules)

    total = validated.count()
    passed = silver_df.count()
    failed = total - passed

    metrics.log_count("quality_total_records", total)
    metrics.log_count("quality_passed_records", passed)
    metrics.log_count("quality_failed_records", failed)
    metrics.log_metric("quality_pass_rate_pct", round(passed / total * 100, 2) if total > 0 else 0.0)
    metrics.log_metric("mean_quality_score", validated.select(F.avg("_quality_score")).first()[0] or 0.0)
    metrics.flush()

    logger.info(f"Quality gate: {passed}/{total} records promoted to Silver ({passed / total * 100:.1f}%)" if total > 0 else "No records to process")
    return silver_df
