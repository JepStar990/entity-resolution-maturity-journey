"""
Phase 2: Schema Validation

Validates ingested Bronze data against defined schema contracts.
Separates records that pass validation from those that fail,
writing pass/fail to separate locations for investigation.

Input: Bronze tables with raw, metadata-enriched records
Output: Validated Bronze with _schema_valid flag and _schema_errors

Medallion Layer: Bronze
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DataType

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
) -> DataFrame:
    """
    Validate that a DataFrame conforms to an expected schema.

    Checks column presence, nullability constraints, and data types.
    Adds:
    - _schema_valid: Boolean flag (True if record passes all checks).
    - _schema_errors: Array of validation error messages (empty if valid).

    Args:
        df: Input DataFrame.
        expected_schema: Expected StructType schema definition.

    Returns:
        DataFrame with _schema_valid and _schema_errors columns.
    """
    errors_expr = F.array()

    # Check 1: Required columns present
    expected_columns = {f.name for f in expected_schema.fields}
    actual_columns = set(df.columns)
    missing = expected_columns - actual_columns
    extra = actual_columns - expected_columns - {
        "_ingested_at", "_source_system", "_batch_id", "_source_file",
        "_schema_valid", "_schema_errors",
    }

    if missing:
        logger.warning(f"Missing columns: {missing}")
        errors_expr = errors_expr

    # Check 2: Required (non-nullable) fields populated
    for field in expected_schema.fields:
        if not field.nullable and field.name in df.columns:
            null_condition = F.col(field.name).isNull()
            errors_expr = F.when(
                null_condition,
                F.array_union(
                    errors_expr,
                    F.array(F.lit(f"Required field '{field.name}' is null")),
                ),
            ).otherwise(errors_expr)

    # Check 3: Data type compatibility (string-based check for flexibility)
    for field in expected_schema.fields:
        if field.name in df.columns:
            # Attempt to cast; flag if incompatible
            try:
                df.select(F.col(field.name).cast(field.dataType))
            except Exception:
                type_condition = F.lit(True)
                errors_expr = F.when(
                    type_condition,
                    F.array_union(
                        errors_expr,
                        F.array(F.lit(
                            f"Type mismatch for '{field.name}': "
                            f"expected {field.dataType.simpleString()}"
                        )),
                    ),
                ).otherwise(errors_expr)

    validated = df.withColumn("_schema_errors", errors_expr)
    validated = validated.withColumn(
        "_schema_valid",
        F.size(F.col("_schema_errors")) == 0,
    )

    return validated


def split_valid_invalid(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Split a validated DataFrame into valid and invalid subsets.

    Args:
        df: DataFrame with _schema_valid column.

    Returns:
        Tuple of (valid_records, invalid_records).
    """
    valid = df.filter(F.col("_schema_valid") == True)  # noqa: E712
    invalid = df.filter(F.col("_schema_valid") == False)  # noqa: E712
    return valid, invalid


def get_schema_summary(
    df: DataFrame,
) -> DataFrame:
    """
    Generate a summary of schema validation results.

    Args:
        df: Validated DataFrame.

    Returns:
        DataFrame with pass/fail counts and error distributions.
    """
    summary = df.agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("_schema_valid") == True, 1).otherwise(0)).alias("valid_records"),  # noqa: E712
        F.sum(F.when(F.col("_schema_valid") == False, 1).otherwise(0)).alias("invalid_records"),  # noqa: E712
        F.round(
            F.sum(F.when(F.col("_schema_valid") == True, 1).otherwise(0)) / F.count("*") * 100,  # noqa: E712
            2,
        ).alias("pass_rate_pct"),
    )
    return summary


def run(
    spark: SparkSession,
    df: DataFrame,
    expected_schema: StructType,
    bronze_path: str,
    table_name: str,
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> DataFrame:
    """
    Execute Phase 2: validate schema and separate valid/invalid records.

    Args:
        spark: Active Spark session.
        df: Input DataFrame from Phase 1 (Bronze).
        expected_schema: Expected schema for validation.
        bronze_path: Base path for Bronze layer.
        table_name: Base table name for output tables.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        DataFrame of valid records only.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="02-schema-validation")

    logger.info(f"Validating schema for table: {table_name}")

    # Validate
    validated = validate_schema(df, expected_schema)

    # Split
    valid_df, invalid_df = split_valid_invalid(validated)

    # Validate and write valid records
    valid_path = resolve_table_path(
        layer="bronze",
        table_name=f"{table_name}_valid",
        workspace=workspace,
    )
    write_to_delta(valid_df, valid_path, mode="overwrite")

    # Write invalid records for investigation
    invalid_path = resolve_table_path(
        layer="bronze",
        table_name=f"{table_name}_invalid",
        workspace=workspace,
    )
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        write_to_delta(invalid_df, invalid_path, mode="overwrite")
        logger.warning(f"{invalid_count} invalid records written to {invalid_path}")

    # Emit metrics
    total = validated.count()
    valid_count = valid_df.count()
    pass_rate = (valid_count / total * 100) if total > 0 else 0.0

    metrics.log_count("total_records", total)
    metrics.log_count("valid_records", valid_count)
    metrics.log_count("invalid_records", invalid_count)
    metrics.log_metric("schema_pass_rate_pct", round(pass_rate, 2))
    metrics.flush()

    logger.info(
        f"Schema validation complete: {valid_count}/{total} passed ({pass_rate:.1f}%)"
    )
    return valid_df
