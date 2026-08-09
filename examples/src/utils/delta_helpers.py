"""
Delta Lake helper functions for the entity resolution pipeline.

Provides common operations for reading, writing, merging, and managing
Delta tables across the Bronze, Silver, and Gold medallion layers.

Usage:
    from utils.delta_helpers import write_to_delta, merge_delta, vacuum_table
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


# OneLake path template for Fabric; falls back to local paths
ONELAKE_BASE = "abfss://{workspace}@onelake.dfs.fabric.microsoft.com"


def resolve_table_path(
    layer: str,
    table_name: str,
    workspace: Optional[str] = None,
    local_base: str = "/tmp/delta",
) -> str:
    """
    Resolve the full path for a Delta table in the medallion layer.

    In Fabric, constructs the OneLake abfss path. In local dev, uses
    a local filesystem path.

    Args:
        layer: Medallion layer (bronze, silver, gold).
        table_name: Name of the table.
        workspace: Fabric workspace name (None for local dev).
        local_base: Base directory for local Delta storage.

    Returns:
        Full path to the Delta table.
    """
    import os
    if workspace and os.environ.get("FABRIC_RUNTIME_VERSION"):
        base = ONELAKE_BASE.format(workspace=workspace)
        return f"{base}/{layer}.Lakehouse/Tables/{table_name}"
    return f"{local_base}/{layer}/{table_name}"


def write_to_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: Optional[list[str]] = None,
    merge_schema: bool = False,
) -> None:
    """
    Write a DataFrame to a Delta table.

    Args:
        df: DataFrame to write.
        path: Delta table path (local or OneLake abfss).
        mode: Write mode (append, overwrite, error, ignore).
        partition_by: Optional list of partition columns.
        merge_schema: Whether to merge schema on append.
    """
    writer = df.write.format("delta").mode(mode)

    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)


def read_delta(
    spark: SparkSession,
    path: str,
    version_as_of: Optional[int] = None,
    timestamp_as_of: Optional[str] = None,
) -> DataFrame:
    """
    Read a Delta table, optionally using time travel.

    Args:
        spark: Active Spark session.
        path: Delta table path.
        version_as_of: Read snapshot at this version.
        timestamp_as_of: Read snapshot at this timestamp (ISO 8601).

    Returns:
        DataFrame of the Delta table contents.
    """
    reader = spark.read.format("delta")

    if version_as_of is not None:
        reader = reader.option("versionAsOf", version_as_of)
    if timestamp_as_of is not None:
        reader = reader.option("timestampAsOf", timestamp_as_of)

    return reader.load(path)


def merge_delta(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    merge_condition: str,
    update_set: dict[str, str],
    insert_values: dict[str, str],
    delete_condition: Optional[str] = None,
) -> None:
    """
    Perform a merge (upsert) operation on a Delta table.

    Args:
        spark: Active Spark session.
        target_path: Path to the target Delta table.
        source_df: Source DataFrame with new/updated records.
        merge_condition: SQL condition for matching (e.g., "t.id = s.id").
        update_set: Dict mapping target columns to source expressions for UPDATE.
        insert_values: Dict mapping target columns to source expressions for INSERT.
        delete_condition: Optional condition to delete matched rows.
    """
    target_table = DeltaTable.forPath(spark, target_path)
    merge = target_table.alias("t").merge(
        source_df.alias("s"), merge_condition
    )

    update_expr = {k: f"s.{v}" for k, v in update_set.items()}
    insert_expr = {k: f"s.{v}" for k, v in insert_values.items()}

    merge = merge.whenMatchedUpdate(set=update_expr)
    merge = merge.whenNotMatchedInsert(values=insert_expr)

    if delete_condition:
        merge = merge.whenMatchedDelete(condition=delete_condition)

    merge.execute()


def vacuum_table(
    spark: SparkSession,
    path: str,
    retention_hours: int = 168,
) -> None:
    """
    Remove old file versions from a Delta table.

    Args:
        spark: Active Spark session.
        path: Delta table path.
        retention_hours: Minimum retention period in hours (default 7 days).
    """
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.vacuum(retention_hours)


def optimize_table(
    spark: SparkSession,
    path: str,
    zorder_by: Optional[list[str]] = None,
) -> None:
    """
    Compact small files and optionally Z-order a Delta table.

    Args:
        spark: Active Spark session.
        path: Delta table path.
        zorder_by: Columns to Z-order by for data skipping.
    """
    delta_table = DeltaTable.forPath(spark, path)
    if zorder_by:
        delta_table.optimize().executeZOrderBy(*zorder_by)
    else:
        delta_table.optimize().executeCompaction()


def table_exists(spark: SparkSession, path: str) -> bool:
    """
    Check whether a Delta table exists at the given path.

    Args:
        spark: Active Spark session.
        path: Delta table path.

    Returns:
        True if the Delta table exists.
    """
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


def get_table_history(
    spark: SparkSession,
    path: str,
    limit: int = 10,
) -> DataFrame:
    """
    Retrieve the operation history of a Delta table.

    Args:
        spark: Active Spark session.
        path: Delta table path.
        limit: Maximum number of history entries.

    Returns:
        DataFrame of operation history.
    """
    delta_table = DeltaTable.forPath(spark, path)
    return delta_table.history(limit)
