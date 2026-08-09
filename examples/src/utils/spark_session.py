"""
Spark session builder for local development and Fabric runtime.

Provides a unified interface to create Spark sessions that work both
locally (with Delta Lake JARs) and inside Microsoft Fabric (where Delta
and OneLake are pre-configured).

Usage:
    from utils.spark_session import create_spark_session
    spark = create_spark_session(app_name="Phase01-Ingestion")
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession


def _is_fabric_runtime() -> bool:
    """Detect whether the code is running inside a Microsoft Fabric runtime."""
    return os.environ.get("FABRIC_RUNTIME_VERSION") is not None


def _is_databricks_runtime() -> bool:
    """Detect whether the code is running inside a Databricks runtime."""
    return os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None


def create_spark_session(
    app_name: str = "EntityResolution",
    master: str = "local[*]",
    executor_memory: str = "4g",
    driver_memory: str = "2g",
    shuffle_partitions: int = 200,
    extra_configs: Optional[dict[str, str]] = None,
) -> SparkSession:
    """
    Create a Spark session configured for the current environment.

    In Fabric or Databricks, returns the existing session with additional
    configs. For local development, builds a new session with Delta Lake
    support and sensible defaults.

    Args:
        app_name: Application name for the Spark UI.
        master: Spark master URL (local[*] for local dev).
        executor_memory: Memory per executor (local only).
        driver_memory: Memory for driver (local only).
        shuffle_partitions: Default number of shuffle partitions.
        extra_configs: Additional Spark configuration key-value pairs.

    Returns:
        Configured SparkSession.
    """
    builder = SparkSession.builder.appName(app_name)

    if _is_fabric_runtime() or _is_databricks_runtime():
        spark = builder.getOrCreate()
    else:
        builder = (
            builder
            .master(master)
            .config("spark.executor.memory", executor_memory)
            .config("spark.driver.memory", driver_memory)
        )

        # Delta Lake support for local development
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        spark = builder.getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    if extra_configs:
        for key, value in extra_configs.items():
            spark.conf.set(key, value)

    return spark


def get_or_create_spark_session(
    app_name: str = "EntityResolution",
) -> SparkSession:
    """
    Get the active Spark session or create a new one.

    This is the preferred method for Fabric notebooks, where a Spark
    session is already running and we want to avoid creating duplicates.

    Args:
        app_name: Fallback application name if a new session is needed.

    Returns:
        Active SparkSession.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = create_spark_session(app_name=app_name)
    return spark
