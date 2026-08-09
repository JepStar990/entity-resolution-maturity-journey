"""
Phase 1: Basic Data Ingestion

Ingests entity data from diverse source systems into the Bronze layer,
preserving raw fidelity while adding metadata columns (source system,
ingestion timestamp, batch ID).

Input: Source systems (CSV, Parquet, JSON, JDBC, REST APIs)
Output: Bronze Delta tables with metadata enrichment

Medallion Layer: Bronze
"""

from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from utils.delta_helpers import write_to_delta, resolve_table_path, table_exists
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def ingest_csv(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    header: bool = True,
    delimiter: str = ",",
    **reader_options,
) -> DataFrame:
    """
    Ingest a CSV file into a Spark DataFrame.

    Args:
        spark: Active Spark session.
        path: Path to CSV file or directory.
        schema: Optional explicit schema.
        header: Whether the CSV has a header row.
        delimiter: Field delimiter.
        **reader_options: Additional Spark CSV reader options.

    Returns:
        DataFrame with raw CSV contents.
    """
    reader = spark.read.option("header", str(header).lower()).option("sep", delimiter)
    for key, value in reader_options.items():
        reader = reader.option(key, str(value))
    if schema:
        reader = reader.schema(schema)
    logger.info(f"Ingesting CSV from: {path}")
    return reader.csv(path)


def ingest_parquet(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """
    Ingest Parquet files into a Spark DataFrame.

    Args:
        spark: Active Spark session.
        path: Path to Parquet file or directory.
        schema: Optional explicit schema for validation.

    Returns:
        DataFrame with Parquet contents.
    """
    reader = spark.read
    if schema:
        reader = reader.schema(schema)
    logger.info(f"Ingesting Parquet from: {path}")
    return reader.parquet(path)


def ingest_json(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    multiline: bool = False,
) -> DataFrame:
    """
    Ingest JSON files into a Spark DataFrame.

    Args:
        spark: Active Spark session.
        path: Path to JSON file or directory.
        schema: Optional explicit schema.
        multiline: Whether JSON records span multiple lines.

    Returns:
        DataFrame with parsed JSON contents.
    """
    reader = spark.read.option("multiline", str(multiline).lower())
    if schema:
        reader = reader.schema(schema)
    logger.info(f"Ingesting JSON from: {path}")
    return reader.json(path)


def ingest_jdbc(
    spark: SparkSession,
    url: str,
    table: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    partition_column: Optional[str] = None,
    num_partitions: int = 8,
    **jdbc_options,
) -> DataFrame:
    """
    Ingest data from a JDBC source (SQL Server, PostgreSQL, etc.).

    Args:
        spark: Active Spark session.
        url: JDBC connection URL.
        table: Table or subquery to read.
        user: Database user.
        password: Database password.
        driver: JDBC driver class name.
        partition_column: Column for parallel reads.
        num_partitions: Number of partitions for parallel reads.
        **jdbc_options: Additional JDBC options.

    Returns:
        DataFrame with JDBC query results.
    """
    options = {
        "url": url,
        "dbtable": table,
        "driver": driver,
        **jdbc_options,
    }
    if user:
        options["user"] = user
    if password:
        options["password"] = password
    if partition_column:
        options["partitionColumn"] = partition_column
        options["numPartitions"] = str(num_partitions)

    logger.info(f"Ingesting JDBC table: {table} from {url}")
    return spark.read.format("jdbc").options(**options).load()


def add_ingestion_metadata(
    df: DataFrame,
    source_system: str,
    batch_id: Optional[str] = None,
) -> DataFrame:
    """
    Add metadata columns required for Bronze layer tracking.

    Columns added:
    - _ingested_at: UTC timestamp of ingestion.
    - _source_system: Identifier of the source system.
    - _batch_id: Unique batch identifier for lineage tracking.
    - _source_file: Original file name (if available).

    Args:
        df: Source DataFrame.
        source_system: Human-readable source system name.
        batch_id: Unique batch identifier (auto-generated if None).

    Returns:
        DataFrame with metadata columns appended.
    """
    if batch_id is None:
        batch_id = f"{source_system}-{int(datetime.now(timezone.utc).timestamp())}"

    now = datetime.now(timezone.utc).isoformat()

    enriched = (
        df
        .withColumn("_ingested_at", F.lit(now).cast("timestamp"))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # Preserve source file name if the _metadata column exists (autoloader pattern)
    if "_metadata" in df.columns:
        enriched = enriched.withColumn(
            "_source_file",
            F.col("_metadata.file_name"),
        )

    return enriched


def run(
    spark: SparkSession,
    source_config: dict,
    bronze_path: str,
    source_system: str,
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> DataFrame:
    """
    Execute Phase 1: ingest data and write to Bronze layer.

    Args:
        spark: Active Spark session.
        source_config: Dict with keys:
            - type: 'csv', 'parquet', 'json', or 'jdbc'
            - path/url: Source location
            - options: Dict of reader-specific options
            - table_name: Target Bronze table name
        bronze_path: Base path for Bronze layer Delta tables.
        source_system: Identifier for the source system.
        workspace: Fabric workspace name for OneLake path resolution.
        metrics: Optional MetricsCollector for run instrumentation.

    Returns:
        DataFrame of the ingested data written to Bronze.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="01-ingestion")

    source_type = source_config["type"]
    table_name = source_config["table_name"]
    options = source_config.get("options", {})

    logger.info(f"Ingesting {source_type} for table: {table_name}")

    # Read from source
    if source_type == "csv":
        df = ingest_csv(spark, source_config["path"], **options)
    elif source_type == "parquet":
        df = ingest_parquet(spark, source_config["path"])
    elif source_type == "json":
        df = ingest_json(spark, source_config["path"], **options)
    elif source_type == "jdbc":
        df = ingest_jdbc(spark, source_config["url"], source_config["table"], **options)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    # Enrich with metadata
    df = add_ingestion_metadata(df, source_system)

    # Write to Bronze
    target_path = resolve_table_path(
        layer="bronze",
        table_name=table_name,
        workspace=workspace,
    )

    if table_exists(spark, target_path):
        write_mode = "append"
    else:
        write_mode = "overwrite"

    write_to_delta(df, target_path, mode=write_mode, partition_by=["_source_system"])

    # Collect metrics
    record_count = df.count()
    metrics.log_count("ingested_records", record_count)
    metrics.log_metadata("source_type", source_type)
    metrics.log_metadata("target_table", table_name)
    metrics.flush()

    logger.info(f"Ingested {record_count} records into {target_path}")
    return df
