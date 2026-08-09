"""
Phase 15: Master Data Management Distribution

Distributes trusted golden records to downstream consumers via REST APIs,
event streams, and batch exports. Provides the interface layer through
which analytics, operational systems, and AI applications consume
authoritative entity data.

Input: Golden records from Phase 13; stewardship-approved matches
Output: MDM API endpoints; event streams; consumer-ready exports

Medallion Layer: Gold
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.delta_helpers import write_to_delta, resolve_table_path, read_delta
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def prepare_export_view(
    golden_df: DataFrame,
    entity_type: str = "customer",
    include_metadata: bool = False,
    field_selector: Optional[list[str]] = None,
) -> DataFrame:
    """
    Prepare a consumer-friendly export view of golden records.

    Strips internal metadata columns and presents a clean, business-ready
    dataset suitable for downstream consumption.

    Args:
        golden_df: Golden records DataFrame.
        entity_type: Type of entity for field selection.
        include_metadata: Whether to include audit/tracking metadata.
        field_selector: Specific fields to include (None = all non-internal).

    Returns:
        Cleaned DataFrame ready for consumer distribution.
    """
    internal_prefixes = ("_",)

    if field_selector:
        cols = [c for c in field_selector if c in golden_df.columns]
    else:
        if include_metadata:
            cols = golden_df.columns
        else:
            cols = [
                c for c in golden_df.columns
                if not any(c.startswith(p) for p in internal_prefixes)
                or c in ("golden_id",)
            ]

    export = golden_df.select(*cols)

    # Rename golden_id to a consumer-friendly name
    if "golden_id" in export.columns:
        export = export.withColumnRenamed("golden_id", f"{entity_type}_id")

    return export


def export_to_delta(
    df: DataFrame,
    export_path: str,
    export_format: str = "delta",
    partition_by: Optional[list[str]] = None,
) -> None:
    """
    Export golden records to a consumer-facing Delta table.

    Args:
        df: Golden records DataFrame.
        export_path: Target path for export.
        export_format: Format for export ('delta', 'parquet', 'csv').
        partition_by: Optional partition columns.
    """
    writer = df.write.format(export_format).mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if export_format == "csv":
        writer = writer.option("header", "true")

    writer.save(export_path)
    logger.info(f"Exported {df.count()} records to {export_path} ({export_format})")


def build_mdm_api_response(
    golden_df: DataFrame,
    entity_id: str,
    entity_type: str = "customer",
) -> Optional[dict]:
    """
    Build an API response for a single entity lookup.

    Args:
        golden_df: Golden records DataFrame.
        entity_id: Entity identifier to look up.
        entity_type: Type of entity.

    Returns:
        Dict ready for JSON serialization, or None if not found.
    """
    id_col = f"{entity_type}_id" if f"{entity_type}_id" in golden_df.columns else "golden_id"
    if id_col not in golden_df.columns:
        return None

    row = golden_df.filter(F.col(id_col) == entity_id).first()
    if row is None:
        return None

    result = row.asDict()

    # Convert Timestamp to ISO string
    from datetime import datetime
    for key, value in result.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()

    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "attributes": result,
        "links": {
            "self": f"/api/v1/{entity_type}/{entity_id}",
            "history": f"/api/v1/{entity_type}/{entity_id}/history",
            "sources": f"/api/v1/{entity_type}/{entity_id}/sources",
        },
    }


def build_event_payload(
    golden_df: DataFrame,
    change_type: str = "upsert",
    entity_type: str = "customer",
) -> list[dict]:
    """
    Build event payloads for streaming distribution (Kafka/Event Hub).

    Each golden record becomes an event with envelope metadata for
    downstream consumers to process.

    Args:
        golden_df: Golden records DataFrame.
        change_type: Type of change ('upsert', 'delete', 'update').
        entity_type: Type of entity.

    Returns:
        List of event dicts with standard envelope.
    """
    from datetime import datetime, timezone

    events = []
    timestamp = datetime.now(timezone.utc).isoformat()

    for row in golden_df.collect():
        entity_id = row.get("golden_id", row.get("id", ""))
        attributes = {
            k: (v.isoformat() if hasattr(v, "isoformat") else v)
            for k, v in row.asDict().items()
            if not k.startswith("_")
        }

        event = {
            "event_id": f"{entity_id}-{change_type}-{int(datetime.now(timezone.utc).timestamp())}",
            "event_type": f"{entity_type}.{change_type}",
            "entity_type": entity_type,
            "entity_id": str(entity_id),
            "timestamp": timestamp,
            "change_type": change_type,
            "data": attributes,
            "source": "entity-resolution-pipeline",
            "version": "1.0",
        }
        events.append(event)

    return events


def configure_fastapi_app(
    spark: SparkSession,
    golden_path: str,
    entity_type: str = "customer",
    host: str = "0.0.0.0",
    port: int = 8000,
) -> str:
    """
    Generate a FastAPI application configuration for serving MDM data.

    Returns the application code as a string for deployment.
    This is a code generator for the MDM API service.

    Args:
        spark: Active Spark session.
        golden_path: Path to golden records Delta table.
        entity_type: Type of entity.
        host: API server host.
        port: API server port.

    Returns:
        FastAPI application code as a string.
    """
    app_code = f'''"""
MDM Distribution API for {entity_type} Entity Resolution.
Auto-generated by Phase 15: MDM Distribution.
"""

from typing import Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = FastAPI(
    title="Entity Resolution MDM API",
    description=f"Trusted {entity_type} golden records API",
    version="1.0.0",
)

spark = SparkSession.builder \\
    .appName("MDM-API-{entity_type}") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
    .getOrCreate()

GOLDEN_PATH = "{golden_path}"


def _load_golden():
    return spark.read.format("delta").load(GOLDEN_PATH)


@app.get("/health")
async def health_check():
    return {{"status": "healthy", "timestamp": datetime.utcnow().isoformat()}}


@app.get("/api/v1/{entity_type}/{{entity_id}}")
async def get_entity(entity_id: str):
    """Retrieve a single golden record by entity ID."""
    golden = _load_golden()
    row = golden.filter(F.col("golden_id") == entity_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail=f"{entity_type} not found: {{entity_id}}")
    result = row.asDict()
    for k, v in result.items():
        if isinstance(v, datetime):
            result[k] = v.isoformat()
    return {{
        "entity_type": "{entity_type}",
        "entity_id": entity_id,
        "attributes": result,
    }}


@app.get("/api/v1/{entity_type}")
async def list_entities(
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
):
    """List golden records with pagination."""
    golden = _load_golden()
    total = golden.count()
    page = golden.limit(limit).offset(offset)
    results = []
    for row in page.collect():
        d = row.asDict()
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        results.append(d)
    return {{
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": results,
    }}


@app.get("/api/v1/{entity_type}/{{entity_id}}/history")
async def get_entity_history(entity_id: str):
    """Retrieve change history for an entity."""
    golden = spark.read.format("delta") \\
        .option("versionAsOf", 0) \\
        .load(GOLDEN_PATH)
    history = golden.filter(F.col("golden_id") == entity_id)
    return {{
        "entity_type": "{entity_type}",
        "entity_id": entity_id,
        "versions": history.count(),
        "latest": {{"entity_id": entity_id}},
    }}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="{host}", port={port})
'''
    return app_code


def run(
    spark: SparkSession,
    golden_df: Optional[DataFrame] = None,
    golden_path: Optional[str] = None,
    entity_type: str = "customer",
    config: Optional[dict] = None,
    gold_path: str = "Tables/gold/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> dict:
    """
    Execute Phase 15: distribute golden records to consumers.

    Args:
        spark: Active Spark session.
        golden_df: Golden records DataFrame from Phase 13.
        golden_path: Path to golden records if loading from storage.
        entity_type: Type of entity.
        config: Dict with:
            - export_formats: List of export formats ('delta', 'parquet', 'csv').
            - export_partitions: Partition columns for exports.
            - api_enabled: Whether to generate API configuration.
            - event_stream_enabled: Whether to generate event payloads.
            - consumer_paths: Dict of consumer-specific export paths.
        gold_path: Gold layer base path.
        table_name: Target base table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        Dict with export paths, API config, and event payloads.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="15-mdm-distribution")

    config = config or {}
    logger.info(f"Distributing golden records for {entity_type}")

    # Load golden records if not provided
    if golden_df is None and golden_path:
        golden_df = read_delta(spark, golden_path)

    if golden_df is None:
        logger.error("No golden records available for distribution")
        return {"error": "No golden records"}

    golden_count = golden_df.count()
    result: dict = {"export_paths": [], "events": [], "api_config": None}

    # Prepare consumer-friendly export
    field_selector = config.get("field_selector")
    export_df = prepare_export_view(
        golden_df,
        entity_type=entity_type,
        include_metadata=config.get("include_metadata", False),
        field_selector=field_selector,
    )

    # Export in configured formats
    export_formats = config.get("export_formats", ["delta"])
    export_partitions = config.get("export_partitions")
    consumer_paths = config.get("consumer_paths", {})

    for fmt in export_formats:
        if fmt in consumer_paths:
            path = consumer_paths[fmt]
        else:
            path = resolve_table_path(
                layer="gold",
                table_name=f"{table_name}_export_{fmt}",
                workspace=workspace,
            )
        export_to_delta(export_df, path, export_format=fmt, partition_by=export_partitions)
        result["export_paths"].append({"format": fmt, "path": path})
        logger.info(f"Exported golden records to {path} ({fmt})")

    # Generate event payloads for streaming
    if config.get("event_stream_enabled", True):
        events = build_event_payload(golden_df, change_type="upsert", entity_type=entity_type)
        result["events"] = events
        logger.info(f"Generated {len(events)} event payloads")

    # Generate API configuration
    if config.get("api_enabled", True):
        golden_table_path = resolve_table_path(
            layer="gold",
            table_name=f"{table_name}_golden",
            workspace=workspace,
        )
        api_config = configure_fastapi_app(
            spark,
            golden_path=golden_table_path,
            entity_type=entity_type,
            host=config.get("api_host", "0.0.0.0"),
            port=config.get("api_port", 8000),
        )
        result["api_config"] = api_config
        logger.info("Generated FastAPI MDM distribution configuration")

    # Write the export view
    export_view_path = resolve_table_path(
        layer="gold",
        table_name=f"{table_name}_consumer_view",
        workspace=workspace,
    )
    write_to_delta(export_df, export_view_path, mode="overwrite")

    # Emit metrics
    metrics.log_count("golden_records_distributed", golden_count)
    metrics.log_count("export_formats", len(export_formats))
    metrics.log_count("event_payloads", len(result["events"]))
    metrics.flush()

    logger.info(
        f"Distributed {golden_count} golden records in {len(export_formats)} formats, "
        f"{len(result['events'])} events"
    )
    return result
