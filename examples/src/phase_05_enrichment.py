"""
Phase 5: Data Enrichment

Enriches standardized Silver records with reference data: postal codes,
industry codes, firmographics, and other external datasets. Applies
slowly changing dimension (SCD) logic to reference data.

Input: Standardized Silver tables
Output: Enriched Silver tables with reference data joined

Medallion Layer: Silver
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.delta_helpers import write_to_delta, resolve_table_path, read_delta
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def enrich_with_reference(
    df: DataFrame,
    reference_df: DataFrame,
    join_key: str,
    ref_join_key: str,
    select_columns: Optional[list[str]] = None,
    join_type: str = "left",
) -> DataFrame:
    """
    Join enrichment data from a reference table.

    Args:
        df: Main entity DataFrame.
        reference_df: Reference/enrichment DataFrame.
        join_key: Column in main DataFrame to join on.
        ref_join_key: Column in reference DataFrame to join on.
        select_columns: Specific reference columns to add (None = all).
        join_type: Type of join (left, inner, etc.).

    Returns:
        Enriched DataFrame.
    """
    if select_columns:
        ref_subset = reference_df.select(
            ref_join_key,
            *[c for c in select_columns if c != ref_join_key],
        )
    else:
        ref_subset = reference_df

    # Prefix reference columns to avoid collisions
    for col_name in ref_subset.columns:
        if col_name != ref_join_key and col_name in df.columns:
            ref_subset = ref_subset.withColumnRenamed(
                col_name, f"ref_{col_name}"
            )

    enriched = df.join(ref_subset, df[join_key] == ref_subset[ref_join_key], join_type)
    if join_type == "left":
        enriched = enriched.drop(ref_join_key)

    return enriched


def enrich_postal_code(
    spark: SparkSession,
    df: DataFrame,
    postal_ref_path: str,
    zip_column: str = "address_zip5",
) -> DataFrame:
    """
    Enrich records with postal code reference data (city, state, lat/lon).

    Args:
        spark: Active Spark session.
        df: Entity DataFrame with zip column.
        postal_ref_path: Path to postal code reference Delta table.
        zip_column: Column containing 5-digit ZIP code.

    Returns:
        DataFrame with postal metadata columns added.
    """
    postal_ref = read_delta(spark, postal_ref_path)
    return enrich_with_reference(
        df,
        postal_ref,
        join_key=zip_column,
        ref_join_key="zip_code",
        select_columns=[
            "zip_code", "city", "state", "county",
            "latitude", "longitude", "timezone",
        ],
        join_type="left",
    )


def enrich_industry_codes(
    spark: SparkSession,
    df: DataFrame,
    industry_ref_path: str,
    sic_column: str = "sic_code",
    naics_column: Optional[str] = "naics_code",
) -> DataFrame:
    """
    Enrich records with industry classification descriptions.

    Args:
        spark: Active Spark session.
        df: Entity DataFrame.
        industry_ref_path: Path to industry code reference Delta table.
        sic_column: Column containing SIC code.
        naics_column: Column containing NAICS code.

    Returns:
        DataFrame with industry descriptions appended.
    """
    industry_ref = read_delta(spark, industry_ref_path)

    result = df
    if sic_column and sic_column in df.columns:
        result = enrich_with_reference(
            result,
            industry_ref,
            join_key=sic_column,
            ref_join_key="sic_code",
            select_columns=["sic_code", "sic_description", "sic_sector"],
            join_type="left",
        )

    if naics_column and naics_column in df.columns:
        naics_ref = industry_ref.select("naics_code", "naics_description").distinct()
        result = enrich_with_reference(
            result,
            naics_ref,
            join_key=naics_column,
            ref_join_key="naics_code",
            select_columns=["naics_code", "naics_description"],
            join_type="left",
        )

    return result


def apply_scd_type2(
    df: DataFrame,
    existing_df: DataFrame,
    business_keys: list[str],
    effective_date_col: str = "updated_at",
) -> DataFrame:
    """
    Apply Slowly Changing Dimension Type 2 to enrichment data.

    Tracks history by adding effective_date, end_date, and is_current
    columns. Expired records are end-dated rather than overwritten.

    Args:
        df: Incoming DataFrame with new/updated records.
        existing_df: Existing dimension records.
        business_keys: Columns that uniquely identify a logical entity.
        effective_date_col: Column indicating when the record became effective.

    Returns:
        DataFrame with SCD Type 2 applied (includes both current and historical rows).
    """
    # Mark incoming records as current
    incoming = df.withColumn("is_current", F.lit(True))
    incoming = incoming.withColumn("effective_date", F.col(effective_date_col))
    incoming = incoming.withColumn("end_date", F.lit(None).cast("date"))

    # Build join condition on business keys
    join_condition = None
    for key in business_keys:
        cond = existing_df[key] == incoming[key]
        join_condition = cond if join_condition is None else join_condition & cond

    # Find records that need to be expired
    window_spec = Window.partitionBy(*business_keys).orderBy(
        F.col("effective_date").desc()
    )

    merged = incoming.unionByName(existing_df, allowMissingColumns=True)

    merged = merged.withColumn(
        "end_date",
        F.lead("effective_date", 1).over(window_spec),
    ).withColumn(
        "is_current",
        F.when(F.col("end_date").isNull(), F.lit(True)).otherwise(F.lit(False)),
    )

    return merged


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
    Execute Phase 5: enrich standardized records with reference data.

    Args:
        spark: Active Spark session.
        df: Input DataFrame from Phase 4 (standardized Silver records).
        entity_type: Type of entity for enrichment logic selection.
        config: Dict with enrichment configuration:
            - postal_ref_path: Path to postal code reference table.
            - industry_ref_path: Path to industry code reference table.
            - reference_tables: Dict of custom reference table configs.
        silver_path: Silver layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        Enriched DataFrame.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="05-enrichment")

    config = config or {}
    logger.info(f"Enriching {entity_type} records")

    result = df

    # Postal code enrichment
    postal_ref_path = config.get("postal_ref_path")
    if postal_ref_path:
        zip_col = config.get("zip_column", "address_zip5")
        if zip_col in result.columns:
            result = enrich_postal_code(spark, result, postal_ref_path, zip_col)
            logger.info("Applied postal code enrichment")

    # Industry code enrichment
    industry_ref_path = config.get("industry_ref_path")
    if industry_ref_path:
        result = enrich_industry_codes(spark, result, industry_ref_path)
        logger.info("Applied industry code enrichment")

    # Custom reference tables from config
    reference_tables = config.get("reference_tables", [])
    for ref_config in reference_tables:
        ref_df = read_delta(spark, ref_config["path"])
        result = enrich_with_reference(
            result,
            ref_df,
            join_key=ref_config["join_key"],
            ref_join_key=ref_config["ref_join_key"],
            select_columns=ref_config.get("select_columns"),
            join_type=ref_config.get("join_type", "left"),
        )
        logger.info(f"Applied enrichment from {ref_config['path']}")

    # Write enriched data to Silver
    target_path = resolve_table_path(
        layer="silver",
        table_name=f"{table_name}_enriched",
        workspace=workspace,
    )
    write_to_delta(result, target_path, mode="overwrite")

    record_count = result.count()
    enrichment_count = len(reference_tables) + bool(postal_ref_path) + bool(industry_ref_path)
    metrics.log_count("enriched_records", record_count)
    metrics.log_count("enrichment_sources_applied", enrichment_count)
    metrics.flush()

    logger.info(f"Enriched {record_count} records with {enrichment_count} sources")
    return result
