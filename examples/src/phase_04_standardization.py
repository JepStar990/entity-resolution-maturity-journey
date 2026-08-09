"""
Phase 4: Standardization

Standardizes entity attributes to consistent formats for reliable matching.
Handles names, phone numbers, addresses, dates, and company identifiers.

Input: Silver tables with quality-passed records
Output: Silver tables with standardized text/date/numeric columns

Medallion Layer: Silver
"""

from __future__ import annotations

import re
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def standardize_names(col: str) -> F.Column:
    """
    Standardize a name column: trim, collapse whitespace, title case.

    Args:
        col: Column name to standardize.

    Returns:
        Spark Column expression.
    """
    return (
        F.trim(F.regexp_replace(F.col(col), r"\s+", " "))
        .cast(StringType())
    )


def standardize_phone(col: str, default_country_code: str = "1") -> F.Column:
    """
    Standardize a phone number to E.164-like format.

    Strips non-numeric characters, handles common country code patterns.

    Args:
        col: Column name containing phone number.
        default_country_code: Default country code when missing.

    Returns:
        Spark Column expression with standardized phone number.
    """
    digits = F.regexp_replace(F.col(col), r"[^\d+]", "")
    return (
        F.when(
            digits.startswith("+"),
            digits,
        )
        .when(
            F.length(digits) == 10,
            F.concat(F.lit(f"+{default_country_code}"), digits),
        )
        .when(
            (F.length(digits) == 11) & digits.startswith("1"),
            F.concat(F.lit("+"), digits),
        )
        .otherwise(digits)
    )


def standardize_date(col: str, input_format: Optional[str] = None) -> F.Column:
    """
    Standardize a date column to ISO 8601 format.

    Args:
        col: Column name containing a date string.
        input_format: Optional Spark date format string.

    Returns:
        Spark Column expression with ISO date string, or null if invalid.
    """
    if input_format:
        return F.date_format(F.to_date(F.col(col), input_format), "yyyy-MM-dd")
    return F.date_format(F.to_date(F.col(col)), "yyyy-MM-dd")


def standardize_email(col: str) -> F.Column:
    """
    Standardize an email address: trim, lowercase.

    Args:
        col: Column name containing email address.

    Returns:
        Spark Column expression.
    """
    return F.lower(F.trim(F.col(col)))


def standardize_address(
    street_col: str,
    city_col: str,
    state_col: str,
    zip_col: str,
    country_col: str,
) -> dict[str, F.Column]:
    """
    Standardize address components to consistent US formats.

    Args:
        street_col: Street address column.
        city_col: City column.
        state_col: State/province column.
        zip_col: ZIP/postal code column.
        country_col: Country column.

    Returns:
        Dict mapping column names to standardized Column expressions.
    """
    return {
        "address_street": F.trim(F.upper(F.regexp_replace(F.col(street_col), r"\s+", " "))),
        "address_city": F.trim(F.upper(F.regexp_replace(F.col(city_col), r"\s+", " "))),
        "address_state": F.when(
            F.length(F.trim(F.col(state_col))) == 2,
            F.upper(F.trim(F.col(state_col))),
        ).otherwise(F.trim(F.col(state_col))),
        "address_zip5": F.regexp_extract(F.col(zip_col), r"(\d{5})", 1),
        "address_country": F.when(
            F.length(F.trim(F.col(country_col))) == 2,
            F.upper(F.trim(F.col(country_col))),
        ).otherwise(F.trim(F.col(country_col))),
    }


def standardize_company_name(col: str) -> F.Column:
    """
    Standardize a company name: uppercase, strip suffixes and punctuation.

    Handles common legal entity suffixes (Inc, Corp, Ltd, LLC, etc.).

    Args:
        col: Column name containing company name.

    Returns:
        Spark Column expression with standardized company name.
    """
    suffixes = r"\b(INC|CORP|CORPORATION|LTD|LIMITED|LLC|LP|LLP|PLC|PTY|SA|AG|GMBH|BV|NV|CO|COMPANY)\.?$"
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.upper(F.regexp_replace(F.col(col), r"\s+", " ")),
                r"[.,;:]",
                "",
            ),
            suffixes,
            "",
        )
    )


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
    Execute Phase 4: standardize entity attributes.

    Args:
        spark: Active Spark session.
        df: Input DataFrame from Phase 3 (quality-passed Silver records).
        entity_type: Type of entity (customer, product, supplier, company).
        config: Optional field mapping configuration.
        silver_path: Silver layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        DataFrame with standardized columns.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="04-standardization")

    logger.info(f"Standardizing {entity_type} entity attributes")

    result = df

    if entity_type in ("customer", "contact", "supplier"):
        if "first_name" in df.columns:
            result = result.withColumn(
                "first_name_std",
                standardize_names("first_name"),
            )
        if "last_name" in df.columns:
            result = result.withColumn(
                "last_name_std",
                standardize_names("last_name"),
            )
        if "name" in df.columns and "full_name_std" not in result.columns:
            result = result.withColumn(
                "full_name_std",
                standardize_names("name"),
            )
        if "phone" in df.columns:
            result = result.withColumn(
                "phone_std",
                standardize_phone("phone"),
            )
        if "email" in df.columns:
            result = result.withColumn(
                "email_std",
                standardize_email("email"),
            )

        # Try to find address columns by common naming patterns
        addr_cols = {c.lower(): c for c in df.columns}
        has_address = any(
            part in addr_cols
            for part in ("street", "address", "city", "state", "zip", "country")
        )
        if has_address:
            street = addr_cols.get("street", addr_cols.get("address", "address"))
            city = addr_cols.get("city", "city")
            state = addr_cols.get("state", addr_cols.get("province", "state"))
            zipcol = addr_cols.get("zip", addr_cols.get("postal_code", "zip"))
            country = addr_cols.get("country", "country")

            addr_std = standardize_address(street, city, state, zipcol, country)
            for col_name, expr in addr_std.items():
                result = result.withColumn(col_name, expr)

    elif entity_type == "company":
        if "company_name" in df.columns:
            result = result.withColumn(
                "company_name_std",
                standardize_company_name("company_name"),
            )
        if "ticker" in df.columns:
            result = result.withColumn(
                "ticker_std",
                F.upper(F.trim(F.col("ticker"))),
            )

    elif entity_type == "product":
        if "product_name" in df.columns:
            result = result.withColumn(
                "product_name_std",
                F.trim(F.upper(F.regexp_replace(F.col("product_name"), r"\s+", " "))),
            )
        if "sku" in df.columns:
            result = result.withColumn(
                "sku_std",
                F.trim(F.upper(F.col("sku"))),
            )

    # Write standardized data to Silver
    target_path = resolve_table_path(
        layer="silver",
        table_name=f"{table_name}_standardized",
        workspace=workspace,
    )
    write_to_delta(result, target_path, mode="overwrite")

    record_count = result.count()
    metrics.log_count("standardized_records", record_count)
    metrics.flush()

    logger.info(f"Standardized {record_count} {entity_type} records")
    return result
