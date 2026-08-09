"""
Dynamic Data Profiler

Auto-detects entity types, column roles, data characteristics, and quality
issues from any DataFrame. No hardcoded column names or entity types.

The profiler analyzes column names, data types, value distributions, and
patterns to infer:
- Entity type (customer, product, company, supplier, or generic)
- Column roles (id, name, email, phone, address components, dates, etc.)
- Data quality metrics and recommended rules
- Optimal matching strategies based on data cardinality and distribution

Usage:
    from engine.data_profiler import DataProfiler
    profiler = DataProfiler(spark)
    profile = profiler.profile(df)
    print(profile.entity_type)  # 'customer'
    print(profile.column_roles) # {'id': 'id', 'email': 'email', ...}
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    DateType, TimestampType, BooleanType, DecimalType,
)

from utils.logging_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Column role patterns — ordered by specificity (most specific first)
# ---------------------------------------------------------------------------
COLUMN_ROLE_PATTERNS: list[tuple[str, list[str], dict]] = [
    # (role, name_patterns, type_preferences)
    ("email", [
        r"^email", r"^e_mail", r"^e-mail", r"email_address", r"^mail$",
        r"contact_email", r"primary_email",
    ], {"prefer_type": "string"}),
    ("phone", [
        r"^phone", r"^tel", r"^mobile", r"^cell", r"^fax",
        r"contact_number", r"phone_number", r"telephone",
    ], {"prefer_type": "string"}),
    ("url", [
        r"^url$", r"^website", r"^homepage", r"^link$", r"^web$",
        r"^site$", r"^domain",
    ], {"prefer_type": "string"}),
    ("first_name", [
        r"^first_name", r"^firstname", r"^given_name", r"^givenname",
        r"^forename", r"^fname",
    ], {"prefer_type": "string"}),
    ("last_name", [
        r"^last_name", r"^lastname", r"^surname", r"^family_name",
        r"^familyname", r"^lname",
    ], {"prefer_type": "string"}),
    ("full_name", [
        r"^full_name", r"^fullname", r"^name$", r"^display_name",
        r"^displayname", r"^contact_name", r"^contactname",
    ], {"prefer_type": "string"}),
    ("company_name", [
        r"^company", r"^organization", r"^organisation", r"^business",
        r"^corp", r"^firm", r"^vendor_name", r"^supplier_name",
        r"^manufacturer", r"^brand",
    ], {"prefer_type": "string"}),
    ("product_name", [
        r"^product", r"^item_name", r"^goods", r"^merchandise",
        r"^title$", r"^description$", r"^desc$",
    ], {"prefer_type": "string"}),
    ("sku", [
        r"^sku$", r"^stock_code", r"^item_code", r"^product_code",
        r"^part_number", r"^part_no", r"^upc$", r"^ean$", r"^barcode",
    ], {"prefer_type": "string"}),
    ("street_address", [
        r"^street", r"^address1", r"^address_1", r"^addr1",
        r"^line1", r"^line_1", r"^address_line_1",
        r"^address$", r"^addr$", r"^location$",
    ], {"prefer_type": "string"}),
    ("city", [
        r"^city$", r"^town$", r"^municipality", r"^locality",
    ], {"prefer_type": "string"}),
    ("state", [
        r"^state$", r"^province", r"^region$", r"^territory",
    ], {"prefer_type": "string"}),
    ("postal_code", [
        r"^zip", r"^postal", r"^postcode", r"^zipcode",
        r"^zip_code", r"^post_code",
    ], {"prefer_type": "string"}),
    ("country", [
        r"^country$", r"^nation", r"^country_code", r"^countrycode",
    ], {"prefer_type": "string"}),
    ("tax_id", [
        r"^tax_id", r"^taxid", r"^vat", r"^ein$", r"^ssn$", r"^tin$",
        r"^tax_number", r"^registration_number", r"^reg_number",
    ], {"prefer_type": "string"}),
    ("company_id", [
        r"^duns", r"^dun$", r"^duns_number", r"^duns_num",
        r"^ticker$", r"^symbol$", r"^stock_symbol",
        r"^lei$", r"^legal_entity_id",
    ], {"prefer_type": "string"}),
    ("industry_code", [
        r"^sic", r"^naics", r"^industry", r"^sector_code",
        r"^industry_code",
    ], {"prefer_type": "string"}),
    ("price", [
        r"^price$", r"^cost$", r"^amount$", r"^rate$", r"^fee$",
        r"^msrp$", r"^list_price", r"^unit_price", r"^total$",
        r"^revenue$", r"^salary$", r"^income$",
    ], {"prefer_type": "numeric"}),
    ("date_created", [
        r"^created", r"^create_date", r"^creation_date",
        r"^date_created", r"^opened", r"^open_date",
    ], {"prefer_type": "date"}),
    ("date_updated", [
        r"^updated", r"^modified", r"^last_modified", r"^date_modified",
        r"^changed", r"^last_changed", r"^update_date",
    ], {"prefer_type": "date"}),
    ("date_birth", [
        r"^birth", r"^dob$", r"^date_of_birth", r"^birthdate",
    ], {"prefer_type": "date"}),
    ("gender", [
        r"^gender$", r"^sex$",
    ], {"prefer_type": "string"}),
    ("is_active", [
        r"^active$", r"^is_active", r"^isactive", r"^enabled$",
        r"^is_enabled", r"^status$", r"^flag$",
    ], {"prefer_type": "boolean"}),
    ("id", [
        r"^id$", r"_id$", r"^pk$", r"^primary_key", r"^uuid$",
        r"^guid$", r"_key$", r"^code$", r"^identifier$",
        r"^row_id", r"^record_id", r"^object_id",
    ], {"prefer_type": "string"}),
]


# Entity type detection patterns
ENTITY_TYPE_PATTERNS: dict[str, dict] = {
    "customer": {
        "required_roles": [],
        "strong_indicators": [
            "email", "first_name", "last_name", "full_name", "phone",
            "date_birth", "gender",
        ],
        "weight": 1.0,
    },
    "product": {
        "required_roles": [],
        "strong_indicators": [
            "product_name", "sku", "price",
        ],
        "weight": 0.9,
    },
    "company": {
        "required_roles": [],
        "strong_indicators": [
            "company_name", "company_id", "industry_code", "tax_id",
        ],
        "weight": 0.9,
    },
    "supplier": {
        "required_roles": [],
        "strong_indicators": [
            "company_name", "email", "phone",
        ],
        "weight": 0.7,
    },
}


@dataclass
class ColumnProfile:
    """Profile of a single column in the dataset."""
    name: str
    dtype: str
    role: Optional[str] = None
    nullable: bool = True
    distinct_count: int = 0
    total_count: int = 0
    null_count: int = 0
    null_ratio: float = 0.0
    distinct_ratio: float = 0.0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    avg_length: Optional[float] = None
    max_length: Optional[int] = None
    sample_values: list[Any] = field(default_factory=list)
    is_pii: bool = False
    quality_issues: list[str] = field(default_factory=list)


@dataclass
class DatasetProfile:
    """Complete profile of a dataset with inferred characteristics."""
    entity_type: str = "generic"
    entity_type_confidence: float = 0.0
    total_rows: int = 0
    total_columns: int = 0
    columns: dict[str, ColumnProfile] = field(default_factory=dict)
    column_roles: dict[str, str] = field(default_factory=dict)
    primary_key_candidates: list[str] = field(default_factory=list)
    match_key_recommendations: dict[str, list[str]] = field(default_factory=dict)
    quality_recommendations: list[dict] = field(default_factory=list)
    dedup_strategy: str = "standard"
    blocking_strategy: str = "sorted_neighborhood"
    blocking_key_recommendation: Optional[str] = None
    fuzzy_threshold_recommendation: float = 0.85
    source_system: str = "unknown"
    profiled_at: str = ""


class DataProfiler:
    """
    Profiles any DataFrame to auto-detect entity type, column roles,
    data quality issues, and optimal matching strategies.

    Works with ANY dataset — no hardcoded column names or assumptions.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def profile(self, df: DataFrame, source_system: str = "unknown") -> DatasetProfile:
        """
        Profile a DataFrame and return a complete DatasetProfile.

        Args:
            df: Any Spark DataFrame.
            source_system: Label for the data source.

        Returns:
            Complete DatasetProfile with entity type, column roles, and recommendations.
        """
        from datetime import datetime, timezone

        profile = DatasetProfile(
            source_system=source_system,
            profiled_at=datetime.now(timezone.utc).isoformat(),
        )

        # Cache for profiling
        df.cache()

        profile.total_rows = df.count()
        profile.total_columns = len(df.columns)

        if profile.total_rows == 0:
            logger.warning("Empty DataFrame; skipping profiling")
            df.unpersist()
            return profile

        # Profile each column
        for col_name, dtype in df.dtypes:
            col_profile = self._profile_column(df, col_name, dtype, profile.total_rows)
            profile.columns[col_name] = col_profile

        # Infer column roles
        profile.column_roles = self._infer_column_roles(profile.columns)

        # Detect entity type
        profile.entity_type, profile.entity_type_confidence = self._detect_entity_type(
            profile.columns, profile.column_roles
        )

        # Identify primary key candidates
        profile.primary_key_candidates = self._find_primary_key_candidates(
            profile.columns, profile.total_rows
        )

        # Generate match key recommendations
        profile.match_key_recommendations = self._recommend_match_keys(
            profile.column_roles, profile.columns, profile.entity_type
        )

        # Generate quality recommendations
        profile.quality_recommendations = self._recommend_quality_rules(
            profile.columns, profile.column_roles
        )

        # Recommend blocking strategy
        profile.blocking_strategy = self._recommend_blocking_strategy(
            profile.columns, profile.column_roles, profile.total_rows
        )

        # Recommend blocking key
        profile.blocking_key_recommendation = self._recommend_blocking_key(
            profile.column_roles
        )

        # Recommend dedup strategy based on cardinality
        profile.dedup_strategy = self._recommend_dedup_strategy(
            profile.columns, profile.column_roles, profile.total_rows
        )

        # Recommend fuzzy threshold based on data cleanliness
        profile.fuzzy_threshold_recommendation = self._recommend_fuzzy_threshold(
            profile.columns
        )

        df.unpersist()
        logger.info(
            f"Profiled {profile.total_rows} rows, "
            f"detected entity_type={profile.entity_type} "
            f"(confidence={profile.entity_type_confidence:.2f}), "
            f"{len(profile.column_roles)} column roles assigned"
        )
        return profile

    def _profile_column(
        self, df: DataFrame, col_name: str, dtype: str, total_rows: int
    ) -> ColumnProfile:
        """Profile a single column comprehensively."""
        col = F.col(col_name)
        profile = ColumnProfile(name=col_name, dtype=dtype, total_count=total_rows)

        # Null count and ratio
        null_count = df.filter(col.isNull()).count()
        profile.null_count = null_count
        profile.null_ratio = null_count / total_rows if total_rows > 0 else 0.0
        profile.nullable = null_count > 0

        # Distinct count and ratio
        distinct_count = df.select(col).distinct().count()
        profile.distinct_count = distinct_count
        profile.distinct_ratio = distinct_count / total_rows if total_rows > 0 else 0.0

        # String-specific profiling
        if dtype == "string":
            length_stats = df.select(
                F.avg(F.length(F.coalesce(col, F.lit("")))).alias("avg_len"),
                F.max(F.length(F.coalesce(col, F.lit("")))).alias("max_len"),
            ).first()
            if length_stats:
                profile.avg_length = length_stats["avg_len"] or 0.0
                profile.max_length = length_stats["max_len"] or 0

            # Sample non-null values
            samples = df.select(col).filter(col.isNotNull()).limit(5).collect()
            profile.sample_values = [r[col_name] for r in samples]

        # Numeric profiling
        if dtype in ("int", "bigint", "double", "float", "decimal"):
            stats = df.select(
                F.min(col).alias("min_val"),
                F.max(col).alias("max_val"),
            ).first()
            if stats:
                profile.min_value = stats["min_val"]
                profile.max_value = stats["max_val"]

        # Detect PII
        profile.is_pii = self._detect_pii(col_name, profile.sample_values)

        # Detect quality issues
        profile.quality_issues = self._detect_quality_issues(profile, df, col_name)

        return profile

    def _infer_column_roles(
        self, columns: dict[str, ColumnProfile]
    ) -> dict[str, str]:
        """Infer the semantic role of each column from its name and data profile."""
        roles: dict[str, str] = {}
        assigned = set()

        # Pass 1: Match by column name patterns (most specific first)
        for role, patterns, _ in COLUMN_ROLE_PATTERNS:
            for col_name in columns:
                if col_name in assigned:
                    continue
                col_lower = col_name.lower().replace(" ", "_").replace("-", "_")
                for pattern in patterns:
                    if re.search(pattern, col_lower, re.IGNORECASE):
                        roles[col_name] = role
                        assigned.add(col_name)
                        break

        # Pass 2: Use data profile heuristics for unassigned columns
        for col_name, col_prof in columns.items():
            if col_name in assigned:
                continue
            # High-cardinality string with low null rate -> potential ID
            if (
                col_prof.dtype == "string"
                and col_prof.distinct_ratio > 0.9
                and col_prof.null_ratio < 0.1
                and col_prof.avg_length
                and col_prof.avg_length > 8
            ):
                roles[col_name] = "id"
                assigned.add(col_name)

        return roles

    def _detect_entity_type(
        self,
        columns: dict[str, ColumnProfile],
        roles: dict[str, str],
    ) -> tuple[str, float]:
        """Detect the entity type from column roles and data patterns."""
        scores: dict[str, float] = {"generic": 0.1}

        for entity_type, config in ENTITY_TYPE_PATTERNS.items():
            score = 0.0
            indicators = config["strong_indicators"]
            matched = sum(
                1 for role in roles.values() if role in indicators
            )
            if indicators:
                score = matched / len(indicators)
            score *= config["weight"]
            scores[entity_type] = score

        best_type = max(scores, key=scores.get)
        best_confidence = scores[best_type]

        return best_type, best_confidence

    def _find_primary_key_candidates(
        self,
        columns: dict[str, ColumnProfile],
        total_rows: int,
    ) -> list[str]:
        """Identify columns that could serve as primary keys."""
        candidates = []
        for col_name, prof in columns.items():
            # Primary key: high cardinality, low null rate
            if (
                prof.distinct_ratio > 0.95
                and prof.null_ratio < 0.05
                and prof.distinct_count >= total_rows * 0.95
            ):
                candidates.append(col_name)
        # Sort by highest distinct ratio
        candidates.sort(
            key=lambda c: columns[c].distinct_ratio,
            reverse=True,
        )
        return candidates

    def _recommend_match_keys(
        self,
        roles: dict[str, str],
        columns: dict[str, ColumnProfile],
        entity_type: str,
    ) -> dict[str, list[str]]:
        """Recommend matching key sets at different strictness levels."""
        # Build role -> column mapping
        role_cols: dict[str, list[str]] = {}
        for col_name, role in roles.items():
            role_cols.setdefault(role, []).append(col_name)

        strict_keys = []
        medium_keys = []
        loose_keys = []

        # Strict: unique identifiers (email, tax_id, sku, company_id)
        for role_name in ("email", "tax_id", "sku", "company_id"):
            strict_keys.extend(role_cols.get(role_name, []))

        # Medium: name combinations
        name_cols = []
        for role_name in ("full_name", "company_name", "product_name"):
            name_cols.extend(role_cols.get(role_name, []))
        if name_cols:
            medium_keys = name_cols
        else:
            # Fallback: any name-like column
            medium_keys = [
                c for c, r in roles.items()
                if "name" in r and c not in strict_keys
            ]

        # Loose: name + location
        location_cols = []
        for role_name in ("postal_code", "city", "state", "country"):
            location_cols.extend(role_cols.get(role_name, []))
        if name_cols and location_cols:
            loose_keys = [name_cols[0], location_cols[0]]

        result = {}
        if strict_keys:
            result["strict"] = strict_keys
        if medium_keys:
            result["medium"] = medium_keys
        if loose_keys:
            result["loose"] = loose_keys

        return result

    def _recommend_quality_rules(
        self,
        columns: dict[str, ColumnProfile],
        roles: dict[str, str],
    ) -> list[dict]:
        """Auto-generate data quality rule recommendations."""
        rules = []

        for col_name, prof in columns.items():
            role = roles.get(col_name)

            # Null check for key columns
            if role in ("id", "email", "full_name", "company_name", "product_name"):
                if prof.null_ratio > 0:
                    rules.append({
                        "name": f"{col_name}_not_null",
                        "column": col_name,
                        "rule": "not_null",
                        "severity": "error",
                        "category": "completeness",
                        "current_failure_rate": prof.null_ratio,
                    })

            # Uniqueness check for id columns
            if role == "id" and prof.distinct_ratio < 0.95:
                rules.append({
                    "name": f"{col_name}_unique",
                    "column": col_name,
                    "rule": "unique",
                    "severity": "warning",
                    "category": "uniqueness",
                    "current_failure_rate": 1.0 - prof.distinct_ratio,
                })

            # Email format
            if role == "email" and prof.dtype == "string":
                rules.append({
                    "name": f"{col_name}_email_format",
                    "column": col_name,
                    "rule": "email_format",
                    "severity": "warning",
                    "category": "validity",
                })

            # String length for name columns
            if "name" in (role or "") and prof.avg_length and prof.avg_length < 2:
                rules.append({
                    "name": f"{col_name}_min_length",
                    "column": col_name,
                    "rule": "min_length",
                    "params": {"min": 2},
                    "severity": "warning",
                    "category": "validity",
                })

        return rules

    def _recommend_blocking_strategy(
        self,
        columns: dict[str, ColumnProfile],
        roles: dict[str, str],
        total_rows: int,
    ) -> str:
        """Recommend the best blocking strategy based on data size and columns."""
        has_geo = any(r in ("state", "postal_code", "city") for r in roles.values())
        has_name = any("name" in (r or "") for r in roles.values())

        if total_rows < 10_000:
            return "standard"
        elif has_name:
            return "sorted_neighborhood"
        elif has_geo:
            return "standard"
        else:
            return "canopy"

    def _recommend_blocking_key(self, roles: dict[str, str]) -> Optional[str]:
        """Recommend the best column for sorted neighborhood blocking."""
        for role in ("full_name", "company_name", "product_name"):
            for col_name, r in roles.items():
                if r == role:
                    return col_name
        # Fallback: any name column
        for col_name, r in roles.items():
            if "name" in (r or ""):
                return col_name
        return None

    def _recommend_dedup_strategy(
        self,
        columns: dict[str, ColumnProfile],
        roles: dict[str, str],
        total_rows: int,
    ) -> str:
        """Recommend deduplication strategy."""
        has_high_card_id = any(
            p.distinct_ratio > 0.99
            for c, p in columns.items()
            if roles.get(c) == "id"
        )
        if has_high_card_id and total_rows < 1_000_000:
            return "composite"
        return "standard"

    def _recommend_fuzzy_threshold(
        self, columns: dict[str, ColumnProfile]
    ) -> float:
        """Recommend fuzzy matching threshold based on data cleanliness."""
        avg_null_ratio = sum(p.null_ratio for p in columns.values()) / max(len(columns), 1)
        if avg_null_ratio < 0.05:
            return 0.90
        elif avg_null_ratio < 0.15:
            return 0.85
        else:
            return 0.80

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _detect_pii(
        self, col_name: str, sample_values: list[Any]
    ) -> bool:
        """Heuristic PII detection."""
        pii_patterns = [
            r"email", r"phone", r"ssn", r"passport", r"credit_card",
            r"dob", r"birth", r"salary", r"income",
        ]
        for pattern in pii_patterns:
            if re.search(pattern, col_name, re.IGNORECASE):
                return True
        # Check sample values for email patterns
        for val in sample_values:
            if isinstance(val, str) and "@" in val:
                return True
        return False

    def _detect_quality_issues(
        self,
        profile: ColumnProfile,
        df: DataFrame,
        col_name: str,
    ) -> list[str]:
        """Detect data quality issues in a column."""
        issues = []

        if profile.null_ratio > 0.2:
            issues.append(f"high_null_rate:{profile.null_ratio:.0%}")
        if profile.dtype == "string" and profile.max_length and profile.max_length == 0:
            issues.append("all_empty_strings")
        if profile.dtype == "string" and profile.distinct_ratio == 1.0 and profile.total_count > 100:
            issues.append("likely_unique_per_row_not_useful")

        return issues
