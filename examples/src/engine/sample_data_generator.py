"""
Sample Data Generator

Generates realistic, diverse synthetic datasets for testing the entity
resolution pipeline end-to-end. Creates data with known duplicates,
near-duplicates, quality issues, and edge cases.

Supports multiple entity types: customer, product, company, and generic.
Data includes intentional duplicates, fuzzy matches, nulls, and format
variations to thoroughly exercise all 15 phases.

Usage:
    from engine.sample_data_generator import generate_test_dataset
    df = generate_test_dataset(spark, entity_type="customer", num_rows=1000)
"""

from __future__ import annotations

import random
import string
from typing import Optional
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, TimestampType, IntegerType,
)

from utils.logging_config import get_logger

logger = get_logger(__name__)

# Seed for reproducibility
SEED = 42
random.seed(SEED)

# ---------------------------------------------------------------------------
# Name/identity pools for realistic data
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
    "Daniel", "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Mark",
    "Margaret", "Donald", "Sandra", "Steven", "Ashley", "Andrew", "Kimberly",
    "Paul", "Emily", "Joshua", "Donna", "Kenneth", "Michelle",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]

COMPANY_PREFIXES = [
    "Global", "American", "United", "International", "National", "Pacific",
    "Atlantic", "Continental", "Metro", "Premier", "Advanced", "Superior",
    "Quality", "Reliable", "Dynamic", "Integrated", "Strategic", "Premier",
]

COMPANY_SUFFIXES = ["Inc", "Corp", "LLC", "Ltd", "Group", "Holdings", "Enterprises", "Partners"]
COMPANY_TYPES = ["Technologies", "Solutions", "Services", "Industries", "Systems", "Consulting"]

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Food", "Office Supplies", "Automotive",
    "Healthcare", "Sports", "Home & Garden", "Books", "Toys",
]
PRODUCT_ADJECTIVES = [
    "Premium", "Professional", "Deluxe", "Standard", "Eco-Friendly",
    "Compact", "Heavy-Duty", "Lightweight", "Ergonomic", "Portable",
]

CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
    ("Fort Worth", "TX"), ("Columbus", "OH"), ("Charlotte", "NC"),
]

STREETS = [
    "Main St", "Oak Ave", "Elm St", "Park Ave", "Washington Blvd",
    "Maple Dr", "Cedar Ln", "Pine St", "Broadway", "First Ave",
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com", "company.com"]

SIC_CODES = ["7372", "7373", "3571", "3674", "4812", "5045", "5099", "2834", "1311", "2911"]


def generate_test_dataset(
    spark: SparkSession,
    entity_type: str = "customer",
    num_rows: int = 1000,
    duplicate_ratio: float = 0.05,
    fuzzy_duplicate_ratio: float = 0.10,
    null_ratio: float = 0.03,
    include_edge_cases: bool = True,
) -> DataFrame:
    """
    Generate a realistic test dataset with known duplicates and quality issues.

    Args:
        spark: Spark session.
        entity_type: Type of entity to generate.
        num_rows: Target number of rows (clean records).
        duplicate_ratio: Fraction of records that are exact duplicates.
        fuzzy_duplicate_ratio: Fraction that are near-duplicates.
        null_ratio: Fraction of values that should be null.
        include_edge_cases: Add edge case records (empty strings, extreme values).

    Returns:
        DataFrame ready for pipeline testing.
    """
    generators = {
        "customer": _generate_customers,
        "product": _generate_products,
        "company": _generate_companies,
        "supplier": _generate_suppliers,
        "generic": _generate_generic,
    }

    generator = generators.get(entity_type, _generate_generic)
    records = generator(num_rows, duplicate_ratio, fuzzy_duplicate_ratio, null_ratio, include_edge_cases)

    # Infer schema from first record
    if records:
        df = spark.createDataFrame(records)
        logger.info(
            f"Generated {len(records)} {entity_type} records "
            f"({duplicate_ratio:.0%} dupes, {fuzzy_duplicate_ratio:.0%} fuzzy, "
            f"{null_ratio:.0%} nulls)"
        )
        return df
    else:
        return spark.createDataFrame([], "id STRING")


# ---------------------------------------------------------------------------
# Entity generators
# ---------------------------------------------------------------------------

def _generate_customers(
    num_rows: int,
    duplicate_ratio: float,
    fuzzy_duplicate_ratio: float,
    null_ratio: float,
    include_edge_cases: bool,
) -> list[dict]:
    """Generate realistic customer records."""
    records = []
    base_id = 100000

    for i in range(num_rows):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city, state = random.choice(CITIES)
        street_num = random.randint(1, 9999)
        street = random.choice(STREETS)

        record = {
            "id": f"CUST-{base_id + i:08d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}@{random.choice(EMAIL_DOMAINS)}",
            "phone": f"+1{random.randint(200, 999):03d}{random.randint(100, 999):03d}{random.randint(0, 9999):04d}",
            "address": f"{street_num} {street}",
            "city": city,
            "state": state,
            "postal_code": f"{random.randint(10000, 99999):05d}",
            "country": "US",
            "created_date": (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1800))).strftime("%Y-%m-%d"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

    # Add exact duplicates
    num_dupes = int(num_rows * duplicate_ratio)
    for _ in range(num_dupes):
        if records:
            orig = random.choice(records[:num_rows])
            dupe = {**orig}
            records.append(dupe)

    # Add fuzzy duplicates (same person, different formatting)
    num_fuzzy = int(num_rows * fuzzy_duplicate_ratio)
    for _ in range(num_fuzzy):
        if records:
            orig = random.choice(records[:num_rows])
            fuzzy = {**orig}
            fuzzy["id"] = f"CUST-{base_id + len(records):08d}"

            # Vary name formatting
            if random.random() < 0.5:
                fuzzy["first_name"] = orig["first_name"][:1] + "."  # J. instead of James
            if random.random() < 0.3:
                fuzzy["last_name"] = orig["last_name"].upper()
            if random.random() < 0.3:
                fuzzy["email"] = fuzzy["email"].replace("@", "+alias@")
            if random.random() < 0.3:
                fuzzy["phone"] = fuzzy["phone"][:5] + "-" + fuzzy["phone"][5:8] + "-" + fuzzy["phone"][8:]
            if random.random() < 0.3:
                fuzzy["address"] = fuzzy["address"].replace("St", "Street").replace("Ave", "Avenue")

            records.append(fuzzy)

    # Add edge cases
    if include_edge_cases:
        edge_cases = [
            {**records[0], "id": "CUST-EDGE-001", "first_name": "", "email": "", "phone": ""},
            {**records[0], "id": "CUST-EDGE-002", "first_name": "A", "last_name": "B", "postal_code": "00000"},
            {**records[0], "id": "CUST-EDGE-003", "first_name": None, "last_name": None, "email": None},
            {**records[0], "id": "CUST-EDGE-004", "email": "not-an-email", "phone": "123"},
            {**records[0], "id": "CUST-EDGE-005", "first_name": "X" * 500, "created_date": "9999-99-99"},
        ]
        records.extend(edge_cases)

    return records


def _generate_products(
    num_rows: int,
    duplicate_ratio: float,
    fuzzy_duplicate_ratio: float,
    null_ratio: float,
    include_edge_cases: bool,
) -> list[dict]:
    """Generate realistic product records."""
    records = []
    for i in range(num_rows):
        adj = random.choice(PRODUCT_ADJECTIVES)
        cat = random.choice(PRODUCT_CATEGORIES)
        record = {
            "id": f"PROD-{10000 + i:06d}",
            "product_name": f"{adj} {cat} Item {i + 1}",
            "sku": f"SKU-{random.randint(10000, 99999)}-{random.choice(string.ascii_uppercase)}",
            "category": cat,
            "subcategory": f"{cat} Subcategory {random.randint(1, 5)}",
            "manufacturer": f"{random.choice(COMPANY_PREFIXES)} {random.choice(COMPANY_TYPES)}",
            "price": round(random.uniform(1.99, 9999.99), 2),
            "cost": round(random.uniform(0.99, 5000.00), 2),
            "created_date": (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

    # Duplicates and fuzzies
    for _ in range(int(num_rows * duplicate_ratio)):
        if records:
            records.append({**random.choice(records[:num_rows])})

    for _ in range(int(num_rows * fuzzy_duplicate_ratio)):
        if records:
            orig = random.choice(records[:num_rows])
            fuzzy = {**orig}
            fuzzy["id"] = f"PROD-{10000 + len(records):06d}"
            if random.random() < 0.5:
                fuzzy["product_name"] = orig["product_name"].replace("Premium", "Deluxe")
            if random.random() < 0.5:
                fuzzy["sku"] = fuzzy["sku"].lower()
            records.append(fuzzy)

    return records


def _generate_companies(
    num_rows: int,
    duplicate_ratio: float,
    fuzzy_duplicate_ratio: float,
    null_ratio: float,
    include_edge_cases: bool,
) -> list[dict]:
    """Generate realistic company records."""
    records = []
    for i in range(num_rows):
        prefix = random.choice(COMPANY_PREFIXES)
        suffix = random.choice(COMPANY_SUFFIXES)
        city, state = random.choice(CITIES)
        record = {
            "id": f"COMP-{20000 + i:06d}",
            "company_name": f"{prefix} {random.choice(COMPANY_TYPES)} {suffix}",
            "ticker": f"{''.join(random.choices(string.ascii_uppercase, k=random.randint(3, 5)))}",
            "duns_number": f"{random.randint(10000000, 99999999):09d}",
            "sic_code": random.choice(SIC_CODES),
            "naics_code": f"{random.randint(10, 99)}{random.randint(100, 999)}",
            "address": f"{random.randint(1, 9999)} {random.choice(STREETS)}",
            "city": city,
            "state": state,
            "postal_code": f"{random.randint(10000, 99999):05d}",
            "country": "US",
            "website": f"www.{prefix.lower().replace(' ', '')}{random.choice(COMPANY_TYPES).lower()}.com",
            "created_date": (datetime(2010, 1, 1) + timedelta(days=random.randint(0, 4000))).strftime("%Y-%m-%d"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

    for _ in range(int(num_rows * duplicate_ratio)):
        if records:
            records.append({**random.choice(records[:num_rows])})

    for _ in range(int(num_rows * fuzzy_duplicate_ratio)):
        if records:
            orig = random.choice(records[:num_rows])
            fuzzy = {**orig}
            fuzzy["id"] = f"COMP-{20000 + len(records):06d}"
            if random.random() < 0.5:
                fuzzy["company_name"] = orig["company_name"].replace("Inc", "Inc.")
            if random.random() < 0.3:
                fuzzy["ticker"] = orig["ticker"].lower()
            records.append(fuzzy)

    return records


def _generate_suppliers(
    num_rows: int,
    duplicate_ratio: float,
    fuzzy_duplicate_ratio: float,
    null_ratio: float,
    include_edge_cases: bool,
) -> list[dict]:
    """Generate realistic supplier records (similar to company but with contact info)."""
    records = _generate_companies(num_rows, duplicate_ratio, fuzzy_duplicate_ratio, null_ratio, include_edge_cases)
    for rec in records[:num_rows]:
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        rec["contact_name"] = f"{first} {last}"
        rec["contact_email"] = f"{first.lower()}.{last.lower()}@{random.choice(EMAIL_DOMAINS)}"
        rec["contact_phone"] = f"+1{random.randint(200, 999):03d}{random.randint(100, 999):03d}{random.randint(0, 9999):04d}"
    return records


def _generate_generic(
    num_rows: int,
    duplicate_ratio: float,
    fuzzy_duplicate_ratio: float,
    null_ratio: float,
    include_edge_cases: bool,
) -> list[dict]:
    """Generate a generic dataset with no assumed entity type (tests auto-detection)."""
    records = []
    for i in range(num_rows):
        record = {
            "id": f"ENT-{10000 + i:06d}",
            "name": f"Entity {i + 1}",
            "description": f"Description for entity {i + 1}",
            "category": random.choice(["TypeA", "TypeB", "TypeC"]),
            "value": round(random.uniform(1.0, 10000.0), 2),
            "created_date": (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime("%Y-%m-%d"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)
    return records
