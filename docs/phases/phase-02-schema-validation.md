# Phase 2: Schema Validation

## Phase Overview

Ensure ingested data conforms to expected structure before it progresses further. Valid records continue to the validated Bronze layer; invalid records are quarantined with failure reasons for investigation.

---

## Business Context

### Why This Phase Matters

Raw ingestion (Phase 1) makes no assumptions about data structure. But downstream processing — quality checks, standardization, matching — depends on predictable schemas.

Without schema validation:

- A column renamed at the source silently produces nulls downstream.
- A date field that changes format (ISO → US) breaks date parsing.
- A required field that goes missing causes cascading failures in matching.

Schema validation acts as a **contract enforcement layer** between source systems and the pipeline. It catches structural changes early, before they corrupt downstream data.

### Capability Unlocked

Confidence that data entering the processing pipeline has the expected structure, enabling safe transformations and matching.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Bronze table from Phase 1 |
| **Schema** | Source-defined — may drift over time |
| **Quality** | Unknown, but structurally intact |
| **Volume** | Same as ingested volume |

---

## Processing Logic

### Step 1: Define Expected Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

expected_schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("customer_name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("signup_date", DateType(), nullable=True),
])
```

### Step 2: Validate Schema

```python
def validate_schema(df, expected_schema):
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}

    missing_cols = set(expected_fields.keys()) - set(actual_fields.keys())
    extra_cols = set(actual_fields.keys()) - set(expected_fields.keys())
    type_mismatches = {
        col: (actual_fields[col], expected_fields[col])
        for col in expected_fields.keys() & actual_fields.keys()
        if actual_fields[col] != expected_fields[col]
    }

    return missing_cols, extra_cols, type_mismatches
```

### Step 3: Check Field-Level Formats

Beyond structural checks, validate field formats:

| Check | Example Failure |
|-------|----------------|
| Date format (ISO 8601) | `2026/99/45` |
| Email format (regex) | `not-an-email` |
| Phone format (pattern) | `abcdefg` |

```python
from pyspark.sql.functions import col, regexp_extract, to_date

email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

df_validated = df \
    .withColumn("_date_valid", to_date(col("signup_date"), "yyyy-MM-dd").isNotNull()) \
    .withColumn("_email_valid", col("email").rlike(email_pattern))
```

### Step 4: Split Pass / Fail

```python
passed = df_validated.filter(col("_date_valid") & col("_email_valid"))
failed = df_validated.filter(~col("_date_valid") | ~col("_email_valid")) \
    .withColumn("failure_reason",
        when(~col("_date_valid"), lit("Invalid date format"))
        .otherwise(lit("Invalid email format")))
```

### Step 5: Write to Separate Locations

- **Passed records** → Validated Bronze table (ready for Phase 3).
- **Failed records** → Quarantine table with `failure_reason` and `failed_at` timestamp.

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Validated Bronze** | Records that passed all schema and format checks |
| **Quarantine Table** | Records that failed, with failure reasons |
| **Metadata** | Schema version, validation timestamp, pass/fail counts |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark StructType** | Schema definition | Programmatic schema contracts |
| **Schema Registry** | Centralized schema store | Enforce schemas across teams |
| **Delta Lake Schema Enforcement** | Write-time validation | Reject writes that don't match the table schema |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-02-schema-validation.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_02_validation.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/02-schema-validation.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Schema pass rate | ≥ 95% | Passed records / total records |
| Schema drift events | 0 unplanned | Alert on any schema change |
| Quarantine backlog | < 24 hours | Time from failure to resolution |
| Missing required columns | 0 | Alert immediately |

---

## When to Advance

Move to Phase 3 when:

- [ ] Expected schemas are defined and versioned for all source systems.
- [ ] Schema validation runs automatically on every ingestion batch.
- [ ] Failed records are quarantined with clear failure reasons.
- [ ] The team has a process for handling schema drift (updating schemas, communicating with source teams).

---

## Common Pitfalls

### 1. Overly Strict Schemas

**Problem**: Rejecting records because an optional field is null, or a new column appears.

**Fix**: Distinguish required from optional fields. New columns should be allowed (additive schema changes) unless explicitly forbidden. Only reject on missing required columns or type mismatches.

### 2. Silent Schema Drift

**Problem**: A source changes a column from `INT` to `STRING`. The ingestion layer adapts silently (`inferSchema=true`), but downstream code that does `col + 1` now crashes.

**Fix**: Use Delta Lake's schema enforcement. Define schemas explicitly — never rely on inference in production pipelines.

### 3. Format Validation Without Feedback

**Problem**: Records are rejected for "invalid date" but the source team doesn't know.

**Fix**: The quarantine table should be queryable by source system owners. Set up alerts when quarantine volume exceeds a threshold. A dashboard showing daily pass/fail rates per source creates accountability.

### 4. Ignoring the Quarantine

**Problem**: Quarantined records accumulate indefinitely. After months, no one remembers why they were rejected.

**Fix**: Assign a TTL (time-to-live) to quarantine records — e.g., 30 days. After that, either fix and reprocess, or archive with a note. The quarantine is a temporary staging area, not a permanent graveyard.

---

## Further Reading

- [Delta Lake Schema Enforcement](https://delta.io/blog/2022-03-23-delta-lake-schema-enforcement/)
- [Schema Evolution in Data Lakes](https://www.databricks.com/blog/2020/02/04/schema-evolution-in-merge-operations.html)
- [Fabric Lakehouse: Schema Validation](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [Apache Avro Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)

---

[:material-arrow-left: Previous: Phase 1](phase-01-basic-data-ingestion.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 3](phase-03-data-quality-rules.md)
