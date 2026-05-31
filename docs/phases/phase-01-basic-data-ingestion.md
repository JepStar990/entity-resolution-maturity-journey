# Phase 1: Basic Data Ingestion

## Phase Overview

The foundation of every data pipeline. Data is ingested from source systems and preserved **exactly as received** in a raw (Bronze) layer. No cleaning, no validation, no transformation — just reliable, auditable ingestion with metadata enrichment.

---

## Business Context

### Why This Phase Matters

Before you can clean, match, or analyze data, you must first **get it into the platform**. Organizations typically have data scattered across:

- CRM systems (customer names, emails, phones)
- ERP systems (addresses, transactions, supplier data)
- Flat files (CSV exports, legacy system dumps)
- APIs (third-party enrichment services)
- Databases (operational systems, data warehouses)

Without a centralized ingestion layer, every downstream team reinvents source connectivity. This phase establishes a **single entry point** with consistent metadata, enabling:

- Full auditability (when did this data arrive? from where?)
- Reprocessing capability (replay from raw data)
- Source fidelity (the raw layer is the system of record for "what the source actually sent")

### Capability Unlocked

Trusted, traceable raw data that serves as the foundation for all downstream phases.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Sources** | CSV files, REST APIs, JDBC databases, event streams, file drops |
| **Schema** | Unknown or loosely defined |
| **Quality** | Unknown — this phase makes no quality assumptions |
| **Volume** | Varies (files from KB to GB, API responses, streaming events) |
| **Arrival Pattern** | Batch (scheduled files), on-demand (API calls), streaming (events) |

---

## Processing Logic

### Step 1: Connect to Source

```python
# Batch file ingestion
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/landing/customer_file_20260530.csv")

# Database ingestion
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://source-db:5432/crm") \
    .option("dbtable", "customers") \
    .load()

# API ingestion
response = requests.get("https://api.example.com/v1/customers")
df = spark.createDataFrame(response.json()["data"])
```

### Step 2: Preserve Source Data Exactly

**No transformations.** The raw data is written as-is. This preserves the "system of record" property — if downstream cleaning introduces an error, the raw layer is always available for reprocessing.

### Step 3: Add Metadata Columns

Every ingested record receives:

| Metadata Column | Type | Purpose |
|-----------------|------|---------|
| `load_timestamp` | Timestamp | When the record was ingested |
| `source_system` | String | Which system provided the data (CRM, ERP, API-name) |
| `source_file` | String | Original file name or API endpoint |
| `batch_id` | String | Unique identifier for this ingestion batch (UUID) |

### Step 4: Write to Bronze Table

```python
from pyspark.sql.functions import current_timestamp, lit, input_file_name
import uuid

batch_id = str(uuid.uuid4())

df_with_metadata = df \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("CRM")) \
    .withColumn("source_file", input_file_name()) \
    .withColumn("batch_id", lit(batch_id))

df_with_metadata.write \
    .format("delta") \
    .mode("append") \
    .save("/delta/bronze/customers")
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Storage** | Delta Lake Bronze table, append-only |
| **Schema** | Source columns + metadata columns |
| **Quality** | Unchanged from source — no cleaning applied |
| **Example** | `customer_name`, `email`, `load_date`, `source`, `batch_id` |

Example output:

| customer_name | email | load_date | source | batch_id |
|---------------|-------|-----------|--------|----------|
| John Smith | john@gmail.com | 2026-05-30 | CRM | b7f3a1... |
| Jane Doe | jane@yahoo.com | 2026-05-30 | CRM | b7f3a1... |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark** | Ingestion engine | Unified API for batch, streaming, JDBC, files |
| **Delta Lake** | Storage format | ACID transactions, schema enforcement, time travel |
| **Spark Structured Streaming** | Streaming ingestion | Exactly-once semantics for event sources |
| **Apache Airflow / Dagster** | Orchestration | Schedule and monitor ingestion jobs |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-01-ingestion.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_01_ingestion.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/01-data-ingestion.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Ingestion success rate | 99.9% | Successful batches / total batches |
| Ingestion latency | < 15 min (batch), < 30 sec (streaming) | Source timestamp to load_timestamp |
| Record count variance | < 5% vs expected | Compare to source record count |
| Metadata completeness | 100% | Records with non-null batch_id, source_system, load_timestamp |

---

## When to Advance

Move to Phase 2 when:

- [ ] Ingestion is reliable (≥ 99.9% success rate) for all planned source systems.
- [ ] All records carry complete metadata (batch_id, source_system, load_timestamp).
- [ ] The Bronze layer is append-only with no manual modifications.
- [ ] You can confidently answer: "Where did this row come from, and when?"

---

## Common Pitfalls

### 1. Cleaning During Ingestion

**Problem**: Engineers apply `trim()`, `lower()`, or null-filling during ingestion "because the source is messy."

**Fix**: Resist the urge. Cleaning happens in Phases 3–4. The raw layer must preserve source truth. If you clean during ingestion, you cannot distinguish source errors from ingestion errors.

### 2. Schema-on-Read Without Documentation

**Problem**: Using `inferSchema=true` without capturing the inferred schema. If a column changes type later, the pipeline breaks silently.

**Fix**: Log the inferred schema as metadata. Consider persisting the schema alongside the data. Phase 2 addresses this directly.

### 3. Siloed Ingestion Scripts

**Problem**: Each team writes their own CSV reader, JDBC connector, or API client. Inconsistent error handling, retry logic, and metadata.

**Fix**: Build a shared ingestion framework. A single `ingest(source_type, source_config)` function that handles all connectivity, metadata enrichment, and error handling.

### 4. No Batch Idempotency

**Problem**: Re-running a failed ingestion batch creates duplicate records with no way to identify them.

**Fix**: Use `batch_id` as an idempotency key. Before ingesting, check if the batch_id already exists in the Bronze table. Delta Lake's `merge` operation can handle upserts.

---

## Further Reading

- [Delta Lake: Reliability for Data Lakes](https://delta.io/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [The Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Data Ingestion Patterns](https://martinfowler.com/articles/data-ingestion-patterns.html)

---

[:material-arrow-right: Next: Phase 2: Schema Validation](phase-02-schema-validation.md)
