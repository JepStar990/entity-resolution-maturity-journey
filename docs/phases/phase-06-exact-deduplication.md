# Phase 6: Exact Deduplication

## Phase Overview

Remove obvious duplicates using deterministic rules — exact matches on key columns. This is the simplest, fastest form of deduplication and should always be applied before more expensive fuzzy or ML-based matching.

---

## Business Context

### Why This Phase Matters

Not all duplicates are subtle. Many are exact copies caused by:

- The same file being loaded twice.
- A customer appearing in both the CRM and the ERP with identical fields.
- An ETL job that re-ran after a partial failure without idempotency checks.

Running fuzzy or ML matching on exact duplicates wastes compute and introduces false positives — two identical records might get a 98% similarity score instead of 100% because of a floating-point rounding difference.

Exact deduplication is **O(n)** with hashing, compared to **O(n^2)** for fuzzy matching. It should always run first.

### Capability Unlocked

A clean dataset with zero exact duplicates, ready for more sophisticated matching.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Enriched Silver table from Phase 5 |
| **Duplicates** | Exact copies from reloads, multi-system ingestion, ETL retries |
| **Volume** | Enriched records, typically 1–5% exact duplicates |

---

## Processing Logic

### Method 1: SELECT DISTINCT (Simple)

```python
# Remove fully identical rows
df_deduped = df.distinct()
```

### Method 2: ROW_NUMBER() Over Business Key (Recommended)

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Define the business key — columns that uniquely identify an entity
business_key = ["email", "phone"]

# Create a window partitioned by the business key
# Order by data quality score (prefer records from trusted sources)
window_spec = Window.partitionBy(business_key).orderBy(
    col("data_quality_score").desc(),
    col("load_timestamp").desc()  # Tie-breaker: prefer most recent
)

# Assign row numbers and keep only the first
df_deduped = df \
    .withColumn("_rn", row_number().over(window_spec)) \
    .filter(col("_rn") == 1) \
    .drop("_rn")
```

### Method 3: Composite Hash Key

```python
from pyspark.sql.functions import sha2, concat_ws

# Create a hash of the deduplication key for efficient comparison
df = df.withColumn(
    "_dedup_key",
    sha2(concat_ws("||", col("email"), col("phone")), 256)
)

# Deduplicate on hash (faster than multi-column comparison)
df_deduped = df.dropDuplicates(["_dedup_key"])
```

### Audit Trail

Always log which records were removed:

```python
# Capture removed duplicates for audit
df_duplicates = df \
    .withColumn("_rn", row_number().over(window_spec)) \
    .filter(col("_rn") > 1) \
    .select(
        col("_rn"),
        col("customer_id"),
        col("email"),
        col("batch_id"),
        current_timestamp().alias("dedup_timestamp")
    )

df_duplicates.write \
    .format("delta") \
    .mode("append") \
    .save("/delta/audit/duplicate_log")
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Storage** | Exact-Deduplicated Silver Delta table |
| **Survivor Rule** | Highest `data_quality_score`, then most recent `load_timestamp` |
| **Audit Log** | Duplicate pairs logged with survivor ID, removed ID, timestamp |
| **Volume Reduction** | Typically 1–5% fewer records |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark Window Functions** | Deduplication with ranking | `ROW_NUMBER() OVER (PARTITION BY ...)` is the standard pattern |
| **Delta Lake** | ACID writes | Atomic dedup → write with no partial results |
| **PySpark `sha2()`** | Hash-based dedup | Efficient comparison on composite keys |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-06-exact-dedup.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_06_exact_dedup.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/06-exact-deduplication.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Exact duplicates removed | ≥ 99% of true exact duplicates | Spot-check 100 random record pairs |
| False positive rate | 0% | No non-duplicates should be removed |
| Survivor quality | Survivor has highest data_quality_score | Audit check |
| Audit log completeness | 100% | Every removed record appears in audit log |

---

## When to Advance

Move to Phase 7 when:

- [ ] Exact deduplication runs reliably after enrichment.
- [ ] Business keys are defined and documented for each entity type.
- [ ] Survivor ranking logic is documented and agreed upon.
- [ ] An audit log captures all removed duplicates.
- [ ] Manual spot-checks confirm zero false positives (no non-duplicates removed).

---

## Common Pitfalls

### 1. The Wrong Business Key

**Problem**: Deduplicating on `email` alone merges `john.smith@gmail.com` (the father) with `john.smith@gmail.com` (the son who shares his name). The records are different people but share an email.

**Fix**: Compound business keys. `(email, phone, date_of_birth)` is safer than `email` alone. Define keys with business stakeholders who understand the domain. Document which column combinations uniquely identify an entity.

### 2. Non-Deterministic Survivor Selection

**Problem**: `ROW_NUMBER() OVER (PARTITION BY email ORDER BY load_timestamp)` breaks ties arbitrarily if two records have identical timestamps. Each run might keep a different record.

**Fix**: Ensure the ORDER BY clause produces a deterministic ordering. Include multiple tie-breaker columns. The last column should always be a unique identifier (e.g., `row_id`, `customer_id`).

### 3. Deduplicating Too Early

**Problem**: Deduplicating before standardization means `john.smith@gmail.com` and `John.Smith@Gmail.com` are treated as different keys. Exact dedup misses the duplicate.

**Fix**: This is why standardization (Phase 4) precedes exact dedup (Phase 6). Normalize email case, strip dots from Gmail addresses, and normalize domains before building dedup keys.

### 4. Silent Audit Failure

**Problem**: The audit log write fails, but the dedup write succeeds. Duplicates are removed but there's no record of which ones.

**Fix**: Wrap both writes in a transaction (Delta Lake supports multi-table commits with `DeltaTable.forPath()`). Or write the audit log first, then dedup — if the audit write fails, the whole batch fails.

---

## Further Reading

- [Window Functions in PySpark](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [Delta Lake ACID Transactions](https://delta.io/blog/2022-01-12-delta-lake-transactions/)
- [SQL Antipatterns: Avoiding the Wrong Key](https://www.amazon.com/SQL-Antipatterns-Programming-Pragmatic-Programmers/dp/1934356557)

---

[:material-arrow-left: Previous: Phase 5](phase-05-data-enrichment.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 7](phase-07-fuzzy-matching.md)
