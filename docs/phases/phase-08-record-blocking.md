# Phase 8: Record Blocking

## Phase Overview

Reduce the computational cost of entity matching from O(n^2) to near-linear by grouping records into blocks and only comparing records within each block. Without blocking, 1 million records require ~500 billion pairwise comparisons — computationally infeasible. With blocking, the same dataset might require only 50 million comparisons — a 10,000x reduction.

---

## Business Context

### Why This Phase Matters

Fuzzy matching (Phase 7) is inherently O(n^2) if applied naively: every record must be compared to every other record. For 1 million records:

```
1,000,000^2 / 2 = 500,000,000,000 comparisons
```

At 1 microsecond per comparison, that's ~139 hours of compute. And 1 million records is a small dataset by enterprise standards.

Blocking is the **single most important scalability technique** in entity resolution. It is not optional for production systems.

### Capability Unlocked

Scalable matching for datasets of any size. The pipeline can handle millions, tens of millions, or hundreds of millions of records without exponential cost growth.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Exact-deduplicated Silver table from Phase 6 |
| **Records** | Clean, standardized, enriched |
| **Volume** | Potentially millions of records |

---

## Processing Logic

### Strategy 1: Blocking by Attribute (Simplest)

Group records by a shared attribute value and only compare within groups.

```python
# Block by postal code — records in different postal codes cannot be the same entity
df_blocked = df.repartition("postal_code")

# Within each postal code block, generate candidate pairs
from pyspark.sql.window import Window
from pyspark.sql.functions import collect_list

window_spec = Window.partitionBy("postal_code")
df_pairs = df \
    .withColumn("_records_in_block", collect_list("customer_id").over(window_spec))
```

**Effectiveness**: Reduces comparisons from N^2 to sum(b_i^2) for each block b_i. If records are evenly distributed across 1000 postal codes, comparisons drop from 500 billion to ~500 million (1000x reduction).

### Strategy 2: Sorted Neighborhood Method

Sort records by a blocking key, then slide a window of fixed size over the sorted list. Only records within the same window are compared.

```python
from pyspark.sql.functions import monotonically_increasing_id, abs as spark_abs

# Sort by blocking key
df_sorted = df.orderBy("blocking_key")

# Assign sequential IDs
df_sorted = df_sorted.withColumn("_seq_id", monotonically_increasing_id())

# Define window size W
W = 100  # Compare each record with its 100 neighbors

# Self-join on |id_a - id_b| <= W
df_pairs = df_sorted.alias("a").join(
    df_sorted.alias("b"),
    (spark_abs(col("a._seq_id") - col("b._seq_id")) <= W)
    & (col("a._seq_id") < col("b._seq_id")),
    how="inner"
)
```

**Effectiveness**: Guaranteed linear scaling — O(n * W) where W is the window size. Typically W=50–200. Records that are far apart in the sort order are never compared.

### Strategy 3: Canopy Clustering

Use a cheap distance metric to create overlapping clusters (canopies). Records in the same canopy are compared with expensive metrics.

```python
# Step 1: Create canopies using a cheap metric (e.g., token overlap)
# Step 2: Within each canopy, apply expensive metrics (Jaro-Winkler, etc.)

def create_canopies(df, cheap_threshold=0.3):
    """
    Canopy clustering:
    1. Pick a random record as the canopy center.
    2. All records within cheap_threshold of the center join the canopy.
    3. Remove canopy members from pool.
    4. Repeat until pool is empty.
    """
    # Tokens for cheap comparison
    df = df.withColumn("_tokens", split(lower(col("name")), " "))

    # In practice, use an iterative algorithm or the canopy-clustering library
    canopies = {}  # canopy_id -> list of record_ids
    remaining = set(df.select("customer_id").collect())

    while remaining:
        center = remaining.pop()
        # Find all records with token overlap above threshold
        canopy_members = find_token_overlap(df, center, cheap_threshold)
        canopies[center] = canopy_members
        remaining -= set(canopy_members)

    return canopies
```

**Effectiveness**: O(n * c) where c is the number of canopies. Overlapping canopies handle the boundary problem — records near a canopy edge might belong in multiple canopies.

### Strategy 4: Multi-Pass Blocking

Run multiple blocking passes with different blocking keys. A true match might be missed by one key but caught by another.

```python
# Pass 1: Block by postal code
pairs_pass1 = generate_pairs(df, block_key="postal_code")

# Pass 2: Block by first 3 characters of company name
pairs_pass2 = generate_pairs(df, block_key="name_prefix_3")

# Pass 3: Block by Soundex code of last name
pairs_pass3 = generate_pairs(df, block_key="soundex_last_name")

# Union all pairs (deduplicate identical pairs)
all_pairs = pairs_pass1 \
    .union(pairs_pass2) \
    .union(pairs_pass3) \
    .dropDuplicates(["record_id_a", "record_id_b"])
```

**Effectiveness**: Multi-pass blocking significantly improves recall. If a single blocking key has 95% recall, three independent keys can approach 99.9% (1 — 0.05^3 = 0.999875).

---

## Blocking Key Design Guidelines

| Principle | Good | Bad |
|-----------|------|-----|
| **High coverage** | Postal code (most records have one) | Middle name (many records don't) |
| **Sufficient granularity** | 100–1000 records per block | 1 record per block (no comparisons) or 100K per block (still O(n^2) within block) |
| **Stability** | Date of birth | Last login date (changes frequently) |
| **Error tolerance** | Soundex(name) | Exact name (typos cause records to fall into different blocks) |

---

## Transitive Closure

After matching within blocks, apply transitive closure across blocks:

```
If A matches B (in block 1), and B matches C (in block 2),
then A, B, C are all the same entity (even though A and C were never compared).
```

```python
# Represent matches as a graph, find connected components
from graphframes import GraphFrame

vertices = df.select("customer_id").distinct()
edges = df_pairs.select(
    col("record_id_a").alias("src"),
    col("record_id_b").alias("dst")
)

graph = GraphFrame(vertices, edges)
components = graph.connectedComponents()
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Blocked Candidate Pairs** | Pairs of records to compare with fuzzy/ML matching |
| **Comparison Reduction** | From O(n^2) to O(n * b) where b is average block size |
| **Transitive Closure** | Connected components representing entity clusters |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark Window Functions** | Sorted neighborhood | Efficient sliding window |
| **GraphFrames** | Transitive closure | Connected components on Spark |
| **Canopy Clustering** | Fast pre-clustering | Overlapping clusters using cheap metrics |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-08-record-blocking.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_08_blocking.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/08-record-blocking.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Pairs reduction ratio (PRR) | ≥ 0.99 (99% reduction) | Candidate pairs / total possible pairs |
| Blocking recall | ≥ 99% | True duplicates in at least one shared block / all true duplicates |
| Blocking precision | Depends on block key quality | True duplicates in candidate pairs / total candidate pairs |
| Transitive closure coverage | 100% of candidate pairs processed | Connected components generated |

---

## When to Advance

Move to Phase 9 when:

- [ ] Blocking reduces pairwise comparisons by at least 99% (PRR ≥ 0.99).
- [ ] Multi-pass blocking is implemented to ensure high recall.
- [ ] Transitive closure merges matches across blocks.
- [ ] Blocking keys are documented and justified.
- [ ] The pipeline can handle the organization's largest entity tables within acceptable time and cost.

---

## Common Pitfalls

### 1. The O(n^2) Trap

**Problem**: "We only have 50,000 records. We can just compare all pairs." Six months later, the dataset is 500,000 records and the nightly pipeline no longer finishes.

**Fix**: Implement blocking from the start, even for small datasets. The code is the same; only the need changes as data grows.

### 2. Blocking Key Data Quality

**Problem**: Blocking by `postal_code` when 40% of records have null postal codes. Those 40% fall into a single "null" block with O((0.4N)^2) comparisons.

**Fix**: Handle nulls explicitly. Route null-key records to a separate pipeline that uses a different blocking key. Or use a multi-pass approach where Pass 1 blocks by postal code (non-null only) and Pass 2 blocks by city for null-postal-code records.

### 3. Overly Aggressive Blocking

**Problem**: Blocking by `first_letter_of_name + postal_code` creates 26 × 10,000 = 260,000 blocks averaging 4 records each. True matches across different first letters are missed.

**Fix**: Measure blocking recall, not just speed. The goal is to reduce comparisons while keeping recall > 99%. If recall drops below 99%, broaden the blocking keys or add more passes.

### 4. Ignoring Transitive Closure

**Problem**: After blocking by postal code, A matches B in block "2000" and B matches C in block "2001" (B has two addresses — home and work). But A and C are never compared because they're in different blocks.

**Fix**: Always run transitive closure after matching. This turns a set of pairwise matches into entity clusters. It's typically O(V + E) using union-find or connected components — negligible compared to the matching cost.

---

## Further Reading

- [Scaling Entity Resolution: A Survey of Blocking Techniques](https://dbs.uni-leipzig.de/file/Taxonomy_of_Blocking.pdf)
- [The Sorted Neighborhood Method](https://dl.acm.org/doi/10.5555/645918.673778)
- [Canopy Clustering for Record Linkage](https://www.cs.cmu.edu/~ccatal/papers/canopy-kdd00.pdf)
- [GraphFrames: Connected Components](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#connected-components)

---

[:material-arrow-left: Previous: Phase 7](phase-07-fuzzy-matching.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 9](phase-09-feature-engineering.md)
