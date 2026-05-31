# Phase 7: Fuzzy Matching

## Phase Overview

Catch near-duplicates that survive exact deduplication — typos, phonetic variations, and formatting differences in names, addresses, and other text fields. This phase introduces string distance algorithms to quantify how "close" two records are.

---

## Business Context

### Why This Phase Matters

Exact deduplication (Phase 6) catches `John Smith = John Smith`. But real-world data is messier:

| Variation | Cause |
|-----------|-------|
| `John Smith` vs `Jon Smith` | Typo, nickname |
| `John Smith` vs `J. Smith` | Abbreviation |
| `John Smith` vs `John Smyth` | Phonetic similarity, data entry error |
| `Acme Inc.` vs `Acme Incorporated` | Legal suffix variation |
| `123 Main St` vs `123 Main Street` | Address abbreviation |

These near-duplicates represent the same real-world entity but would never match on exact comparison. Fuzzy matching bridges this gap.

### Capability Unlocked

Detection of non-exact duplicates using string similarity metrics, producing a candidate match set for further scoring.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Exact-deduplicated Silver table from Phase 6 |
| **Records** | Clean, standardized, enriched — but still contains near-duplicates |
| **Volume** | Reduced by exact dedup |

---

## Processing Logic

### Algorithm 1: Levenshtein Distance

Measures the minimum number of single-character edits (insertions, deletions, substitutions) to transform one string into another.

```python
from pyspark.sql.functions import levenshtein

df_pairs = df_pairs.withColumn(
    "levenshtein_dist",
    levenshtein(col("name_a"), col("name_b"))
)

# Normalize to 0-1 similarity
df_pairs = df_pairs.withColumn(
    "levenshtein_sim",
    1 - (col("levenshtein_dist")
         / greatest(length(col("name_a")), length(col("name_b"))))
)
```

**Best for**: Short strings (names, product codes). **Weakness**: Sensitive to length differences — `John Smith` vs `John Smith Jr.` has a misleadingly large distance.

### Algorithm 2: Jaro-Winkler Similarity

Measures string similarity with a bias toward strings that match from the beginning. Excellent for names.

```python
# textdistance library provides Jaro-Winkler
import textdistance

@udf(FloatType())
def jaro_winkler_sim(s1, s2):
    if s1 is None or s2 is None:
        return 0.0
    return textdistance.jaro_winkler(s1, s2)

df_pairs = df_pairs.withColumn(
    "jaro_winkler_sim",
    jaro_winkler_sim(col("name_a"), col("name_b"))
)
```

**Best for**: Personal names. **Strength**: `John Smith` and `Jon Smith` get ~0.92. The common prefix `"John"`/`"Jon"` is rewarded.

### Algorithm 3: Soundex / Metaphone (Phonetic)

Encodes words by how they sound, not how they're spelled. `Smith` and `Smyth` produce the same Soundex code.

```python
# fuzzywuzzy or jellyfish library
import jellyfish

@udf(StringType())
def soundex_code(name):
    if name is None:
        return None
    return jellyfish.soundex(name)

df = df.withColumn("soundex_name", soundex_code(col("name")))

# Match on identical Soundex codes
df_pairs = df.alias("a").join(
    df.alias("b"),
    (col("a.soundex_name") == col("b.soundex_name"))
    & (col("a.id") < col("b.id")),  # Avoid self-joins and duplicates
    how="inner"
)
```

**Best for**: Names with phonetic variations. **Weakness**: Language-specific. English Soundex is poor for non-English names.

### Algorithm 4: Token-Based Address Similarity

Addresses benefit from token-based comparison rather than character-based:

```python
from pyspark.sql.functions import split, array_intersect, size, array_union

def address_jaccard(df, col_a, col_b):
    """Jaccard similarity on address tokens."""
    df = df \
        .withColumn("_tokens_a", split(col(col_a), " ")) \
        .withColumn("_tokens_b", split(col(col_b), " ")) \
        .withColumn("_intersection",
            size(array_intersect(col("_tokens_a"), col("_tokens_b")))) \
        .withColumn("_union",
            size(array_union(col("_tokens_a"), col("_tokens_b")))) \
        .withColumn("address_jaccard",
            col("_intersection") / col("_union"))
    return df
```

**Best for**: Addresses, company names, multi-word strings.

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Match Pairs** | Candidate pairs with similarity scores per algorithm |
| **Scores** | `levenshtein_sim`, `jaro_winkler_sim`, `soundex_match`, `address_jaccard` |
| **Thresholds** | High (≥ 95%: auto-merge), Medium (80–95%: review), Low (< 80%: keep separate) |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark `levenshtein()`** | Built-in Levenshtein | No UDF overhead |
| **jellyfish** | Phonetic algorithms | Soundex, Metaphone, NYSIIS |
| **textdistance** | String metrics | Jaro-Winkler, Damerau-Levenshtein, many others |
| **rapidfuzz** (alternative) | Fast fuzzy matching | C++ backend, 10–100x faster than Python libs |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-07-fuzzy-matching.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_07_fuzzy_matching.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/07-fuzzy-matching.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Fuzzy match recall | ≥ 95% | True duplicates found / known duplicates |
| Fuzzy match precision | ≥ 90% | True duplicates / total flagged pairs |
| Algorithm coverage | All non-null text fields covered | Fields with at least one similarity score |

---

## When to Advance

Move to Phase 8 when:

- [ ] Fuzzy matching detects near-duplicates that exact matching missed.
- [ ] Similarity thresholds are calibrated with representative data.
- [ ] The team understands each algorithm's strengths and weaknesses.
- [ ] Match results include per-algorithm scores (not just a binary match/no-match).

---

## Common Pitfalls

### 1. Brute-Force All-Pairs Comparison

**Problem**: Fuzzy matching 100K records means ~5 billion pairwise comparisons. With UDFs, this takes days.

**Fix**: Phase 8 (Blocking) must be implemented before fuzzy matching at scale. Blocking reduces the comparison space from O(n^2) to O(n * b) where b is the block size.

### 2. Python UDF Performance

**Problem**: Jaro-Winkler via a Python UDF is 10–100x slower than a native Spark function. A PySpark UDF on 1 million rows takes minutes, not seconds.

**Fix**: Use Pandas UDFs (`@pandas_udf`) for vectorized operations. Consider the `rapidfuzz` library (C++ backend). For very large datasets, implement critical algorithms as Spark built-in functions or Scala UDFs.

### 3. Thresholds Set Without Testing

**Problem**: A threshold of 0.8 for Jaro-Winkler is chosen because "it sounds right." At 0.8, you either miss half the true duplicates or generate thousands of false positives.

**Fix**: Calibrate thresholds using labeled data. Create a golden dataset of 500–1000 known duplicate pairs and known non-duplicate pairs. Measure precision and recall at each threshold. Choose the threshold that balances business needs (usually: minimize false positives for auto-merge, maximize recall for review queue).

### 4. Ignoring Field Semantics

**Problem**: Applying Levenshtein equally to names, addresses, and phone numbers. A Levenshtein distance of 2 on a phone number is catastrophic; on a company name, it's trivial.

**Fix**: Use different algorithms per field type. Names → Jaro-Winkler. Addresses → token Jaccard. Phones → exact after normalization. Dates → numeric difference.

---

## Further Reading

- [String Similarity Metrics Overview](https://www.joyofdata.de/blog/comparison-of-string-distance-algorithms/)
- [Jaro-Winkler: Theory and Practice](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
- [Soundex and Metaphone for Record Linkage](https://www.census.gov/srd/papers/pdf/rrs2006-01.pdf)
- [rapidfuzz: Fast Fuzzy Matching](https://github.com/rapidfuzz/RapidFuzz)

---

[:material-arrow-left: Previous: Phase 6](phase-06-exact-deduplication.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 8](phase-08-record-blocking.md)
