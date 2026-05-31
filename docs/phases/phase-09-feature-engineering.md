# Phase 9: Feature Engineering for Matching

## Phase Overview

Transform each candidate record pair into a feature vector that a machine learning model can consume. Each feature captures one dimension of similarity between the two records — name similarity, address overlap, date proximity, geographic distance, categorical agreement.

---

## Business Context

### Why This Phase Matters

Fuzzy matching (Phase 7) produces individual similarity scores. But no single algorithm tells the whole story:

- Two records might have 95% name similarity but different addresses → probably different people.
- Two records might have 60% name similarity (nickname) but identical phone and address → probably the same person.

Feature engineering combines multiple signals into a unified representation. This is the bridge from heuristic matching to machine learning.

### Capability Unlocked

Structured, ML-ready feature vectors that capture all dimensions of entity similarity.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Candidate record pairs from Phase 8 (blocking) |
| **Pairs** | (record_a, record_b) tuples with all enriched, standardized fields |
| **Labels** (if supervised) | Known match / no-match labels for training |

---

## Processing Logic

### Feature Categories

#### 1. Numeric Similarity Features

```python
from pyspark.sql.functions import levenshtein, greatest, length

def numeric_similarity_features(df, col_a, col_b, prefix):
    """Generate similarity features for a text column pair."""
    return df \
        .withColumn(f"{prefix}_lev_dist",
            levenshtein(col(col_a), col(col_b))) \
        .withColumn(f"{prefix}_lev_sim",
            1 - col(f"{prefix}_lev_dist")
            / greatest(length(col(col_a)), length(col(col_b)))) \
        .withColumn(f"{prefix}_len_diff",
            abs(length(col(col_a)) - length(col(col_b))))
```

#### 2. Categorical Agreement Features

```python
def categorical_features(df, col_a, col_b, prefix):
    """Binary: 1 if values match exactly, 0 otherwise."""
    return df \
        .withColumn(f"{prefix}_match",
            when(
                col(col_a).isNull() | col(col_b).isNull(), 0
            ).when(
                col(col_a) == col(col_b), 1
            ).otherwise(0))
```

#### 3. Date Proximity Features

```python
from pyspark.sql.functions import datediff, abs as spark_abs

def date_proximity_features(df, col_a, col_b, prefix):
    """Days between two dates, with null handling."""
    return df \
        .withColumn(f"{prefix}_days_diff",
            spark_abs(datediff(col(col_a), col(col_b)))) \
        .withColumn(f"{prefix}_same_year",
            when(
                year(col(col_a)) == year(col(col_b)), 1
            ).otherwise(0))
```

#### 4. Geographic Distance Features

```python
from pyspark.sql.functions import acos, sin, cos, radians, lit
import math

def haversine_distance(lat_a, lon_a, lat_b, lon_b):
    """Calculate distance in kilometers between two lat/lon points."""
    return acos(
        sin(radians(lat_a)) * sin(radians(lat_b))
        + cos(radians(lat_a)) * cos(radians(lat_b))
        * cos(radians(lon_a) - radians(lon_b))
    ) * lit(6371.0)  # Earth radius in km
```

#### 5. Phonetic Features

```python
import jellyfish

@udf(IntegerType())
def soundex_match(name_a, name_b):
    if name_a is None or name_b is None:
        return 0
    return 1 if jellyfish.soundex(name_a) == jellyfish.soundex(name_b) else 0
```

### Feature Vector Assembly

```python
from pyspark.ml.feature import VectorAssembler

# Assemble all features into a single vector column
feature_columns = [
    "name_lev_sim", "name_jw_sim",
    "email_lev_sim",
    "phone_exact_match",
    "address_jaccard",
    "city_match", "province_match", "country_match",
    "industry_match",
    "signup_days_diff", "signup_same_year",
    "geo_distance_km",
    "soundex_name_match", "soundex_company_match",
]

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)

df_features = assembler.transform(df_pairs)
```

### Feature Scaling

```python
from pyspark.ml.feature import StandardScaler

# Scale features to zero mean and unit variance (important for some models)
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withMean=True,
    withStd=True
)

scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Feature Vectors** | Numeric arrays: `[0.92, 0.98, 0.60, 1.00, 0, 3, 1, 1, ...]` |
| **Feature Store** | Versioned, queryable feature sets for training and inference |
| **Metadata** | Feature names, types, coverage (non-null rate), importance scores |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark ML VectorAssembler** | Feature assembly | Native Spark ML pipeline integration |
| **PySpark ML StandardScaler** | Feature scaling | Required for distance-based models |
| **Feature Store (Feast / Tecton)** | Feature management | Version, share, and serve features |
| **MLflow** | Experiment tracking | Track which feature sets produced which results |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-09-feature-engineering.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_09_feature_engineering.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/09-feature-engineering.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Feature coverage | ≥ 95% | Features with non-null values across all pairs |
| Feature correlation | No pair > 0.95 | Pearson correlation between feature pairs |
| Feature importance variance | ≥ 2 features with > 5% importance | After training, check feature importance distribution |

---

## When to Advance

Move to Phase 10 when:

- [ ] Feature vectors are assembled for all candidate pairs.
- [ ] Feature scaling is applied (if needed by downstream models).
- [ ] Features are versioned and logged with metadata (names, types, coverage).
- [ ] Labeled training data exists (if doing supervised learning) or an unsupervised approach is planned.

---

## Common Pitfalls

### 1. Leaking the Label

**Problem**: Including a feature that is a proxy for the match label. For example, `is_exact_match` = 1 if the records are exact duplicates. The model learns to predict `is_exact_match` instead of learning similarity.

**Fix**: Audit features for label leakage. Any feature that is deterministically correlated with the target should be removed or used only as a pre-filter (exact matches already handled in Phase 6).

### 2. Missing Value Handling

**Problem**: Features compute `null` when one side of the pair has a missing value. The `VectorAssembler` fails or the model interprets `null` as a valid value.

**Fix**: Handle nulls explicitly in every feature function. Common strategies: impute with 0 (for similarity, missing = "not similar"), impute with mean, or add a binary `_is_null` indicator feature.

### 3. Too Many Features, Not Enough Labels

**Problem**: 50 features on 200 labeled pairs. The model overfits immediately.

**Fix**: If labeled data is scarce (the usual case), limit to 10–15 carefully chosen features. Use L1 regularization (Lasso) to drive irrelevant feature coefficients to zero. Active learning (Phase 14) grows the labeled set over time.

### 4. Feature Drift

**Problem**: The feature distribution in production shifts over time (new regions, new naming conventions), but the model was trained on old data.

**Fix**: Monitor feature distributions in production. Set up drift detection alerts (e.g., Kolmogorov-Smirnov test between training and production distributions). Retrain when drift exceeds threshold.

---

## Further Reading

- [Feature Engineering for Machine Learning](https://www.oreilly.com/library/view/feature-engineering-for/9781491953235/)
- [PySpark ML Pipeline Guide](https://spark.apache.org/docs/latest/ml-pipeline.html)
- [Feast Feature Store](https://docs.feast.dev/)
- [Data Leakage in Machine Learning](https://www.kaggle.com/code/alexisbcook/data-leakage)

---

[:material-arrow-left: Previous: Phase 8](phase-08-record-blocking.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 10](phase-10-probabilistic-matching.md)
