# Phase 10: Probabilistic Matching with Machine Learning

## Phase Overview

Train supervised machine learning models to predict whether two records represent the same entity. Unlike threshold-based fuzzy matching, ML models learn the optimal combination of similarity signals from labeled training data, producing calibrated match probabilities.

---

## Business Context

### Why This Phase Matters

Threshold-based matching (Phase 7) requires a human to decide: "Is a Jaro-Winkler score of 0.85 a match or not?" This is fragile because:

- The optimal threshold varies by field, by data source, and over time.
- Individual thresholds ignore interactions between features.
- Different business units have different tolerance for false positives vs false negatives.

ML models learn these trade-offs from data. Given labeled examples of "same entity" and "different entity" pairs, they find the decision boundary that maximizes accuracy.

### Capability Unlocked

Calibrated, data-driven match probabilities with measurable precision, recall, and confidence intervals.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Training Data** | Labeled feature vectors (features + match/no-match label) |
| **Inference Data** | Unlabeled feature vectors for new candidate pairs |
| **Labels** | From manual annotation, active learning, or known dedup ground truth |

---

## Processing Logic

### Step 1: Train/Test Split

```python
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier

# Split labeled data
train_df, test_df = df_labeled.randomSplit([0.8, 0.2], seed=42)
```

### Step 2: Train Multiple Models

```python
# Model 1: Logistic Regression (baseline, interpretable)
lr = LogisticRegression(
    featuresCol="scaled_features",
    labelCol="label",
    predictionCol="lr_prediction",
    probabilityCol="lr_probability",
    maxIter=100,
    regParam=0.1,
    elasticNetParam=0.5  # L1 + L2 regularization
)
lr_model = lr.fit(train_df)

# Model 2: Random Forest (handles non-linear relationships)
rf = RandomForestClassifier(
    featuresCol="features",  # RF doesn't need scaling
    labelCol="label",
    predictionCol="rf_prediction",
    probabilityCol="rf_probability",
    numTrees=100,
    maxDepth=10,
    featureSubsetStrategy="sqrt"
)
rf_model = rf.fit(train_df)
```

**Note**: XGBoost requires a separate library (`xgboost4j-spark`). The XGBoost pattern follows the same API:

```python
from xgboost.spark import SparkXGBClassifier

xgb = SparkXGBClassifier(
    features_col="features",
    label_col="label",
    prediction_col="xgb_prediction",
    probability_col="xgb_probability",
    num_workers=4,
    max_depth=6,
    eta=0.1,
    objective="binary:logistic"
)
xgb_model = xgb.fit(train_df)
```

### Step 3: Evaluate and Compare

```python
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

def evaluate_model(model, test_df, model_name):
    predictions = model.transform(test_df)

    # ROC-AUC
    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction"
    )
    auc = auc_evaluator.evaluate(predictions)

    # Precision, Recall, F1
    precision = predictions.filter("prediction = 1.0 AND label = 1.0").count() \
        / predictions.filter("prediction = 1.0").count()

    recall = predictions.filter("prediction = 1.0 AND label = 1.0").count() \
        / predictions.filter("label = 1.0").count()

    f1 = 2 * (precision * recall) / (precision + recall)

    return {
        "model": model_name,
        "auc": auc,
        "precision": precision,
        "recall": recall,
        "f1": f1
    }

results = [
    evaluate_model(lr_model, test_df, "LogisticRegression"),
    evaluate_model(rf_model, test_df, "RandomForest"),
    evaluate_model(xgb_model, test_df, "XGBoost"),
]
```

### Step 4: Select and Register Best Model

```python
# Select best by F1 score
best_result = max(results, key=lambda r: r["f1"])

import mlflow
mlflow.set_experiment("entity-resolution-matching")

with mlflow.start_run():
    mlflow.log_params(best_result["params"])
    mlflow.log_metrics({
        "auc": best_result["auc"],
        "precision": best_result["precision"],
        "recall": best_result["recall"],
        "f1": best_result["f1"],
    })
    mlflow.spark.log_model(best_result["model"], "matching_model")
```

### Step 5: Inference with Decision Thresholds

```python
predictions = best_model.transform(df_candidates)

# Decision tiers based on probability
df_results = predictions \
    .withColumn("decision",
        when(col("probability")[1] >= 0.95, lit("auto_merge"))
        .when(col("probability")[1] >= 0.80, lit("review"))
        .otherwise(lit("reject"))
    )
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Match Probabilities** | 0.0–1.0 per candidate pair |
| **Decision Tiers** | ≥ 95% auto-merge, 80–95% review, < 80% reject |
| **Model Artifacts** | Registered in MLflow (model + metrics + parameters) |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark MLlib** | Distributed ML training | Native Spark integration, scales to large datasets |
| **XGBoost (xgboost4j-spark)** | Gradient boosting | State-of-the-art tabular performance |
| **scikit-learn** (alternative) | Single-node training | Better for small labeled datasets (< 100K pairs) |
| **MLflow** | Model registry | Track experiments, version models, manage deployment |
| **imbalanced-learn** | Class balancing | SMOTE, random undersampling for imbalanced classes |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-10-probabilistic-ml.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_10_probabilistic_ml.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/10-probabilistic-matching.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| ROC-AUC | ≥ 0.95 | BinaryClassificationEvaluator |
| Precision (auto-merge) | ≥ 99% | True positives / predicted positives in ≥ 95% tier |
| Recall | ≥ 95% | True duplicates found / all true duplicates |
| F1 Score | ≥ 0.90 | Harmonic mean of precision and recall |

---

## When to Advance

Move to Phase 11 when:

- [ ] An ML model outperforms threshold-based fuzzy matching on held-out test data.
- [ ] The model is registered in MLflow with versioned artifacts.
- [ ] Decision tiers are calibrated and agreed upon with business stakeholders.
- [ ] The model handles class imbalance (duplicates are typically < 5% of all pairs).
- [ ] There is a process for periodic retraining as new labeled data arrives.

---

## Common Pitfalls

### 1. Severe Class Imbalance

**Problem**: In a dataset of 1 million records, there might be 5,000 true duplicate pairs and 499,995,000,000 non-duplicate pairs. Training on raw pairs gives 99.9999% accuracy by predicting "no match" for everything — and missing every duplicate.

**Fix**: Undersample non-duplicates. Use a 1:1 or 1:5 ratio of duplicates to non-duplicates for training. Use class weights (`weightCol` in PySpark) to penalize misclassifying the minority class more heavily. Use Stratified sampling for train/test split.

### 2. Overfitting on Training Sources

**Problem**: The model is trained on CRM data and achieves 98% F1. Applied to ERP data, it drops to 70%. The model learned CRM-specific patterns that don't generalize.

**Fix**: Train on labeled data from ALL source systems. Use cross-validation that holds out entire source systems (not just random records). Test on data from sources the model hasn't seen.

### 3. Uncalibrated Probabilities

**Problem**: The model outputs "95% probability of match" but only 70% of such predictions are actually matches. The probabilities are not calibrated.

**Fix**: Use probability calibration (Platt scaling or isotonic regression) to align predicted probabilities with empirical frequencies. In PySpark, this requires a custom `ProbabilityCalibrator` using `IsotonicRegression`.

### 4. Model Staleness

**Problem**: The model was trained in January. By June, new product categories and regional expansions have changed the data distribution. Match quality degrades silently.

**Fix**: Schedule periodic retraining. Monitor prediction drift — if the daily distribution of predicted probabilities shifts significantly, trigger an alert and investigate. Phase 14 (Stewardship) provides a steady stream of new labels.

---

## Further Reading

- [Fellegi-Sunter Model for Record Linkage](https://www.jstor.org/stable/2286061) (the foundational paper)
- [XGBoost for Entity Resolution](https://xgboost.readthedocs.io/)
- [Calibrating Classifier Probabilities](https://scikit-learn.org/stable/modules/calibration.html)
- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)

---

[:material-arrow-left: Previous: Phase 9](phase-09-feature-engineering.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 11](phase-11-semantic-matching-llm.md)
