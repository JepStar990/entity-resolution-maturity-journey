"""
Phase 10: Probabilistic Matching

Trains a machine learning model to predict match probability for each
candidate record pair. Supports Logistic Regression (baseline) and
XGBoost (production). Integrates with MLflow for experiment tracking
and model registry.

Input: Feature vectors from Phase 9
Output: Match probabilities; registered ML model

Medallion Layer: Gold
"""

from __future__ import annotations

from typing import Optional

import numpy as np

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def prepare_training_data(
    features_df: DataFrame,
    label_col: str = "is_match",
    feature_cols: Optional[list[str]] = None,
    test_ratio: float = 0.2,
    random_seed: int = 42,
) -> tuple[DataFrame, DataFrame]:
    """
    Split feature vectors into training and test sets.

    Args:
        features_df: Feature vector DataFrame.
        label_col: Column with ground-truth labels (0 or 1).
        feature_cols: Specific feature columns to use (None = all float/double columns).
        test_ratio: Fraction of data for the test split.
        random_seed: Random seed for reproducible splits.

    Returns:
        Tuple of (train_df, test_df).
    """
    if feature_cols is None:
        # Auto-detect numeric feature columns
        metadata_cols = {"id_left", "id_right", "_aggregate_similarity", label_col}
        feature_cols = [
            c for c, dtype in features_df.dtypes
            if c not in metadata_cols and dtype in ("float", "double", "int", "bigint")
        ]

    logger.info(f"Using {len(feature_cols)} features: {feature_cols[:5]}...")

    # Filter to labeled data only
    labeled_df = features_df.filter(F.col(label_col).isNotNull())

    # Cast label to double
    labeled_df = labeled_df.withColumn(label_col, F.col(label_col).cast("double"))

    # Stratified split by label
    train_df, test_df = labeled_df.randomSplit(
        [1.0 - test_ratio, test_ratio],
        seed=random_seed,
    )

    logger.info(
        f"Training set: {train_df.count()} rows, "
        f"Test set: {test_df.count()} rows"
    )
    return train_df, test_df


def train_logistic_regression(
    spark: SparkSession,
    train_df: DataFrame,
    test_df: DataFrame,
    feature_cols: list[str],
    label_col: str = "is_match",
) -> dict:
    """
    Train a Logistic Regression model for probabilistic matching.

    Args:
        spark: Active Spark session.
        train_df: Training DataFrame.
        test_df: Test DataFrame.
        feature_cols: List of feature column names.
        label_col: Label column name.

    Returns:
        Dict with model, metrics, and feature importances.
    """
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml import Pipeline

    # Assemble feature vector
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )

    # Define model
    lr = LogisticRegression(
        featuresCol="features",
        labelCol=label_col,
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="raw_prediction",
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.5,
    )

    pipeline = Pipeline(stages=[assembler, lr])

    logger.info("Training Logistic Regression model...")
    model = pipeline.fit(train_df)

    # Evaluate on test set
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="raw_prediction",
        metricName="areaUnderROC",
    )
    auc_roc = evaluator.evaluate(predictions)

    # Compute additional metrics
    tp = predictions.filter((F.col(label_col) == 1) & (F.col("prediction") == 1)).count()
    fp = predictions.filter((F.col(label_col) == 0) & (F.col("prediction") == 1)).count()
    fn = predictions.filter((F.col(label_col) == 1) & (F.col("prediction") == 0)).count()
    tn = predictions.filter((F.col(label_col) == 0) & (F.col("prediction") == 0)).count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    metrics_dict = {
        "model_type": "logistic_regression",
        "auc_roc": round(auc_roc, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
    }

    # Extract coefficients as feature importance
    lr_model = model.stages[-1]
    coefficients = lr_model.coefficients.toArray().tolist()
    feature_importance = sorted(
        zip(feature_cols, coefficients),
        key=lambda x: abs(x[1]),
        reverse=True,
    )

    logger.info(f"LR Model: AUC={auc_roc:.4f}, F1={f1:.4f}")
    return {
        "model": model,
        "metrics": metrics_dict,
        "feature_importance": feature_importance[:10],
    }


def train_xgboost(
    spark: SparkSession,
    train_df: DataFrame,
    test_df: DataFrame,
    feature_cols: list[str],
    label_col: str = "is_match",
    params: Optional[dict] = None,
) -> dict:
    """
    Train an XGBoost model for entity matching.

    Falls back to Logistic Regression if xgboost is not installed.

    Args:
        spark: Active Spark session.
        train_df: Training DataFrame.
        test_df: Test DataFrame.
        feature_cols: List of feature column names.
        label_col: Label column name.
        params: XGBoost hyperparameters.

    Returns:
        Dict with model, metrics, and feature importances.
    """
    try:
        from xgboost.spark import SparkXGBClassifier
    except ImportError:
        logger.warning("xgboost not installed; falling back to Logistic Regression")
        return train_logistic_regression(
            spark, train_df, test_df, feature_cols, label_col
        )

    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    # Assemble feature vector
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )

    train_assembled = assembler.transform(train_df)
    test_assembled = assembler.transform(test_df)

    # XGBoost parameters
    xgb_params = {
        "featuresCol": "features",
        "labelCol": label_col,
        "predictionCol": "prediction",
        "maxDepth": 6,
        "eta": 0.1,
        "nEstimators": 100,
        "subsample": 0.8,
        "colsampleBytree": 0.8,
        "objective": "binary:logistic",
        "evalMetric": "auc",
        **(params or {}),
    }

    xgb = SparkXGBClassifier(**xgb_params)

    logger.info("Training XGBoost model...")
    model = xgb.fit(train_assembled)

    # Evaluate
    predictions = model.transform(test_assembled)

    evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc_roc = evaluator.evaluate(predictions)

    # Additional metrics
    tp = predictions.filter((F.col(label_col) == 1) & (F.col("prediction") == 1)).count()
    fp = predictions.filter((F.col(label_col) == 0) & (F.col("prediction") == 1)).count()
    fn = predictions.filter((F.col(label_col) == 1) & (F.col("prediction") == 0)).count()
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    metrics_dict = {
        "model_type": "xgboost",
        "auc_roc": round(auc_roc, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "true_positives": tp,
        "false_positives": fp,
        "params": xgb_params,
    }

    # Feature importance
    importance = model.nativeBooster.get_score(importance_type="gain")
    feature_importance = sorted(
        [(feature_cols[int(k.replace("f", ""))], v) for k, v in importance.items()],
        key=lambda x: x[1],
        reverse=True,
    )[:10]

    logger.info(f"XGBoost Model: AUC={auc_roc:.4f}, F1={f1:.4f}")
    return {
        "model": model,
        "metrics": metrics_dict,
        "feature_importance": feature_importance,
    }


def score_pairs(
    spark: SparkSession,
    model,
    features_df: DataFrame,
    feature_cols: list[str],
    model_type: str = "logistic_regression",
) -> DataFrame:
    """
    Apply a trained model to score unlabeled candidate pairs.

    Args:
        spark: Active Spark session.
        model: Trained Spark ML model or XGBoost model.
        features_df: Unlabeled feature vectors.
        feature_cols: Feature columns used by the model.
        model_type: 'logistic_regression' or 'xgboost'.

    Returns:
        DataFrame with match_probability column added.
    """
    from pyspark.ml.feature import VectorAssembler

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )

    assembled = assembler.transform(features_df)
    predictions = model.transform(assembled)

    # Extract match probability from probability vector (index 1 = positive class)
    scored = predictions.withColumn(
        "match_probability",
        F.col("probability").getItem(1).cast(FloatType()),
    )

    # Keep relevant columns
    keep_cols = [
        "id_left", "id_right", "match_probability",
    ]
    keep_cols = [c for c in keep_cols if c in scored.columns]

    result = scored.select(*keep_cols)
    return result


def run(
    spark: SparkSession,
    features_df: DataFrame,
    entity_type: str = "customer",
    config: Optional[dict] = None,
    gold_path: str = "Tables/gold/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> dict:
    """
    Execute Phase 10: train probabilistic matching model and score pairs.

    Args:
        spark: Active Spark session.
        features_df: Feature vector DataFrame from Phase 9.
        entity_type: Type of entity.
        config: Dict with:
            - model_type: 'logistic_regression' or 'xgboost'.
            - label_col: Ground-truth label column name.
            - test_ratio: Fraction for test split.
            - xgboost_params: Dict of XGBoost hyperparameters.
            - feature_cols: Specific features to use.
            - match_threshold: Probability threshold for declaring a match.
        gold_path: Gold layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        Dict with model, scored pairs, and evaluation metrics.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="10-probabilistic-matching")

    config = config or {}
    model_type = config.get("model_type", "logistic_regression")
    label_col = config.get("label_col", "is_match")
    test_ratio = config.get("test_ratio", 0.2)
    match_threshold = config.get("match_threshold", 0.5)

    logger.info(f"Training {model_type} model for {entity_type} matching")

    # Auto-detect feature columns
    metadata_cols = {"id_left", "id_right", "_aggregate_similarity", label_col}
    feature_cols = config.get("feature_cols", [
        c for c in features_df.columns
        if c not in metadata_cols and dict(features_df.dtypes).get(c) in ("float", "double", "int", "bigint")
    ])

    # Prepare data
    train_df, test_df = prepare_training_data(
        features_df, label_col, feature_cols, test_ratio
    )

    # Train model
    if model_type == "xgboost":
        xgb_params = config.get("xgboost_params")
        result = train_xgboost(
            spark, train_df, test_df, feature_cols, label_col, xgb_params
        )
    else:
        result = train_logistic_regression(
            spark, train_df, test_df, feature_cols, label_col
        )

    model = result["model"]
    train_metrics = result["metrics"]

    # Score all pairs
    scored_pairs = score_pairs(
        spark, model, features_df, feature_cols, model_type
    )

    # Apply match threshold
    scored_pairs = scored_pairs.withColumn(
        "_is_match",
        F.when(F.col("match_probability") >= match_threshold, F.lit(True)).otherwise(F.lit(False)),
    )

    # Write scored pairs to Gold
    target_path = resolve_table_path(
        layer="gold",
        table_name=f"{table_name}_match_scores",
        workspace=workspace,
    )
    write_to_delta(scored_pairs, target_path, mode="overwrite")

    # Log metrics
    for key, value in train_metrics.items():
        if isinstance(value, (int, float)):
            metrics.log_metric(f"model_{key}", float(value))
    metrics.log_count("scored_pairs", scored_pairs.count())

    # Log to MLflow if available
    try:
        import mlflow
        mlflow.set_experiment(f"/entity-resolution/{entity_type}")
        with mlflow.start_run(run_name=f"phase-10-{model_type}"):
            mlflow.log_params({"model_type": model_type, "threshold": match_threshold})
            mlflow.log_metrics({k: v for k, v in train_metrics.items() if isinstance(v, (int, float))})
            if model_type == "logistic_regression":
                mlflow.spark.log_model(model, f"{entity_type}_matching_model")
    except ImportError:
        pass

    metrics.flush()

    logger.info(
        f"Model training complete: AUC={train_metrics['auc_roc']:.4f}, "
        f"F1={train_metrics['f1_score']:.4f}"
    )
    return {
        "model": model,
        "scored_pairs": scored_pairs,
        "metrics": train_metrics,
    }
