"""
Metrics collection for the entity resolution pipeline.

Provides consistent metric emission across all 15 phases. In Fabric,
metrics can be logged to MLflow automatically. In local dev, they
are written to a metrics Delta table or printed to stdout.

Usage:
    from utils.metrics import MetricsCollector
    metrics = MetricsCollector(spark, phase="03-data-quality")
    metrics.log_count("valid_records", 98500)
    metrics.log_metric("pass_rate", 0.985)
    metrics.flush()
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession, Row


class MetricsCollector:
    """
    Collect and emit pipeline metrics for a specific phase.

    Metrics are collected in memory during a phase run and flushed
    to the metrics store (Delta table or MLflow) at phase completion.
    """

    def __init__(
        self,
        spark: SparkSession,
        phase: str = "unknown",
        run_id: Optional[str] = None,
        mlflow_experiment: Optional[str] = None,
    ):
        """
        Initialize a metrics collector.

        Args:
            spark: Active Spark session.
            phase: Phase identifier (e.g., "03-data-quality").
            run_id: Unique run identifier (auto-generated if None).
            mlflow_experiment: MLflow experiment path for tracking.
        """
        self.spark = spark
        self.phase = phase
        self.run_id = run_id or f"{phase}-{int(time.time() * 1000)}"
        self.mlflow_experiment = mlflow_experiment
        self._counts: dict[str, int] = {}
        self._metrics: dict[str, float] = {}
        self._start_time = time.time()
        self._metadata: dict[str, str] = {
            "phase": phase,
            "run_id": self.run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

    def log_count(self, name: str, value: int) -> None:
        """Log a count metric (record counts, pass/fail counts, etc.)."""
        self._counts[name] = value

    def log_metric(self, name: str, value: float) -> None:
        """Log a numeric metric (rates, scores, durations, etc.)."""
        self._metrics[name] = value

    def log_metadata(self, key: str, value: str) -> None:
        """Log metadata key-value pair."""
        self._metadata[key] = value

    def increment_count(self, name: str, delta: int = 1) -> None:
        """Increment a count metric by delta."""
        self._counts[name] = self._counts.get(name, 0) + delta

    def to_dataframe(self) -> DataFrame:
        """
        Convert collected metrics to a Spark DataFrame.

        Returns:
            DataFrame with columns: run_id, phase, metric_name,
            metric_type, metric_value, metadata, timestamp.
        """
        rows = []
        timestamp = datetime.now(timezone.utc).isoformat()
        elapsed = time.time() - self._start_time

        for name, value in self._counts.items():
            rows.append(Row(
                run_id=self.run_id,
                phase=self.phase,
                metric_name=name,
                metric_type="count",
                metric_value=float(value),
                metadata=str(self._metadata),
                recorded_at=timestamp,
            ))

        for name, value in self._metrics.items():
            rows.append(Row(
                run_id=self.run_id,
                phase=self.phase,
                metric_name=name,
                metric_type="metric",
                metric_value=value,
                metadata=str(self._metadata),
                recorded_at=timestamp,
            ))

        # Always log elapsed time
        rows.append(Row(
            run_id=self.run_id,
            phase=self.phase,
            metric_name="elapsed_seconds",
            metric_type="metric",
            metric_value=elapsed,
            metadata=str(self._metadata),
            recorded_at=timestamp,
        ))

        return self.spark.createDataFrame(rows)

    def flush(
        self,
        metrics_table_path: Optional[str] = None,
    ) -> None:
        """
        Persist metrics to storage.

        In Fabric with MLflow configured, logs to the experiment.
        Otherwise, appends to the metrics Delta table if a path is given,
        or prints a summary to stdout.

        Args:
            metrics_table_path: Delta table path for metrics storage.
        """
        elapsed = time.time() - self._start_time
        summary = (
            f"[{self.phase}] run={self.run_id} "
            f"elapsed={elapsed:.1f}s "
            f"counts={self._counts} "
            f"metrics={self._metrics}"
        )
        print(summary)

        if self.mlflow_experiment:
            try:
                import mlflow
                mlflow.set_experiment(self.mlflow_experiment)
                with mlflow.start_run(run_name=self.phase):
                    for name, value in self._counts.items():
                        mlflow.log_metric(f"{name}_count", value)
                    for name, value in self._metrics.items():
                        mlflow.log_metric(name, value)
                    mlflow.log_metric("elapsed_seconds", elapsed)
            except ImportError:
                pass

        if metrics_table_path:
            df = self.to_dataframe()
            df.write.format("delta").mode("append").save(metrics_table_path)


class PhaseTimer:
    """Context manager for timing a phase execution."""

    def __init__(self, metrics: MetricsCollector, phase_name: str):
        self.metrics = metrics
        self.phase_name = phase_name
        self._start = 0.0

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, *args):
        elapsed = time.time() - self._start
        self.metrics.log_metric(f"phase_{self.phase_name}_seconds", elapsed)
