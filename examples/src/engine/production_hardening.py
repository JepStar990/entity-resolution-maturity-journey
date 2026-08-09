"""
Production Hardening Module

Adds production-grade resilience to the entity resolution pipeline:
- Retry logic with exponential backoff
- Dead-letter queues for failed records
- Checkpoint/resume for long-running pipelines
- Alerting hooks for pipeline failures
- Idempotency guards
- Circuit breakers for external services

Pure Azure-native: uses Azure Event Hubs for alerting, Azure Storage
Queues for dead-letter, and Azure Monitor for observability.

Usage:
    from engine.production_hardening import (
        retry_with_backoff, DeadLetterQueue, PipelineCheckpoint,
        AlertManager, CircuitBreaker,
    )
"""

from __future__ import annotations

import time
import json
import functools
from typing import Optional, Any, Callable
from datetime import datetime, timezone
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.delta_helpers import write_to_delta, read_delta, resolve_table_path
from utils.logging_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Retry with Exponential Backoff
# ---------------------------------------------------------------------------

def retry_with_backoff(
    max_retries: int = 3,
    base_delay_seconds: float = 1.0,
    max_delay_seconds: float = 60.0,
    backoff_factor: float = 2.0,
    retryable_exceptions: tuple = (Exception,),
):
    """
    Decorator: retry a function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay_seconds: Initial delay between retries.
        max_delay_seconds: Maximum delay cap.
        backoff_factor: Multiplier for successive delays.
        retryable_exceptions: Exception types that trigger retry.

    Usage:
        @retry_with_backoff(max_retries=3)
        def call_external_api(payload):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(
                            base_delay_seconds * (backoff_factor ** attempt),
                            max_delay_seconds,
                        )
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for "
                            f"{func.__name__}: {e}. Waiting {delay:.1f}s"
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All {max_retries} retries exhausted for "
                            f"{func.__name__}: {e}"
                        )
            raise last_exception
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Dead-Letter Queue
# ---------------------------------------------------------------------------

@dataclass
class DeadLetterRecord:
    """A record sent to the dead-letter queue for investigation."""
    record_id: str
    phase: str
    error_message: str
    error_type: str
    record_data: dict
    timestamp: str
    retry_count: int = 0
    resolved: bool = False


class DeadLetterQueue:
    """
    Dead-letter queue for records that fail processing.

    Failed records are written to a Delta table for investigation and
    reprocessing. Supports Azure-native storage via OneLake.
    """

    def __init__(
        self,
        spark: SparkSession,
        workspace: Optional[str] = None,
        dlq_path: Optional[str] = None,
    ):
        self.spark = spark
        self.workspace = workspace
        self.dlq_path = dlq_path or resolve_table_path(
            layer="bronze",
            table_name="dead_letter_queue",
            workspace=workspace,
        )

    def send(
        self,
        record_id: str,
        phase: str,
        error: Exception,
        record_data: dict,
    ) -> None:
        """Send a failed record to the dead-letter queue."""
        dlq_record = DeadLetterRecord(
            record_id=record_id,
            phase=phase,
            error_message=str(error),
            error_type=type(error).__name__,
            record_data=record_data,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        df = self.spark.createDataFrame([{
            "record_id": dlq_record.record_id,
            "phase": dlq_record.phase,
            "error_message": dlq_record.error_message,
            "error_type": dlq_record.error_type,
            "record_data": json.dumps(dlq_record.record_data),
            "timestamp": dlq_record.timestamp,
            "retry_count": dlq_record.retry_count,
            "resolved": dlq_record.resolved,
        }])

        write_to_delta(df, self.dlq_path, mode="append")
        logger.info(f"Sent record {record_id} to DLQ (phase={phase})")

    def get_pending(self, phase: Optional[str] = None) -> DataFrame:
        """Retrieve unresolved dead-letter records."""
        dlq_df = read_delta(self.spark, self.dlq_path)
        result = dlq_df.filter(F.col("resolved") == False)  # noqa: E712
        if phase:
            result = result.filter(F.col("phase") == phase)
        return result.orderBy(F.col("timestamp").asc())

    def mark_resolved(self, record_id: str) -> None:
        """Mark a dead-letter record as resolved after reprocessing."""
        from delta.tables import DeltaTable
        dlq_table = DeltaTable.forPath(self.spark, self.dlq_path)
        dlq_table.update(
            condition=F.col("record_id") == record_id,
            set={"resolved": "true"},
        )
        logger.info(f"Marked DLQ record {record_id} as resolved")

    def reprocess_all(self, reprocess_fn: Callable[[DataFrame], DataFrame]) -> int:
        """Reprocess all pending DLQ records through a handler function."""
        pending = self.get_pending()
        count = pending.count()
        if count == 0:
            return 0

        try:
            reprocess_fn(pending)
            # Mark all as resolved
            from delta.tables import DeltaTable
            dlq_table = DeltaTable.forPath(self.spark, self.dlq_path)
            dlq_table.update(
                condition=F.col("resolved") == False,  # noqa: E712
                set={"resolved": "true"},
            )
            logger.info(f"Reprocessed {count} DLQ records")
        except Exception as e:
            logger.error(f"DLQ reprocessing failed: {e}")
            raise

        return count


# ---------------------------------------------------------------------------
# Pipeline Checkpoint/Resume
# ---------------------------------------------------------------------------

class PipelineCheckpoint:
    """
    Checkpoint manager for long-running pipelines.

    Saves pipeline state after each phase so that the pipeline can
    resume from the last successful phase after failure or interruption.
    """

    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
        workspace: Optional[str] = None,
    ):
        self.spark = spark
        self.run_id = run_id
        self.checkpoint_path = resolve_table_path(
            layer="metrics",
            table_name=f"checkpoint_{run_id}",
            workspace=workspace,
        )

    def save(self, phase: int, state: dict) -> None:
        """Save checkpoint after a phase completes."""
        record = {
            "run_id": self.run_id,
            "phase": phase,
            "status": "completed",
            "state": json.dumps(state),
            "checkpointed_at": datetime.now(timezone.utc).isoformat(),
        }
        df = self.spark.createDataFrame([record])
        write_to_delta(df, self.checkpoint_path, mode="append")
        logger.debug(f"Checkpoint saved: phase {phase}")

    def load(self) -> Optional[dict]:
        """Load the latest checkpoint state."""
        try:
            df = read_delta(self.spark, self.checkpoint_path)
            latest = df.orderBy(F.col("checkpointed_at").desc()).first()
            if latest:
                return {
                    "phase": latest["phase"],
                    "state": json.loads(latest["state"]),
                }
        except Exception:
            pass
        return None

    def get_last_completed_phase(self) -> int:
        """Get the last successfully completed phase number."""
        checkpoint = self.load()
        if checkpoint:
            return checkpoint["phase"]
        return 0


# ---------------------------------------------------------------------------
# Alerting (Azure Monitor / Event Hubs)
# ---------------------------------------------------------------------------

class AlertManager:
    """
    Pipeline alerting via Azure Monitor custom metrics and Event Hubs.

    Sends alerts for:
    - Phase failures
    - Quality gate failures (pass rate below threshold)
    - Performance degradation (phase duration exceeding baseline)
    - Data volume anomalies
    """

    def __init__(
        self,
        run_id: str,
        alert_thresholds: Optional[dict] = None,
        event_hub_connection_string: Optional[str] = None,
        event_hub_name: Optional[str] = None,
    ):
        self.run_id = run_id
        self.thresholds = alert_thresholds or {
            "quality_pass_rate_min": 0.80,
            "phase_duration_max_seconds": 3600,
            "volume_drop_ratio": 0.5,
        }
        self.event_hub_connection_string = event_hub_connection_string
        self.event_hub_name = event_hub_name
        self._alerts: list[dict] = []

    def send_alert(
        self,
        severity: str,
        title: str,
        message: str,
        phase: Optional[str] = None,
        metrics: Optional[dict] = None,
    ) -> None:
        """
        Send an alert.

        Severity levels: 'critical', 'error', 'warning', 'info'.

        In Azure, alerts are sent to Event Hubs for routing to:
        - Azure Monitor / Application Insights
        - PagerDuty / OpsGenie
        - Email via Logic Apps
        - Teams/Slack via webhooks
        """
        alert = {
            "run_id": self.run_id,
            "severity": severity,
            "title": title,
            "message": message,
            "phase": phase,
            "metrics": metrics or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._alerts.append(alert)

        logger.warning(f"[{severity.upper()}] {title}: {message}")

        # Send to Azure Event Hubs if configured
        if self.event_hub_connection_string and self.event_hub_name:
            self._send_to_event_hub(alert)

    def check_quality_gate(self, pass_rate: float, phase: str) -> None:
        """Alert if quality pass rate is below threshold."""
        threshold = self.thresholds.get("quality_pass_rate_min", 0.80)
        if pass_rate < threshold:
            self.send_alert(
                severity="warning",
                title=f"Quality gate below threshold in {phase}",
                message=f"Pass rate {pass_rate:.1%} < threshold {threshold:.1%}",
                phase=phase,
                metrics={"pass_rate": pass_rate, "threshold": threshold},
            )

    def check_phase_duration(self, duration_seconds: float, phase: str) -> None:
        """Alert if a phase takes longer than expected."""
        threshold = self.thresholds.get("phase_duration_max_seconds", 3600)
        if duration_seconds > threshold:
            self.send_alert(
                severity="warning",
                title=f"Phase {phase} exceeded duration threshold",
                message=f"Took {duration_seconds:.0f}s (threshold: {threshold}s)",
                phase=phase,
                metrics={"duration_seconds": duration_seconds},
            )

    def check_volume_anomaly(
        self, current_count: int, previous_count: int, phase: str
    ) -> None:
        """Alert on significant volume changes between runs."""
        if previous_count == 0:
            return
        ratio = current_count / previous_count
        threshold = self.thresholds.get("volume_drop_ratio", 0.5)
        if ratio < threshold:
            self.send_alert(
                severity="warning",
                title=f"Volume anomaly detected in {phase}",
                message=f"Record count dropped from {previous_count} to {current_count} ({ratio:.1%})",
                phase=phase,
                metrics={"current_count": current_count, "previous_count": previous_count},
            )

    def get_alerts(self) -> list[dict]:
        """Return all alerts generated during this run."""
        return self._alerts

    def _send_to_event_hub(self, alert: dict) -> None:
        """Send alert to Azure Event Hubs."""
        try:
            from azure.eventhub import EventHubProducerClient, EventData
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.event_hub_connection_string,
                eventhub_name=self.event_hub_name,
            )
            with producer:
                event_data = EventData(json.dumps(alert).encode("utf-8"))
                producer.send_batch([event_data])
        except ImportError:
            logger.debug("azure-eventhub not installed; alert logged only")
        except Exception as e:
            logger.error(f"Failed to send alert to Event Hub: {e}")


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """
    Circuit breaker for external service calls (LLM APIs, reference data APIs).

    Prevents cascading failures by stopping calls to a failing service
    after a threshold of consecutive failures is reached.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 60.0,
        half_open_max_calls: int = 3,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.half_open_max_calls = half_open_max_calls

        self._failure_count: int = 0
        self._last_failure_time: float = 0.0
        self._state: str = "closed"  # closed, open, half_open
        self._half_open_calls: int = 0

    @property
    def is_open(self) -> bool:
        """Check if the circuit breaker is open (blocking calls)."""
        if self._state == "closed":
            return False
        if self._state == "open":
            if time.time() - self._last_failure_time >= self.recovery_timeout_seconds:
                self._state = "half_open"
                self._half_open_calls = 0
                logger.info(f"Circuit breaker '{self.name}' -> half_open")
                return False
            return True
        if self._state == "half_open":
            return self._half_open_calls >= self.half_open_max_calls
        return False

    def success(self) -> None:
        """Record a successful call."""
        self._failure_count = 0
        if self._state == "half_open":
            self._half_open_calls += 1
            if self._half_open_calls >= self.half_open_max_calls:
                self._state = "closed"
                logger.info(f"Circuit breaker '{self.name}' -> closed")

    def failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.failure_threshold:
            self._state = "open"
            logger.warning(
                f"Circuit breaker '{self.name}' -> open "
                f"({self._failure_count} failures)"
            )


# ---------------------------------------------------------------------------
# Idempotency Guard
# ---------------------------------------------------------------------------

class IdempotencyGuard:
    """
    Ensures pipeline operations are idempotent — safe to re-run.

    Tracks which batches/runs have already been processed so that
    re-running the pipeline does not produce duplicate records.
    """

    def __init__(
        self,
        spark: SparkSession,
        workspace: Optional[str] = None,
    ):
        self.spark = spark
        self.guard_path = resolve_table_path(
            layer="metrics",
            table_name="idempotency_guard",
            workspace=workspace,
        )

    def was_processed(self, batch_id: str, phase: str) -> bool:
        """Check if a batch was already processed in a given phase."""
        try:
            guard_df = read_delta(self.spark, self.guard_path)
            count = guard_df.filter(
                (F.col("batch_id") == batch_id) & (F.col("phase") == phase)
            ).count()
            return count > 0
        except Exception:
            return False

    def mark_processed(self, batch_id: str, phase: str) -> None:
        """Mark a batch as processed for a given phase."""
        record = {
            "batch_id": batch_id,
            "phase": phase,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        df = self.spark.createDataFrame([record])
        write_to_delta(df, self.guard_path, mode="append")
