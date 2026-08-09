"""
Logging configuration for the entity resolution pipeline.

Configures structured logging that works across local development,
Fabric notebooks, and Databricks. Provides log context enrichment
with run_id, phase, and medallion layer.

Usage:
    from utils.logging_config import get_logger, configure_logging
    configure_logging(level="INFO")
    logger = get_logger(__name__)
"""

from __future__ import annotations

import logging
import sys
from typing import Optional


LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] %(name)s "
    "run=%(run_id)s phase=%(phase)s layer=%(layer)s "
    "%(message)s"
)

LOG_FORMAT_SIMPLE = (
    "%(asctime)s [%(levelname)s] %(name)s %(message)s"
)


class ContextFilter(logging.Filter):
    """Inject pipeline context (run_id, phase, layer) into log records."""

    def __init__(self, run_id: str = "", phase: str = "", layer: str = ""):
        super().__init__()
        self.run_id = run_id
        self.phase = phase
        self.layer = layer

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = getattr(record, "run_id", self.run_id) or "-"
        record.phase = getattr(record, "phase", self.phase) or "-"
        record.layer = getattr(record, "layer", self.layer) or "-"
        return True


_context_filter: Optional[ContextFilter] = None


def configure_logging(
    level: str = "INFO",
    run_id: str = "",
    phase: str = "",
    layer: str = "",
    log_format: Optional[str] = None,
) -> None:
    """
    Configure the root logger for the pipeline.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR).
        run_id: Run identifier for log enrichment.
        phase: Current phase identifier.
        layer: Medallion layer (bronze, silver, gold).
        log_format: Custom log format string.
    """
    global _context_filter

    fmt = log_format or LOG_FORMAT_SIMPLE
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%dT%H:%M:%S"))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    _context_filter = ContextFilter(
        run_id=run_id, phase=phase, layer=layer
    )
    root.addFilter(_context_filter)

    # Silence noisy third-party loggers
    for noisy in ("py4j", "pyspark", "azure", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with pipeline context.

    Args:
        name: Logger name (typically __name__).

    Returns:
        Configured logger instance.
    """
    return logging.getLogger(name)


def set_context(
    run_id: Optional[str] = None,
    phase: Optional[str] = None,
    layer: Optional[str] = None,
) -> None:
    """
    Update the logging context during a run.

    Args:
        run_id: New run identifier.
        phase: New phase identifier.
        layer: New medallion layer.
    """
    global _context_filter
    if _context_filter is None:
        _context_filter = ContextFilter()
        logging.getLogger().addFilter(_context_filter)
    if run_id is not None:
        _context_filter.run_id = run_id
    if phase is not None:
        _context_filter.phase = phase
    if layer is not None:
        _context_filter.layer = layer
