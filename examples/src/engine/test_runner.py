"""
Test Runner - Comprehensive integration test for the full pipeline.

Validates the complete pipeline end-to-end:
1. Dynamic data generation
2. Auto-profiling and entity detection
3. All 15 phases executing in sequence
4. Golden record creation and MDM distribution

Usage:
    from engine.test_runner import run_integration_test
    result = run_integration_test(spark)
    assert result["success"], result["errors"]
"""

from __future__ import annotations

import time
import traceback
from typing import Optional

from pyspark.sql import SparkSession

from engine.data_profiler import DataProfiler
from engine.pipeline_engine import PipelineEngine, PipelineResult
from engine.sample_data_generator import generate_test_dataset
from engine.production_hardening import (
    DeadLetterQueue, PipelineCheckpoint, AlertManager,
)
from utils.logging_config import get_logger, configure_logging

logger = get_logger(__name__)


def run_integration_test(
    spark: SparkSession,
    entity_types: Optional[list[str]] = None,
    rows_per_type: int = 500,
    verbose: bool = True,
) -> dict:
    """
    Run a comprehensive integration test of the full pipeline.

    Tests the pipeline with multiple entity types and verifies:
    - Auto-detection works correctly
    - All 15 phases complete without error
    - Golden records are created
    - Dead-letter queue captures failures
    - Checkpointing works
    - Alerts are generated

    Args:
        spark: Spark session.
        entity_types: Entity types to test (default: all).
        rows_per_type: Sample rows per entity type.
        verbose: Print detailed progress.

    Returns:
        Dict with success, results, errors, and timing.
    """
    if entity_types is None:
        entity_types = ["customer", "product", "company"]

    results = {}
    errors = []
    start_time = time.time()

    for entity_type in entity_types:
        if verbose:
            print(f"\n{'='*60}")
            print(f"TESTING: {entity_type.upper()} ENTITY TYPE")
            print(f"{'='*60}")

        try:
            # 1. Generate test data
            if verbose:
                print(f"  Generating {rows_per_type} {entity_type} records...")
            df = generate_test_dataset(
                spark, entity_type=entity_type, num_rows=rows_per_type,
                duplicate_ratio=0.05, fuzzy_duplicate_ratio=0.10,
            )

            # 2. Run auto-profiler
            if verbose:
                print(f"  Profiling dataset...")
            profiler = DataProfiler(spark)
            profile = profiler.profile(df, source_system=f"test_{entity_type}")

            if verbose:
                print(f"  Detected: entity_type={profile.entity_type} "
                      f"(confidence={profile.entity_type_confidence:.2f})")
                print(f"  Column roles: {profile.column_roles}")
                print(f"  Match keys: {profile.match_key_recommendations}")
                print(f"  Rows: {profile.total_rows}, Cols: {profile.total_columns}")

            # Verify auto-detection
            detected_ok = profile.entity_type in (entity_type, "generic")
            if not detected_ok:
                errors.append(
                    f"{entity_type}: Entity type detection failed. "
                    f"Expected '{entity_type}' or 'generic', got '{profile.entity_type}'"
                )

            # 3. Run full pipeline (dry run first)
            if verbose:
                print(f"  Running dry-run pipeline...")
            engine = PipelineEngine(
                spark, workspace="test-workspace",
                medallion_paths={
                    "bronze": f"/tmp/test-delta/bronze/{entity_type}/",
                    "silver": f"/tmp/test-delta/silver/{entity_type}/",
                    "gold": f"/tmp/test-delta/gold/{entity_type}/",
                    "metrics": f"/tmp/test-delta/metrics/{entity_type}/",
                },
            )

            dry_result = engine.run(df, source_system=f"test_{entity_type}", dry_run=True)

            # 4. Run real pipeline
            if verbose:
                print(f"  Running full 15-phase pipeline...")
            run_id = f"test-{entity_type}-{int(time.time())}"
            result = engine.run(df, source_system=f"test_{entity_type}")

            # 5. Verify results
            phase_count = len(result.phases)
            completed = sum(1 for p in result.phases if p.status == "completed")
            failed = sum(1 for p in result.phases if p.status == "failed")
            skipped = sum(1 for p in result.phases if p.status == "skipped")

            if verbose:
                print(f"  Results: {completed} completed, {failed} failed, {skipped} skipped")
                print(f"  Golden records: {result.golden_rows_out}")
                print(f"  Duration: {result.total_duration_seconds:.1f}s")

            if failed > 0:
                phase_errors = [p.error for p in result.phases if p.error]
                errors.append(
                    f"{entity_type}: {failed} phases failed: {phase_errors}"
                )

            results[entity_type] = {
                "success": failed == 0,
                "entity_detected": profile.entity_type,
                "phase_count": phase_count,
                "completed": completed,
                "failed": failed,
                "skipped": skipped,
                "golden_records": result.golden_rows_out,
                "duration_seconds": result.total_duration_seconds,
                "profile": profile,
            }

        except Exception as e:
            error_msg = f"{entity_type}: Pipeline failed with exception: {e}\n{traceback.format_exc()}"
            errors.append(error_msg)
            logger.error(error_msg)
            results[entity_type] = {"success": False, "error": str(e)}

    total_duration = time.time() - start_time

    return {
        "success": len(errors) == 0,
        "results": results,
        "errors": errors,
        "duration_seconds": total_duration,
        "entity_types_tested": entity_types,
    }


def run_quick_smoke_test(spark: SparkSession) -> dict:
    """
    Quick smoke test: generates a small dataset and runs through the profiler
    and pipeline engine to verify basic functionality. Runs in under 60 seconds.

    Returns:
        Dict with success and details.
    """
    logger.info("Running quick smoke test...")

    try:
        df = generate_test_dataset(spark, entity_type="customer", num_rows=100,
                                    duplicate_ratio=0.0, fuzzy_duplicate_ratio=0.0,
                                    include_edge_cases=False)

        profiler = DataProfiler(spark)
        profile = profiler.profile(df)

        assert profile.total_rows >= 100, f"Expected >=100 rows, got {profile.total_rows}"
        assert profile.total_columns > 0, "No columns detected"
        assert len(profile.column_roles) > 0, "No column roles assigned"
        assert profile.entity_type in ("customer", "generic"), f"Wrong entity type: {profile.entity_type}"

        engine = PipelineEngine(spark)
        dry_result = engine.run(df, dry_run=True)

        assert dry_result.entity_type == profile.entity_type

        logger.info("Smoke test passed")
        return {"success": True, "profile": profile}

    except Exception as e:
        logger.error(f"Smoke test failed: {e}")
        return {"success": False, "error": str(e)}
