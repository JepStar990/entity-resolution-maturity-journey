"""
Phase 11 Production: LLM Matching at Scale (Azure-Native)

Production-grade LLM-based entity matching that actually executes at scale
using Spark UDFs with batch processing, rate limiting, and provider fallback.

Uses Azure OpenAI Service as the primary provider with:
- Spark mapInPandas for distributed batch inference
- Token bucket rate limiter per partition
- Exponential backoff with circuit breaker
- Automatic failover to fallback models
- Response caching to avoid redundant calls

Usage:
    from phase_11_llm_production import LLMMatcher
    matcher = LLMMatcher(spark, azure_openai_endpoint="...", azure_openai_key="...")
    results = matcher.match_pairs(pairs_df, entity_fields=["name", "email"])
"""

from __future__ import annotations

import time
import json
import hashlib
from typing import Optional, Iterator

import pandas as pd
import numpy as np

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, BooleanType,
)

from utils.logging_config import get_logger

logger = get_logger(__name__)

# Prompt template
MATCH_PROMPT = """You are an entity resolution system. Compare these two records.

Record A: {record_a}
Record B: {record_b}

Are they the same entity? Consider:
- Name variations, abbreviations, nicknames
- Address formatting differences
- Common typos and data entry errors
- Industry context

Respond ONLY with a JSON object:
{{"match": true/false, "confidence": 0.0-1.0, "reasoning": "<one sentence>"}}

JSON:"""


class TokenBucketRateLimiter:
    """Token bucket rate limiter for LLM API calls."""

    def __init__(self, rate: float, burst: int):
        self.rate = rate  # tokens per second
        self.burst = burst
        self.tokens = float(burst)
        self.last_refill = time.monotonic()

    def acquire(self) -> bool:
        """Try to acquire a token. Returns True if acquired."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_refill = now

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False

    def wait_and_acquire(self, timeout: float = 30.0) -> bool:
        """Wait until a token is available or timeout."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.acquire():
                return True
            time.sleep(0.1)
        return False


class LLMMatcher:
    """
    Production LLM matcher using Azure OpenAI Service.

    Handles:
    - Distributed batch inference via Spark mapInPandas
    - Rate limiting per partition
    - Exponential backoff and circuit breaker
    - Response caching (in-memory per partition)
    - Automatic provider failover
    """

    def __init__(
        self,
        spark: SparkSession,
        azure_endpoint: Optional[str] = None,
        azure_key: Optional[str] = None,
        azure_deployment: str = "gpt-4o",
        api_version: str = "2024-02-15-preview",
        requests_per_second: float = 10.0,
        max_retries: int = 3,
        fallback_deployment: Optional[str] = "gpt-4o-mini",
    ):
        self.spark = spark
        self.azure_endpoint = azure_endpoint
        self.azure_key = azure_key
        self.azure_deployment = azure_deployment
        self.api_version = api_version
        self.requests_per_second = requests_per_second
        self.max_retries = max_retries
        self.fallback_deployment = fallback_deployment

    def match_pairs(
        self,
        pairs_df: DataFrame,
        entity_fields: list[str],
        confidence_band: tuple[float, float] = (0.3, 0.7),
        max_pairs: Optional[int] = None,
    ) -> DataFrame:
        """
        Match uncertain entity pairs using LLM at scale.

        Only pairs in the confidence band are sent to the LLM.
        High-confidence pairs are passed through unchanged.

        Args:
            pairs_df: DataFrame of candidate pairs with match_probability.
            entity_fields: Fields to include in the LLM prompt.
            confidence_band: (low, high) threshold for LLM review.
            max_pairs: Cap on pairs to process (None = unlimited).

        Returns:
            DataFrame with llm_match, llm_confidence, llm_reasoning columns.
        """
        low, high = confidence_band

        # Split: uncertain pairs get LLM review
        uncertain = pairs_df.filter(
            (F.col("match_probability") >= low)
            & (F.col("match_probability") <= high)
        )

        high_conf = pairs_df.filter(
            (F.col("match_probability") < low)
            | (F.col("match_probability") > high)
        )

        uncertain_count = uncertain.count()
        logger.info(f"LLM review: {uncertain_count} uncertain pairs in band [{low}, {high}]")

        if uncertain_count == 0:
            high_conf = high_conf.withColumn("llm_match", F.lit(None).cast(BooleanType()))
            high_conf = high_conf.withColumn("llm_confidence", F.lit(None).cast(FloatType()))
            high_conf = high_conf.withColumn("llm_reasoning", F.lit(None).cast(StringType()))
            return high_conf

        # Apply limit if specified
        if max_pairs and uncertain_count > max_pairs:
            uncertain = uncertain.limit(max_pairs)
            logger.info(f"Capped LLM review at {max_pairs} pairs")

        # Distribute LLM calls via mapInPandas
        # Each partition gets its own rate limiter and cache
        endpoint = self.azure_endpoint
        key = self.azure_key
        deployment = self.azure_deployment
        api_ver = self.api_version
        rps = self.requests_per_second
        retries = self.max_retries
        fallback = self.fallback_deployment

        left_fields = [f"{f}_left" for f in entity_fields]
        right_fields = [f"{f}_right" for f in entity_fields]

        # Only keep fields that exist
        available_left = [c for c in left_fields if c in uncertain.columns]
        available_right = [c for c in right_fields if c in uncertain.columns]
        entity_cols = list(set(available_left + available_right))

        select_cols = ["id_left", "id_right", "match_probability"] + entity_cols
        select_cols = [c for c in select_cols if c in uncertain.columns]

        limited_df = uncertain.select(*select_cols)

        def llm_partition_handler(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            """Process a Spark partition with LLM calls."""
            rate_limiter = TokenBucketRateLimiter(rate=rps, burst=int(rps * 2))
            cache: dict[str, dict] = {}

            for pdf in iterator:
                results = []
                for _, row in pdf.iterrows():
                    pair_id = f"{row.get('id_left', '')}|{row.get('id_right', '')}"
                    cache_key = hashlib.md5(pair_id.encode()).hexdigest()

                    if cache_key in cache:
                        result = cache[cache_key]
                    else:
                        record_a = _format_record(row, entity_fields, "_left")
                        record_b = _format_record(row, entity_fields, "_right")
                        prompt = MATCH_PROMPT.format(
                            record_a=record_a,
                            record_b=record_b,
                        )

                        if not rate_limiter.wait_and_acquire(timeout=30.0):
                            result = {"match": None, "confidence": None, "reasoning": "rate_limit_timeout"}

                        result = _call_azure_openai(
                            prompt=prompt,
                            endpoint=endpoint,
                            api_key=key,
                            deployment=deployment,
                            api_version=api_ver,
                            max_retries=retries,
                            fallback_deployment=fallback,
                        )
                        cache[cache_key] = result

                    results.append({
                        "id_left": row.get("id_left", ""),
                        "id_right": row.get("id_right", ""),
                        "llm_match": result.get("match"),
                        "llm_confidence": result.get("confidence"),
                        "llm_reasoning": result.get("reasoning", ""),
                    })

                yield pd.DataFrame(results)

        # Define output schema
        output_schema = StructType([
            StructField("id_left", StringType(), True),
            StructField("id_right", StringType(), True),
            StructField("llm_match", BooleanType(), True),
            StructField("llm_confidence", FloatType(), True),
            StructField("llm_reasoning", StringType(), True),
        ])

        # Execute distributed LLM calls
        llm_results = limited_df.mapInPandas(
            llm_partition_handler,
            schema=output_schema,
        )

        # Merge LLM results back
        high_conf = high_conf.withColumn("llm_match", F.lit(None).cast(BooleanType()))
        high_conf = high_conf.withColumn("llm_confidence", F.lit(None).cast(FloatType()))
        high_conf = high_conf.withColumn("llm_reasoning", F.lit(None).cast(StringType()))

        uncertain_with_llm = uncertain.join(
            llm_results, on=["id_left", "id_right"], how="left"
        )

        # Union all results
        result = high_conf.unionByName(uncertain_with_llm, allowMissingColumns=True)

        # Final match decision
        result = result.withColumn(
            "final_match",
            F.when(
                F.col("llm_match").isNotNull(),
                F.col("llm_match"),
            ).otherwise(F.col("_is_match")),
        )

        logger.info(f"LLM matching complete: {llm_results.count()} pairs reviewed")
        return result


@retry_with_backoff(max_retries=3, base_delay_seconds=1.0)
def _call_azure_openai(
    prompt: str,
    endpoint: str,
    api_key: str,
    deployment: str,
    api_version: str = "2024-02-15-preview",
    max_retries: int = 3,
    fallback_deployment: Optional[str] = None,
) -> dict:
    """
    Call Azure OpenAI Service for entity matching.

    Args:
        prompt: The formatted prompt.
        endpoint: Azure OpenAI endpoint URL.
        api_key: Azure OpenAI API key.
        deployment: Model deployment name.
        api_version: API version.
        max_retries: Number of retries.
        fallback_deployment: Fallback deployment if primary fails.

    Returns:
        Dict with match, confidence, reasoning.
    """
    default_result = {"match": None, "confidence": None, "reasoning": "api_unavailable"}

    if not endpoint or not api_key:
        return default_result

    try:
        import urllib.request
        import urllib.error

        url = f"{endpoint.rstrip('/')}/openai/deployments/{deployment}/chat/completions?api-version={api_version}"

        body = json.dumps({
            "messages": [
                {"role": "system", "content": "You are an entity resolution expert. Respond only with JSON."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
            "max_tokens": 200,
            "response_format": {"type": "json_object"},
        }).encode("utf-8")

        req = urllib.request.Request(url, data=body, headers={
            "Content-Type": "application/json",
            "api-key": api_key,
        })

        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
            content = data["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            return {
                "match": parsed.get("match", False),
                "confidence": float(parsed.get("confidence", 0.5)),
                "reasoning": parsed.get("reasoning", ""),
            }

    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else str(e)
        logger.error(f"Azure OpenAI HTTP {e.code}: {error_body}")

        # Try fallback deployment
        if fallback_deployment and fallback_deployment != deployment:
            logger.info(f"Falling back to {fallback_deployment}")
            return _call_azure_openai(
                prompt, endpoint, api_key, fallback_deployment,
                api_version, max_retries - 1, None,
            )

        return default_result

    except Exception as e:
        logger.error(f"Azure OpenAI call failed: {e}")
        return default_result


def _format_record(row, fields: list[str], suffix: str) -> str:
    """Format a record for LLM prompt."""
    lines = []
    for field in fields:
        col = f"{field}{suffix}"
        val = row.get(col, "")
        if val is not None and str(val).strip():
            lines.append(f"  {field}: {val}")
    return "\n".join(lines) if lines else "(no data)"


# Re-import decorator for module-level use
from engine.production_hardening import retry_with_backoff
