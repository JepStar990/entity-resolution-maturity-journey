"""
Phase 11: Semantic Matching with LLMs

Uses Large Language Models (LLMs) to resolve ambiguous entity matches
that probabilistic models cannot confidently decide. LLMs understand
semantic meaning, abbreviations, and contextual relationships.

Input: Low-confidence candidate pairs from Phase 10
Output: Semantic match scores; LLM-augmented match decisions

Medallion Layer: Gold
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


# Prompt template for entity matching
MATCH_PROMPT = """You are an entity resolution expert. Compare the two records below and determine if they refer to the same entity.

Record A:
{record_a}

Record B:
{record_b}

Consider:
1. Name variations, abbreviations, and nicknames
2. Address similarities and formatting differences
3. Industry/business context
4. Common data entry errors

Respond with a JSON object:
{{"match": true/false, "confidence": 0.0-1.0, "reasoning": "<one sentence explanation>"}}

JSON:"""


def format_record_for_llm(
    row: dict,
    prefix: str,
    fields: list[str],
) -> str:
    """
    Format a record as a human-readable string for LLM prompt injection.

    Args:
        row: Dict of column values (single row).
        prefix: Column prefix (e.g., '_left', '_right').
        fields: Base field names without prefix.

    Returns:
        Formatted string representation.
    """
    lines = []
    for field in fields:
        col = f"{field}{prefix}"
        val = row.get(col, "")
        if val is not None and str(val).strip():
            lines.append(f"  {field}: {val}")
    return "\n".join(lines) if lines else "(no data)"


def build_llm_prompt(
    row_a: dict,
    row_b: dict,
    entity_fields: list[str],
) -> str:
    """
    Build a prompt for LLM-based entity matching.

    Args:
        row_a: Left-side record as dict.
        row_b: Right-side record as dict.
        entity_fields: Fields to include in the prompt.

    Returns:
        Prompt string.
    """
    record_a = format_record_for_llm(row_a, "_left", entity_fields)
    record_b = format_record_for_llm(row_b, "_right", entity_fields)
    return MATCH_PROMPT.format(record_a=record_a, record_b=record_b)


def call_llm_match(
    prompt: str,
    model: str = "gpt-4o",
    api_key: Optional[str] = None,
    temperature: float = 0.0,
    max_tokens: int = 200,
) -> dict:
    """
    Call an LLM API for entity matching.

    Supports Azure OpenAI, OpenAI, and LiteLLM for provider-agnostic access.
    Returns parsed match decision.

    Args:
        prompt: The formatted prompt string.
        model: Model identifier (Azure OpenAI, OpenAI, or LiteLLM format).
        api_key: API key (read from env if not provided).
        temperature: LLM temperature.
        max_tokens: Maximum tokens in response.

    Returns:
        Dict with keys: match, confidence, reasoning, raw_response.
    """
    default_result = {
        "match": False,
        "confidence": 0.0,
        "reasoning": "LLM unavailable",
        "raw_response": "",
    }

    try:
        import litellm

        messages = [{"role": "user", "content": prompt}]
        response = litellm.completion(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
        )
        content = response.choices[0].message.content

        # Parse JSON from response
        import json
        try:
            # Extract JSON object from the response
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            result = json.loads(content.strip())
        except (json.JSONDecodeError, IndexError):
            # Fallback: try to find JSON-like structure
            import re
            match = re.search(r'\{"match":\s*(true|false).*\}', content, re.DOTALL)
            if match:
                try:
                    result = json.loads(match.group())
                except json.JSONDecodeError:
                    result = default_result
            else:
                result = default_result

        return {
            "match": result.get("match", False),
            "confidence": float(result.get("confidence", 0.5)),
            "reasoning": result.get("reasoning", ""),
            "raw_response": content,
        }

    except ImportError:
        logger.warning("litellm not installed; LLM matching unavailable")
        return default_result
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return default_result


def select_low_confidence_pairs(
    scored_pairs: DataFrame,
    low_threshold: float = 0.3,
    high_threshold: float = 0.7,
) -> DataFrame:
    """
    Select pairs in the uncertainty band for LLM review.

    Pairs with probability below low_threshold are likely non-matches.
    Pairs above high_threshold are likely matches.
    The band between is where LLM semantic understanding adds value.

    Args:
        scored_pairs: DataFrame from Phase 10 with match_probability.
        low_threshold: Lower bound of uncertainty band.
        high_threshold: Upper bound of uncertainty band.

    Returns:
        DataFrame of uncertain pairs.
    """
    uncertain = scored_pairs.filter(
        (F.col("match_probability") >= low_threshold)
        & (F.col("match_probability") <= high_threshold)
    )
    return uncertain


def run(
    spark: SparkSession,
    scored_pairs: DataFrame,
    original_df: Optional[DataFrame] = None,
    entity_type: str = "customer",
    config: Optional[dict] = None,
    gold_path: str = "Tables/gold/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> DataFrame:
    """
    Execute Phase 11: apply LLM semantic matching to uncertain pairs.

    In production, this would submit batched LLM calls via a UDF or
    map-reduce pattern. This implementation provides the scaffolding
    with a synchronous per-row UDF for demonstration.

    Args:
        spark: Active Spark session.
        scored_pairs: Match-scored pairs from Phase 10.
        original_df: Original entity DataFrame for record details (optional).
        entity_type: Type of entity.
        config: Dict with:
            - low_confidence_threshold: Lower bound for LLM review.
            - high_confidence_threshold: Upper bound for LLM review.
            - llm_model: Model identifier (default: gpt-4o).
            - llm_api_key: API key for LLM service.
            - entity_fields: Fields to include in LLM prompt.
        gold_path: Gold layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        DataFrame of match scored pairs with LLM-augmented decisions.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="11-llm-semantic")

    config = config or {}
    logger.info(f"Running LLM semantic matching for {entity_type}")

    low_threshold = config.get("low_confidence_threshold", 0.3)
    high_threshold = config.get("high_confidence_threshold", 0.7)
    llm_model = config.get("llm_model", "gpt-4o")
    llm_api_key = config.get("llm_api_key")
    entity_fields = config.get("entity_fields", ["full_name_std", "email_std", "phone_std"])

    # Select uncertain pairs
    uncertain = select_low_confidence_pairs(scored_pairs, low_threshold, high_threshold)
    uncertain_count = uncertain.count()

    if uncertain_count == 0:
        logger.info("No uncertain pairs for LLM review")
        scored_pairs = scored_pairs.withColumn("llm_match", F.lit(None).cast("boolean"))
        scored_pairs = scored_pairs.withColumn("llm_confidence", F.lit(None).cast(FloatType()))
        scored_pairs = scored_pairs.withColumn("final_match", F.col("_is_match"))
        return scored_pairs

    logger.info(f"Submitting {uncertain_count} uncertain pairs for LLM review")

    # For demonstration: mark pairs for LLM review
    # In production, use a UDF or batch API call pattern
    # This shows the integration pattern

    # Add LLM review columns to uncertain pairs
    uncertain = uncertain.withColumn(
        "_needs_llm_review",
        F.lit(True),
    )

    # Add a placeholder LLM decision column
    # In production: apply llm_match UDF per row
    uncertain = uncertain.withColumn(
        "llm_match",
        F.lit(None).cast("boolean"),
    )
    uncertain = uncertain.withColumn(
        "llm_confidence",
        F.lit(None).cast(FloatType()),
    )
    uncertain = uncertain.withColumn(
        "llm_reasoning",
        F.lit(None).cast(StringType()),
    )

    # Merge LLM decisions back with high-confidence automated decisions
    high_confidence = scored_pairs.filter(
        (F.col("match_probability") < low_threshold)
        | (F.col("match_probability") > high_threshold)
    )
    high_confidence = high_confidence.withColumn("_needs_llm_review", F.lit(False))
    high_confidence = high_confidence.withColumn("llm_match", F.lit(None).cast("boolean"))
    high_confidence = high_confidence.withColumn("llm_confidence", F.lit(None).cast(FloatType()))
    high_confidence = high_confidence.withColumn("llm_reasoning", F.lit(None).cast(StringType()))

    # Combine
    result = high_confidence.unionByName(uncertain, allowMissingColumns=True)

    # Final match decision: LLM takes precedence for uncertain pairs
    result = result.withColumn(
        "final_match",
        F.when(
            F.col("_needs_llm_review") == True,  # noqa: E712
            F.col("llm_match"),
        ).otherwise(F.col("_is_match")),
    )

    # Write results to Gold
    target_path = resolve_table_path(
        layer="gold",
        table_name=f"{table_name}_llm_matches",
        workspace=workspace,
    )
    write_to_delta(result, target_path, mode="overwrite")

    # Emit metrics
    metrics.log_count("uncertain_pairs_for_llm", uncertain_count)
    metrics.log_count("total_scored_pairs", scored_pairs.count())
    metrics.log_metric("llm_review_ratio", round(uncertain_count / max(scored_pairs.count(), 1), 4))
    metrics.flush()

    logger.info(
        f"LLM review: {uncertain_count} uncertain pairs flagged for review "
        f"({uncertain_count / max(scored_pairs.count(), 1) * 100:.1f}% of total)"
    )
    return result
