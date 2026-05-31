# Phase 11: Semantic Matching with LLMs

## Phase Overview

Leverage Large Language Models (LLMs) to understand meaning beyond strings — resolving abbreviations, legal name variations, and contextual equivalence that traditional algorithms miss. LLMs excel at the hardest matching cases that confuse both fuzzy string metrics and ML models.

---

## Business Context

### Why This Phase Matters

Traditional algorithms and even ML models struggle with cases requiring **semantic understanding**:

| Record A | Record B | Traditional Score | LLM Verdict |
|----------|----------|-------------------|-------------|
| `IBM` | `International Business Machines` | Jaro-Winkler: 0.47 | Match (abbreviation) |
| `Apple Inc.` | `Apple` | Token: 0.50 | Match (common shorthand) |
| `Apt 4B` | `Apartment 4-B` | Levenshtein: 0.60 | Match (format variation) |
| `Google LLC` | `Alphabet Inc.` | All low: 0.20 | No match (different entities) |
| `Bank of America, N.A.` | `BofA` | Jaro-Winkler: 0.40 | Match (common nickname) |

ML models can partially address these if enough labeled examples exist — but LLMs bring **world knowledge** that requires zero training data. They know that "IBM" stands for "International Business Machines" without ever being trained on your entity resolution dataset.

### Capability Unlocked

Human-level semantic understanding for the hardest matching cases, with zero or few-shot learning.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Low-confidence pairs from Phase 10 (probability 40–80%) |
| **Pairs** | Entity pairs that ML couldn't confidently classify |
| **Volume** | Typically 5–15% of the original candidate pairs |

---

## Processing Logic

### Step 1: Prompt Construction

```python
SYSTEM_PROMPT = """You are an entity resolution expert. Your task is to determine
whether two records represent the same real-world entity.

Consider:
- Abbreviations and acronyms (IBM = International Business Machines)
- Legal suffixes (Inc., LLC, Ltd., Pty Ltd, N.A.)
- Formatting variations (Apt 4B = Apartment 4-B)
- Common nicknames and shorthand
- Different languages or transliterations

Do NOT match records just because they are in the same industry or category.
Different branches of the same company are DIFFERENT entities."""


def build_matching_prompt(record_a, record_b):
    return f"""
## Record A
- Name: {record_a.get('company_name', 'N/A')}
- Address: {record_a.get('address', 'N/A')}
- City: {record_a.get('city', 'N/A')}
- Country: {record_a.get('country', 'N/A')}
- Industry: {record_a.get('industry', 'N/A')}

## Record B
- Name: {record_b.get('company_name', 'N/A')}
- Address: {record_b.get('address', 'N/A')}
- City: {record_b.get('city', 'N/A')}
- Country: {record_b.get('country', 'N/A')}
- Industry: {record_b.get('industry', 'N/A')}

Are these the same entity? Answer with a JSON object:
{{"match": true/false, "confidence": 0.0-1.0, "reasoning": "brief explanation"}}
"""
```

### Step 2: Batch LLM Evaluation

```python
import asyncio
from openai import AsyncOpenAI

client = AsyncOpenAI()

async def evaluate_pair(pair):
    prompt = build_matching_prompt(pair["record_a"], pair["record_b"])
    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"},
        temperature=0.0,  # Deterministic for entity resolution
        max_tokens=500
    )
    result = json.loads(response.choices[0].message.content)
    return {**pair, "llm_match": result["match"],
            "llm_confidence": result["confidence"],
            "llm_reasoning": result["reasoning"]}


async def batch_evaluate(pairs, concurrency=10):
    """Evaluate pairs in parallel with rate limiting."""
    semaphore = asyncio.Semaphore(concurrency)
    async def with_limit(pair):
        async with semaphore:
            return await evaluate_pair(pair)

    return await asyncio.gather(*[with_limit(p) for p in pairs])
```

### Step 3: Cost and Latency Guardrails

```python
# Estimate cost before running (GPT-4o: ~$5/1M input tokens, ~$15/1M output tokens)
def estimate_cost(pairs):
    avg_input_tokens = len(SYSTEM_PROMPT.split()) + 200  # ~400 tokens
    avg_output_tokens = 100  # JSON response
    total_input_tokens = len(pairs) * avg_input_tokens
    total_output_tokens = len(pairs) * avg_output_tokens
    cost = (total_input_tokens / 1_000_000 * 5.0
            + total_output_tokens / 1_000_000 * 15.0)
    return cost

# Only send to LLM if ML confidence is in the uncertain range
uncertain_pairs = df_results.filter(
    (col("probability") >= 0.40) & (col("probability") <= 0.80)
)

estimated_cost = estimate_cost(uncertain_pairs)
print(f"LLM evaluation cost: ${estimated_cost:.2f} for {uncertain_pairs.count()} pairs")
```

### Step 4: Merge LLM Results with ML Predictions

```python
# LLM high-confidence matches override ML uncertainty
df_final = df_results \
    .withColumn("final_decision",
        when(col("llm_confidence") >= 0.90, lit("auto_merge"))
        .when(col("llm_confidence") >= 0.70, lit("review"))
        .when(col("llm_match") == True, lit("review"))
        .otherwise(col("decision"))  # Fall back to ML decision
    )
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **LLM Scores** | Match/no-match + confidence + reasoning per pair |
| **Merged Decisions** | Combined ML + LLM verdicts |
| **Cost Report** | Tokens consumed, cost incurred, pairs evaluated |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **OpenAI API (GPT-4o)** | LLM evaluation | State-of-the-art reasoning, JSON mode |
| **Anthropic API (Claude)** | Alternative LLM | Strong at nuanced comparisons |
| **Open-source models (Llama, Mistral)** | Self-hosted LLM | No data leaving your network |
| **LangChain / LiteLLM** | LLM orchestration | Provider-agnostic API, retry logic, cost tracking |
| **Redis** | Response cache | Identical pairs don't re-invoke the LLM |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-11-llm-semantic.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_11_llm_semantic.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/11-llm-semantic-matching.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| LLM resolution rate | ≥ 80% of uncertain pairs resolved | Pairs with LLM confidence ≥ 0.90 or ≤ 0.30 |
| LLM accuracy | ≥ 95% on known test set | LLM verdict vs ground truth on held-out pairs |
| Cost per pair | < $0.01 | Total cost / pairs evaluated |
| Latency per pair | < 2 seconds | End-to-end including API round trip |

---

## When to Advance

Move to Phase 12 when:

- [ ] LLM evaluation is integrated for uncertain ML pairs.
- [ ] Cost estimation and guardrails are in place.
- [ ] LLM responses are cached (identical or near-identical pairs).
- [ ] A process exists for reviewing LLM decisions that differ from ML predictions.
- [ ] Private data handling is reviewed (if using external LLM APIs).

---

## Common Pitfalls

### 1. Sending Everything to the LLM

**Problem**: Every candidate pair goes to the LLM. For 100K pairs, cost is ~$500 and latency is ~55 hours (at 1 pair/second). This is wasteful when ML confidently classifies 90% of pairs.

**Fix**: Only send pairs where ML confidence is uncertain (typically 40–80%). The LLM adds value on hard cases; for easy cases, ML is sufficient and far cheaper.

### 2. Prompt Instability

**Problem**: A prompt that works perfectly in testing produces garbled responses in production because of a model update or minor prompt drift.

**Fix**: Pin the model version (e.g., `gpt-4o-2024-08-06`). Use structured output modes (JSON mode, function calling). Validate every LLM response against a schema before accepting it. Implement retry logic for malformed responses.

### 3. Data Privacy with External LLMs

**Problem**: Sending customer PII (names, addresses, phone numbers) to an external LLM API may violate GDPR, CCPA, or internal data governance policies.

**Fix**: Evaluate whether data can be sent to external APIs. If not, use self-hosted models (Llama 3, Mistral) or an Azure/AWS private deployment. Consider pseudonymizing data before sending (replace names with placeholders, use hash-based lookups to reconstruct results).

### 4. LLM Hallucination in Entity Resolution

**Problem**: The LLM confidently declares "IBM" and "Apple" are the same entity because "both are technology companies." It hallucinates a connection that doesn't exist.

**Fix**: Instruct the LLM to be conservative. Include negative examples in the prompt. Use chain-of-thought: "First, identify the specific entity each record refers to. Then, determine if they are the same entity. Then, explain your reasoning step by step." Always validate LLM decisions against other signals (address, phone, industry).

---

## Further Reading

- [GPT-4 for Entity Resolution Benchmarks](https://arxiv.org/abs/2304.10566)
- [Prompt Engineering for Record Linkage](https://arxiv.org/abs/2310.10883)
- [LiteLLM: Multi-Provider LLM Proxy](https://github.com/BerriAI/litellm)
- [Self-Hosted LLMs with vLLM](https://github.com/vllm-project/vllm)

---

[:material-arrow-left: Previous: Phase 10](phase-10-probabilistic-matching.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 12](phase-12-embedding-matching.md)
