# Phase 3: Data Quality Rules

## Phase Overview

Apply data quality rules to identify bad data before it contaminates downstream processes. This phase uses a declarative expectations framework (Great Expectations) to measure completeness, validity, uniqueness, and consistency, generating quality metrics and gating progression to the Silver layer.

---

## Business Context

### Why This Phase Matters

Schema validation (Phase 2) ensures data has the right **shape**. Data quality rules ensure data has the right **content**.

A record can pass schema validation (valid date format, valid email regex) and still be garbage:
- `age = -5` (valid integer, impossible value)
- `salary = $0.50/hour` (valid float, below minimum wage)
- `customer_id = NULL` (schema says nullable, business logic says required)

Without quality rules, bad data flows into matching engines and produces bad matches (garbage in, garbage out). Data quality is the last gate before the Silver layer.

### Capability Unlocked

Measurable, enforced data quality standards. The organization can quantify data trustworthiness and alert when quality degrades.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Validated Bronze table from Phase 2 |
| **Schema** | Enforced — correct columns, types, formats |
| **Content** | Unknown — may contain nulls, outliers, duplicates, invalid values |
| **Volume** | Passed records from Phase 2 |

---

## Processing Logic

### Step 1: Define Expectations

Great Expectations uses a declarative syntax to define what "good data" looks like:

```python
import great_expectations as gx

context = gx.get_context()

suite = context.create_expectation_suite("customer_quality_suite")

# Completeness checks
suite.expect_column_values_to_not_be_null("customer_id")
suite.expect_column_values_to_not_be_null("email")

# Range validation
suite.expect_column_values_to_be_between("age", min_value=0, max_value=120)
suite.expect_column_values_to_be_between("salary", min_value=15000, max_value=500000)

# Uniqueness
suite.expect_column_values_to_be_unique("customer_id")

# Set membership (valid enum values)
suite.expect_column_values_to_be_in_set(
    "status",
    value_set=["active", "inactive", "suspended", "closed"]
)

# Compound business rules
suite.expect_column_pair_values_a_to_be_greater_than_b(
    "end_date", "start_date"
)

# Regex patterns
suite.expect_column_values_to_match_regex(
    "phone",
    r'^\+?[1-9]\d{1,14}$'  # E.164 format
)
```

### Step 2: Run Validation

```python
batch = context.get_batch_list(
    suite=suite,
    batch_request={
        "datasource_name": "customer_db",
        "data_connector_name": "default_runtime_data_connector",
        "data_asset_name": "customers",
        "runtime_parameters": {"query": "SELECT * FROM validated_bronze.customers"},
    }
)[0]

results = context.run_checkpoint(
    checkpoint_name="customer_quality_checkpoint",
    expectation_suite=suite,
    batch_request=batch
)
```

### Step 3: Evaluate Quality Gate

```python
validation_results = results.to_json_dict()
success_rate = (
    validation_results["statistics"]["successful_expectations"]
    / validation_results["statistics"]["evaluated_expectations"]
)

if success_rate >= 0.95:
    # Promote to Silver
    df.write.format("delta").mode("overwrite").save("/delta/silver/customers")
else:
    # Alert and quarantine
    send_alert(f"Quality gate failed: {success_rate:.1%} success rate")
```

### Step 4: Generate Quality Metrics

Quality results include per-column and per-rule metrics:

| Expectation | Result | Unexpected % |
|-------------|--------|--------------|
| `customer_id not null` | ✓ Passed | 0% |
| `email not null` | ✓ Passed | 0% |
| `age between 0 and 120` | ✗ Failed | 2.1% |
| `customer_id unique` | ✗ Failed | 3.4% |
| `status in set` | ✓ Passed | 0% |

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Silver Table** | Records that passed quality gates |
| **Quarantine Table** | Records that failed quality rules, with rule-level failure details |
| **Quality Dashboard** | Metrics per source, per batch, over time |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **Great Expectations** | Expectations framework | Python-native, declarative, built-in data docs |
| **Deequ** (alternative) | Quality on Spark | Scala/Java-native, better for very large Spark workloads |
| **Soda** (alternative) | Data quality monitoring | SQL-based checks, simpler learning curve |
| **Delta Lake** | Silver storage | ACID for quality-gated writes |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-03-data-quality.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_03_quality.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/03-data-quality.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Overall quality score | ≥ 95% | Passed expectations / total expectations |
| Completeness | ≥ 99% | Non-null values / total values (per critical column) |
| Uniqueness | ≥ 99.5% | Unique values / total values (per key column) |
| Range validity | ≥ 98% | Values within expected range / total values |

---

## When to Advance

Move to Phase 4 when:

- [ ] Quality expectations are defined for all critical columns across all sources.
- [ ] Quality checks run automatically on every validation batch.
- [ ] A quality dashboard exists and is reviewed regularly.
- [ ] Alerts fire when quality scores drop below thresholds.
- [ ] The team has a process for resolving quality issues (data fix, source fix, or expectation adjustment).

---

## Common Pitfalls

### 1. Expectation Proliferation

**Problem**: Teams create hundreds of expectations for every column, many of which are irrelevant or always pass. This creates noise and slows validation.

**Fix**: Focus on business-critical expectations. Every expectation should have a documented business justification. Review and prune expectations quarterly.

### 2. Catastrophic Quality Gates

**Problem**: A quality gate blocks ALL data because one record fails one check. A single null email halts the entire pipeline.

**Fix**: Route individual failing records to quarantine, not the entire batch. The quality gate should block the batch only if the failure **rate** exceeds a threshold (e.g., >5% of records fail). This prevents one bad record from stopping all processing.

### 3. Expectations That Don't Evolve

**Problem**: Quality rules written for a CRM system in 2020 flag `status = "churned"` as invalid because "churned" wasn't in the original set. The business added this status in 2021 but no one updated the expectations.

**Fix**: Treat expectations as code — version them in Git, review them in PRs, and involve business stakeholders in expectation changes. Set up periodic reviews.

### 4. Alert Fatigue

**Problem**: A "data quality" Slack channel gets 50 alerts per day. Everyone mutes it.

**Fix**: Tier your alerts. P0 (page on-call): data completeness drops below 80%. P1 (Slack alert): uniqueness drops below 99%. P2 (dashboard only): formatting issues. Only page when human action is urgently needed.

---

## Further Reading

- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Data Quality Dimensions](https://www.dataversity.net/data-quality-dimensions/)
- [DAMA Data Quality Framework](https://www.dama.org/)

---

[:material-arrow-left: Previous: Phase 2](phase-02-schema-validation.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 4](phase-04-standardization.md)
