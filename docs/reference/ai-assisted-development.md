# AI-Assisted Development Guide

This guide describes how to use an AI coding agent to extend, customize, and harden the entity resolution maturity pipeline -- with a zero-tolerance posture toward bugs, errors, and hallucinations. The agent is not a replacement for engineering judgment; it is an accelerator that operates within strict verification boundaries.

---

## Philosophy: Zero-Tolerance by Design

### The Core Principle

Every change the agent makes must be **verifiable by a deterministic check** before it is accepted. The agent may propose, but the verification gate decides. This is not aspirational -- it is enforced through the workflow patterns described in this guide.

### What Zero-Tolerance Means in Practice

| Principle | What It Prohibits | What It Requires |
|-----------|-------------------|------------------|
| **No untested code** | Accepting an agent's code change without running it against test data. | Every change is followed by execution against a known dataset with known expected output. |
| **No assumed correctness** | Trusting the agent's claim that "the pipeline config change works." | The change is applied, the pipeline is run, the output is compared against expected results. |
| **No config drift** | Allowing the running config to diverge from the documented config without detection. | Config changes are version-controlled and validated against the config schema before application. |
| **No silent regression** | A change to Phase 3 breaking Phase 7 without detection. | The full pipeline is re-run on every change that touches shared interfaces (DataFrame schema, config structure, Delta table layout). |
| **No orphaned code** | An agent adding a custom standardization UDF that nothing calls. | Every new function must be reachable from a `run()` path that is exercised by the test suite. |
| **No hallucinated APIs** | An agent calling `spark.sql.functions.magical_matcher()` because it guessed the API exists. | Every external API call the agent writes must be verified against actual library documentation or confirmed by a `help()` call in the runtime. |

### The Verification Ladder

Every change passes through these gates in order:

```
Gate 1: Schema Validation
  └── Does the config parse? Does the code compile? Are all imports resolvable?

Gate 2: Unit Execution
  └── Does the changed phase run() without error on a small (100-record) dataset?

Gate 3: Output Contract
  └── Does the output DataFrame have the expected columns, types, and row count?

Gate 4: Integration
  └── Does the downstream phase consume the output without error?

Gate 5: Business Correctness
  └── Do the match results, quality scores, or golden records match expected values?
```

A change is not accepted until it clears the gate appropriate to its risk level. A config change (risk: low) must clear Gates 1-2. A new phase module (risk: medium) must clear Gates 1-4. A change to the DataFrame contract between phases (risk: high) must clear all 5 gates.

---

## Assistant Capabilities

The AI agent is capable of the following categories of work on this codebase:

### Category A: Configuration Changes (No New Code)

The agent reads existing YAML configs, applies changes, and validates the result.

**Examples:**
- Add a new ingestion source to `pipeline-config.yaml`.
- Add a new data quality rule.
- Change fuzzy matching thresholds or column weights.
- Add a new entity type by copying and modifying `entity-customer.yaml`.
- Change survivorship rules.
- Adjust blocking window size or strategy.

**Risk level**: Low. Config changes are validated by the YAML schema and by running the phase with the new config.

### Category B: Custom Logic in Existing Extension Points

The agent writes new code within well-defined extension points.

**Examples:**
- Write a custom `QualityRule` with a domain-specific Spark Column expression.
- Write a custom standardization UDF (e.g., `standardize_south_african_id_number`).
- Write a custom `ingest_*` function for a proprietary data source.
- Write a custom survivorship strategy (e.g., `strategy: prefer_authoritative_source`).

**Risk level**: Medium. New code must be verified against the phase's output contract.

### Category C: New Phase Modules

The agent writes a new phase module following the `run(spark, df, config, ...) -> DataFrame` contract.

**Examples:**
- A pre-processing phase that validates tax IDs against a government API before standardization.
- A post-matching phase that enriches matched pairs with external risk scores.
- A custom reporting phase that generates an entity resolution quality PDF.

**Risk level**: Medium-High. New phases must be integrated into the orchestrator and verified against downstream consumers.

### Category D: Core Contract Modifications

The agent modifies the DataFrame contract between phases, the config schema, or the Delta table layout.

**Examples:**
- Adding a new metadata column that all phases must propagate.
- Changing the `run()` function signature (e.g., adding a required parameter).
- Modifying the Delta table schema in a way that downstream phases depend on.

**Risk level**: High. Requires full-pipeline regression verification.

### What the Agent Must Not Do

- **Modify production data.** All verification runs against a dev/test Fabric workspace or local Spark instance.
- **Commit directly to main.** All changes go through a feature branch with PR review.
- **Delete or alter audit logs.**
- **Send real PII to external APIs during testing.** Test data is synthetic or anonymized.
- **Modify the verification scripts themselves** without a separate, human-reviewed change.

---

## Prompt Templates

The following templates are designed to produce deterministic, verifiable changes. Each template includes a verification step that must be executed before the change is accepted.

### Template 1: Add a New Data Source

```
I need to add a new data source to the entity resolution pipeline.

Source details:
- Type: [csv / parquet / json / jdbc]
- Path/URL: [absolute path or JDBC URL]
- Table name for Bronze layer: [e.g., customer_erp]
- Source system identifier: [e.g., ERP_SAP]
- Entity type this source contains: [customer / company / product]

Steps:
1. Add a source entry to pipeline-config.yaml under ingestion.sources.
2. If this is a new entity type, create a new entity-[type].yaml config file
   by copying entity-customer.yaml and adapting the schema, matching rules,
   and survivorship rules.
3. Verify: run Phase 1 ingestion and confirm that records are written to
   the Bronze table with the expected metadata columns.
4. Report: record count, schema, and any ingestion errors.
```

### Template 2: Add Data Quality Rules

```
I need to add data quality rules for [entity_type] entities.

Rules to add:
1. Rule name: [name]
   - Description: [human-readable description]
   - Condition: [Spark SQL expression, e.g., "email rlike '^[A-Za-z0-9...$'"]
   - Severity: [error / warning]
   - Category: [completeness / uniqueness / validity / consistency / timeliness]

[Repeat for each rule.]

Steps:
1. Add each rule to the data_quality.rules.[entity_type] section of
   pipeline-config.yaml.
2. If the condition requires a custom Spark expression that cannot be
   expressed in YAML, write it as a QualityRule object in a new function
   build_[entity_type]_rules() following the pattern in
   phase_03_data_quality.py:build_common_rules().
3. Verify: run Phase 3 on a sample of the entity data. Confirm that:
   a. The quality report shows pass/fail counts for each new rule.
   b. The gateway threshold correctly blocks/passes records.
   c. The quality score column reflects the new rules.
4. Report: pass rate per rule, any unexpected failures, and the number
   of records blocked at the quality gate.
```

### Template 3: Add a Custom Standardization Function

```
I need to add a custom standardization function for [field_name].

Field details:
- Source column name: [e.g., tax_id]
- Current format examples: ["123-45-6789", "123456789", "123 45 6789"]
- Target format: [e.g., "123456789" - digits only, no separators]
- Entity type this applies to: [customer / company / product]

Steps:
1. Write a function standardize_[field_name](col: str) -> F.Column in a new
   file phase_04_[field_name]_standardization.py, OR add it to the
   appropriate entity_type branch in phase_04_standardization.py:run().
2. The function must:
   a. Accept a column name string and return a Spark Column expression.
   b. Handle null input (return null, not error).
   c. Handle empty string input (return empty string, not error).
   d. Include a docstring with before/after examples.
3. Wire the function into phase_04_standardization.py:run() under the
   correct entity_type branch.
4. Verify: run Phase 4 on a sample of the entity data. Confirm that:
   a. The standardized column exists in the output.
   b. At least 3 example records are transformed correctly.
   c. Null and empty values are handled without errors.
5. Report: before/after examples for 5 records, and any records where
   standardization produced unexpected output.
```

### Template 4: Add a New Phase Between Existing Phases

```
I need to insert a new phase between Phase [N] and Phase [N+1].

New phase details:
- Phase name: [e.g., "Tax ID Validation"]
- Purpose: [one sentence describing what it does]
- Input: DataFrame from Phase [N] with these expected columns: [...]
- Processing logic: [detailed description of what the phase does]
- Output: DataFrame with these added/modified columns: [...]
- Entity types it applies to: [customer / all / etc.]

Steps:
1. Create a new file phase_[N]b_[name].py in examples/src/.
2. Implement the run() function following the standard contract:
   def run(spark, df, entity_type, config, ..., metrics) -> DataFrame
3. The function must:
   a. Accept and pass through all columns from the input DataFrame
      (no column dropping unless explicitly specified).
   b. Add new columns with a distinguishing prefix (e.g., _tax_id_status).
   c. Log record counts before and after processing.
   d. Emit metrics via the MetricsCollector.
4. Add configuration for the new phase to pipeline-config.yaml under
   a new top-level key.
5. Insert the phase call in the orchestrator notebook between
   Phase [N] and Phase [N+1].
6. Verify:
   a. Run the new phase in isolation on a 100-record sample.
   b. Run Phase [N] → New Phase → Phase [N+1] end-to-end on the sample.
   c. Confirm Phase [N+1] output is identical to before (if the new phase
      is additive) or differs only in expected ways.
7. Report: input/output record counts, new columns added, any schema
   changes, and confirmation that Phase [N+1] still functions correctly.
```

### Template 5: Modify Core DataFrame Contract

```
[WARNING: High-risk change. Requires full pipeline regression.]

I need to modify the DataFrame contract between phases.

Change details:
- What column is being added/changed/removed: [column name and type]
- Which phase produces it: [phase number]
- Which phases consume it: [phase numbers]
- Reason: [why this change is necessary]

Steps:
1. Identify every phase that reads or writes this column.
   Search all phase_*.py files for references to the column name.
2. Modify the producing phase to output the new/changed column.
3. Modify every consuming phase to handle the new/changed column.
4. Update pipeline-config.yaml if the column is configurable.
5. Verify:
   a. Run the full pipeline (all 15 phases) on a 10,000-record synthetic
      dataset before and after the change.
   b. Compare the before/after golden records: they should differ only
      in the expected ways.
   c. Run the verification script at tests/verify_pipeline_output.py
      (create this if it does not exist) that validates:
      - All expected columns are present.
      - Column types are correct.
      - Row counts are within expected ranges.
      - No nulls in non-nullable columns.
6. Report: full diff of schema changes, before/after golden record
   comparison for 10 sample entities, and confirmation that all 15
   phases complete without error.
```

### Template 6: LLM Prompt Modification

```
I need to modify the LLM prompt used in Phase 11 for [entity_type] matching.

Current prompt version: [v1.0]
New prompt version: [v1.1]
Change description: [e.g., "Add instruction to consider middle name variations"]

Current prompt:
[paste current prompt template]

New prompt:
[paste new prompt template]

Steps:
1. Add the new prompt as a new version in llm_config.prompt_versions.
2. Set up A/B testing: 20% of pairs use the new prompt, 80% use the current.
3. Run Phase 11 on a test set of [N] known pairs where ground truth is known.
4. Compare accuracy:
   a. Current prompt: [X]% accuracy on test set.
   b. New prompt: [Y]% accuracy on test set.
5. If new prompt accuracy >= current prompt accuracy, promote to 100%.
6. Verify: manually inspect 10 pairs where the prompts disagreed to
   confirm the new prompt's judgment is correct.
7. Report: accuracy comparison, disagreement analysis, and the 10
   manually reviewed cases.
```

---

## Verification Scripts

The following verification scripts should exist in the repository. The agent writes and maintains them as it modifies the codebase.

### `tests/verify_config_schema.py`

Validates that every YAML config file parses and conforms to the expected structure.

```python
"""Validate all YAML config files against expected schema."""
import yaml
import sys
from pathlib import Path

REQUIRED_TOP_KEYS = [
    "pipeline", "environment", "medallion", "ingestion",
    "schema_validation", "data_quality", "standardization",
    "enrichment", "exact_dedup", "fuzzy_matching",
    "record_blocking", "feature_engineering",
    "probabilistic_matching", "llm_semantic",
    "embedding_matching", "golden_record",
    "stewardship", "mdm_distribution",
]

def validate_config(path: str) -> list[str]:
    """Validate a pipeline config file. Returns list of errors (empty = valid)."""
    errors = []
    with open(path) as f:
        config = yaml.safe_load(f)

    for key in REQUIRED_TOP_KEYS:
        if key not in config:
            errors.append(f"Missing required top-level key: {key}")

    # Validate ingestion sources
    if "ingestion" in config and "sources" in config["ingestion"]:
        for i, source in enumerate(config["ingestion"]["sources"]):
            if "type" not in source:
                errors.append(f"ingestion.sources[{i}]: missing 'type'")
            if "table_name" not in source:
                errors.append(f"ingestion.sources[{i}]: missing 'table_name'")
            valid_types = ["csv", "parquet", "json", "jdbc"]
            if source.get("type") not in valid_types:
                errors.append(
                    f"ingestion.sources[{i}]: type '{source.get('type')}' "
                    f"not in {valid_types}"
                )

    # Validate data quality rules
    if "data_quality" in config and "rules" in config["data_quality"]:
        for entity, rules in config["data_quality"]["rules"].items():
            for j, rule in enumerate(rules):
                if "name" not in rule:
                    errors.append(f"data_quality.rules.{entity}[{j}]: missing 'name'")
                if "severity" not in rule:
                    errors.append(f"data_quality.rules.{entity}[{j}]: missing 'severity'")
                valid_severities = ["error", "warning"]
                if rule.get("severity") not in valid_severities:
                    errors.append(
                        f"data_quality.rules.{entity}[{j}]: severity "
                        f"'{rule.get('severity')}' not in {valid_severities}"
                    )

    return errors


if __name__ == "__main__":
    config_dir = Path("examples/config")
    all_errors = []
    for config_file in config_dir.glob("*.yaml"):
        errors = validate_config(str(config_file))
        if errors:
            all_errors.append((config_file.name, errors))

    if all_errors:
        for filename, errors in all_errors:
            print(f"ERRORS in {filename}:")
            for e in errors:
                print(f"  - {e}")
        sys.exit(1)
    else:
        print("All config files valid.")
```

### `tests/verify_phase_output_contract.py`

Validates that a phase's output DataFrame conforms to its documented contract.

```python
"""Verify that each phase produces output matching its documented contract."""
from pyspark.sql import DataFrame

# Contract: (phase_name, required_columns, required_types, min_rows)
PHASE_CONTRACTS = {
    "01-ingestion": {
        "required_columns": ["_ingested_at", "_source_system", "_batch_id"],
        "required_types": {"_ingested_at": "timestamp", "_source_system": "string"},
        "min_rows": 1,
    },
    "03-data-quality": {
        "required_columns": [
            "_quality_score", "_quality_errors",
            "_quality_warnings", "_quality_passed",
        ],
        "required_types": {
            "_quality_score": "double",
            "_quality_passed": "boolean",
        },
        "min_rows": 0,
    },
    "07-fuzzy-matching": {
        "required_columns": [
            "id_left", "id_right", "_aggregate_similarity", "_match_confidence",
        ],
        "required_types": {
            "_aggregate_similarity": "double",
            "_match_confidence": "double",
        },
        "min_rows": 0,
    },
    # ... contracts for all phases
}


def verify_phase_output(phase_name: str, df: DataFrame) -> list[str]:
    """Verify a phase's output against its contract. Returns list of errors."""
    errors = []
    contract = PHASE_CONTRACTS.get(phase_name)
    if not contract:
        return [f"No contract defined for phase: {phase_name}"]

    # Check required columns
    for col in contract["required_columns"]:
        if col not in df.columns:
            errors.append(f"{phase_name}: missing required column '{col}'")

    # Check column types
    for col, expected_type in contract.get("required_types", {}).items():
        if col in df.columns:
            actual_type = df.schema[col].dataType.typeName()
            if actual_type != expected_type:
                errors.append(
                    f"{phase_name}: column '{col}' expected type "
                    f"'{expected_type}', got '{actual_type}'"
                )

    # Check minimum rows
    row_count = df.count()
    min_rows = contract.get("min_rows", 0)
    if row_count < min_rows:
        errors.append(
            f"{phase_name}: expected at least {min_rows} rows, got {row_count}"
        )

    return errors
```

### `tests/verify_end_to_end.py`

Runs the full pipeline on synthetic data and validates the golden records.

```python
"""End-to-end verification: run full pipeline and validate golden records."""
# This script:
# 1. Generates 10,000 synthetic customer records with known duplicates.
# 2. Runs all 15 phases.
# 3. Validates that:
#    a. All duplicate records were correctly merged.
#    b. No non-duplicate records were incorrectly merged.
#    c. Golden records have all required fields.
#    d. Survivorship rules produced the expected field values.
# 4. Reports precision, recall, and F1 against the known ground truth.
```

---

## Verification Checklist Per Change Category

### Category A: Config Change

- [ ] `python -m py_compile` on all modified `.py` files (if any).
- [ ] `python tests/verify_config_schema.py` passes.
- [ ] `mkdocs build --strict` passes (if docs were modified).
- [ ] The affected phase `run()` executes without error on a 100-record sample.
- [ ] The phase's output metrics (record count, pass rate) are within expected ranges.
- [ ] The change is committed with a descriptive message referencing the config key changed.

### Category B: Custom Logic

- [ ] All of Category A checks.
- [ ] `python tests/verify_phase_output_contract.py [phase_name]` passes.
- [ ] The new function has a docstring with before/after examples.
- [ ] The new function handles null, empty, and edge-case inputs without error.
- [ ] The new function is reachable from a `run()` path.
- [ ] The downstream phase consumes the output without schema mismatch.

### Category C: New Phase Module

- [ ] All of Category A and B checks.
- [ ] The new module exposes a `run(spark, df, ...) -> DataFrame` function.
- [ ] The `run()` function logs input/output record counts.
- [ ] The `run()` function emits metrics via `MetricsCollector`.
- [ ] Configuration for the new phase exists in `pipeline-config.yaml`.
- [ ] The phase is integrated into the orchestrator notebook.
- [ ] The pipeline runs end-to-end (new phase + upstream + downstream) on a 1,000-record sample.
- [ ] The golden records at Phase 13 are correct (match ground truth for known duplicates).
- [ ] The new phase's documentation is added to `docs/phases/` and `mkdocs.yml` nav.

### Category D: Core Contract Change

- [ ] All of Category A, B, and C checks.
- [ ] Full pipeline regression on 10,000-record synthetic dataset.
- [ ] Before/after golden record comparison for 100 sample entities.
- [ ] All 15 phases complete without error.
- [ ] All `tests/verify_*.py` scripts pass.
- [ ] Schema documentation is updated.
- [ ] CHANGELOG.md entry describing the contract change.
- [ ] PR reviewed by at least one other engineer.

---

## Anti-Patterns: What the Agent Must Not Do

### 1. Generate Hallucinated APIs

```
WRONG:
from pyspark.sql.functions import magical_deduplicate
df = magical_deduplicate(df, threshold=0.9)
```

The agent must verify every function it calls: `help()` in a Python REPL, reference the PySpark API docs, or search existing code for prior usage. If an API does not exist, the agent must implement it or use an alternative.

### 2. Assume Column Existence

```
WRONG:
df.withColumn("email_std", standardize_email("email"))
# Assumes 'email' column exists. What if the source system has 'email_address'?
```

The agent must check column existence before referencing columns: look at the source config schema, inspect the upstream phase's output contract, or use `df.columns` defensively.

### 3. Drop Columns Silently

```
WRONG:
return df.drop("_quality_score", "_quality_errors", "_quality_warnings", "_quality_passed")
# Downstream phases depend on these columns. Silent drop breaks the pipeline.
```

The agent must never drop metadata columns (`_quality_*`, `_source_*`, `_batch_id`, `_ingested_at`) unless explicitly instructed and after verifying no downstream phase depends on them.

### 4. Change Implicit Behavior

```
WRONG:
# Before: threshold=0.85 meant "aggregate similarity >= 0.85"
# After:  threshold=0.85 means "aggregate similarity > 0.85"
# This changes matching behavior for boundary cases and is invisible to config.
```

The agent must not change comparison operators, default values, or edge-case behavior without explicit instruction and documentation.

### 5. Skip Verification

```
WRONG:
Agent: "I've added the new data quality rule. The config looks correct."
Engineer: "Did you run Phase 3 against test data?"
Agent: "No, but the YAML is valid."
```

Never accept a change that has not cleared the appropriate verification gate for its category.

---

## Workflow: Typical Extension Session

The following is the recommended workflow for using the agent to extend the pipeline:

### Step 1: Define the Change

Start with a clear, structured prompt using the templates above. Specify:
- What to change (config, code, or both).
- The entity type and phase affected.
- The expected behavior before and after.
- The verification steps to run.

### Step 2: Agent Proposes the Change

The agent reads the relevant files (config YAML, phase module, utility modules), makes the change, and reports what it modified.

### Step 3: Verification Gate

The agent runs the verification script appropriate to the change category. If verification fails, the agent diagnoses and fixes the issue, then re-runs verification. This cycle repeats until verification passes or the agent hits a hard limit (configurable: 3 retry attempts).

### Step 4: Human Review

For Category A (config) changes: human review is optional but recommended. The agent's change and verification output serve as the review.

For Category B (custom logic) changes: human review of the new code is required. The agent annotates the diff with explanations of each new function.

For Category C (new phase) and Category D (contract change): human review is mandatory. The agent provides a summary of the change, the verification output, and a diff. A second engineer must approve.

### Step 5: Commit

The agent commits the change (or the human does, depending on team policy) with a conventional commit message referencing the phase, config key, or feature:

```
feat(phase-03): add tax_id_format quality rule for customer entities
fix(config): lower fuzzy matching threshold for company entities from 0.85 to 0.80
feat(phase-07): add custom Jaro-Winkler weighting for Hispanic name patterns
```

### Step 6: Documentation Update

If the change affects configuration options, phase behavior, or the DataFrame contract, the agent updates the corresponding documentation in `docs/` and verifies `mkdocs build --strict` passes.

---

## Agent Configuration

The agent operates with the following constraints defined in the project's configuration:

```yaml
agent:
  boundaries:
    max_retry_attempts: 3
    max_files_per_change: 5
    prohibited_dirs:
      - /prod/
      - /production-data/
      - .git/
    prohibited_operations:
      - delete_phase_module
      - modify_verification_scripts_without_review
      - drop_delta_table
      - send_pii_to_external_api
  verification:
    auto_run_on_change: true
    required_scripts:
      - tests/verify_config_schema.py
    gate_level_by_category:
      A: 2   # Config: parse + unit execution
      B: 3   # Logic: parse + unit + output contract
      C: 4   # New phase: parse + unit + contract + integration
      D: 5   # Contract: parse + unit + contract + integration + business
  documentation:
    auto_update_docs: true
    required_sections:
      - purpose
      - input_columns
      - output_columns
      - configuration_keys
      - example_usage
```

---

## Example Session: Adding a Custom Data Quality Rule

### Human Prompt

```
I need to add data quality rules for customer entities.

Rules to add:
1. Rule name: tax_id_format
   - Description: US Tax ID (EIN) must match XX-XXXXXXX format
   - Condition: tax_id IS NULL OR tax_id RLIKE '^[0-9]{2}-[0-9]{7}$'
   - Severity: error
   - Category: validity

2. Rule name: phone_not_future
   - Description: Phone number must not start with '999' (reserved range)
   - Condition: phone IS NULL OR phone_std NOT RLIKE '^\+1 999'
   - Severity: warning
   - Category: validity

Steps:
1. Add each rule to pipeline-config.yaml under data_quality.rules.customer.
2. Verify: run Phase 3 on a sample of customer data. Confirm that:
   a. The quality report shows pass/fail counts for both new rules.
   b. Records with invalid tax IDs are quarantined.
3. Report: pass rate per rule.
```

### Agent Actions

1. Reads `pipeline-config.yaml` and `phase_03_data_quality.py`.
2. Adds the two rules to `data_quality.rules.customer` in the config.
3. Notes that `tax_id` and `phone_std` columns must exist — verifies this by checking the schema validation config and Phase 4 output contract.
4. Runs `tests/verify_config_schema.py` — passes.
5. Runs Phase 3 on a 1,000-record sample with known tax ID violations — quality report shows 12 failures for `tax_id_format`, 0 for `phone_not_future`.
6. Inspects the 12 failures — all have invalid tax ID formats. No false positives.
7. Inspects 20 passing records — all have valid tax IDs or nulls. No false negatives.
8. Reports: pass rate 98.8% (988/1000), 12 records quarantined, verification complete.

### Human Acceptance

The human reviews the config diff and the verification output. The change is committed.

---

## Example Session: Adding a New Rest API Ingestion Source

### Human Prompt

```
I need to add a private REST API as an ingestion source.

Source details:
- Name: Internal Customer Profile Service
- URL: https://internal-api.company.com/v2/customers
- Auth: OAuth2 client credentials (client ID and secret in Azure Key Vault)
- Pagination: cursor-based, returns 500 records per page
- Response format: JSON array under 'data' key
- Table name for Bronze: customer_profile_service
- Entity type: customer
- Refresh schedule: daily at 03:00 UTC

Steps:
1. Write an ingest_rest_api() function in phase_01_ingestion.py
   following the pattern of ingest_csv()/ingest_jdbc().
2. Add the source entry to pipeline-config.yaml.
3. Verify: run Phase 1, confirm records land in Bronze with correct metadata.
```

### Agent Actions

1. Reads `phase_01_ingestion.py` to understand the existing ingestion function patterns.
2. Writes `ingest_rest_api()` with:
    - OAuth2 token acquisition from Azure Key Vault (using `azure-keyvault-secrets`).
    - Cursor-based pagination with configurable page size.
    - Rate limit handling (respects `Retry-After` header).
    - Timeout and retry logic.
3. Adds `rest_api` to the source type dispatch in `run()`.
4. Adds the source entry to `pipeline-config.yaml`:
   ```yaml
   - type: rest_api
     url: https://internal-api.company.com/v2/customers
     table_name: customer_profile_service
     options:
       auth_type: oauth2_client_credentials
       key_vault_url: https://kv-company.vault.azure.net/
       secret_name: customer-api-credentials
       page_size: 500
       max_pages: 200
       timeout_seconds: 30
   ```
5. Runs `tests/verify_config_schema.py` — fails because `rest_api` is not in the valid types list. Updates the verification script to include `rest_api`. Re-runs — passes.
6. Runs Phase 1 against the actual API (in dev environment) — 15,234 records ingested.
7. Verifies metadata columns are present and populated.
8. Reports: record count, schema, ingestion latency, and any API errors encountered.

### Human Acceptance

Human reviews the new `ingest_rest_api()` function (for correct OAuth flow, error handling, pagination), the config change, and the verification output. Approved and committed.

---

## Guardrails Summary

| Guardrail | Enforcement Mechanism |
|-----------|----------------------|
| No hallucinated APIs | Agent runs `python -c "from pyspark.sql.functions import [name]; help([name])"` before using any PySpark function it hasn't seen in the existing codebase. |
| No broken configs | `tests/verify_config_schema.py` runs automatically after every config change. |
| No broken phase contracts | `tests/verify_phase_output_contract.py` runs after every code change. |
| No regression | Full pipeline re-run on synthetic data for Category C and D changes. |
| No PII sent to external APIs | Agent checks for `redact_pii()` call before any external API invocation. Test data is synthetic. |
| No orphaned code | Agent verifies every new function is reachable from a `run()` path and exercised by a test. |
| No silent behavior changes | Agent compares before/after output for the same input and reports any differences. |
| No unreasonable config values | Agent validates threshold ranges (0.0-1.0), window sizes (> 0), and column references (exist in schema). |

---

## Further Reading

- [Feature Roadmap](feature-roadmap.md) — Planned capabilities that extend the reference scaffolding.
- [Technology Stack](tech-stack.md) — Libraries and tools the agent works with.
- [Contributing Guide](https://github.com/JepStar990/entity-resolution-maturity-journey/blob/main/CONTRIBUTING.md) — Code style and PR process.
- [Use Cases](use-cases.md) — Business domains that inform configuration choices.
