# Phase 13: Golden Record Creation

## Phase Overview

Merge matched records into a single, trusted "golden record" for each real-world entity. Survivorship rules determine which value wins when matched records disagree — the most recent value, the most frequent value, or the value from the most trusted source.

---

## Business Context

### Why This Phase Matters

Matching (Phases 6–12) tells you **which records belong together**. Golden record creation tells you **what the truth is** when those records disagree.

Consider three matched records for John Smith:

| Field | CRM Record | ERP Record | Support System |
|-------|-----------|------------|----------------|
| Name | John Smith | John A. Smith | J. Smith |
| Phone | +27821234567 | NULL | +27829876543 |
| Address | 123 Main St | 123 Main Street, JHB | NULL |
| Email | john@gmail.com | NULL | john@company.com |

All three are the same person, but each source has different information (and contradictions). The golden record must select the best value for each field.

### Capability Unlocked

A single, trusted entity profile that combines the best information from all source systems.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Entity clusters from Phases 10–12 (matched records grouped by entity) |
| **Clusters** | Groups of 2–N records that represent the same entity |
| **Conflicts** | Disagreeing values across sources for the same field |

---

## Processing Logic

### Step 1: Define Survivorship Rules

```yaml
# survivorship_rules.yml
rules:
  - field: name
    strategy: most_trusted_source
    source_priority: [crm, erp, support_system]

  - field: phone
    strategy: most_recent
    tie_breaker: most_trusted_source

  - field: email
    strategy: most_frequent
    tie_breaker: most_recent

  - field: address
    strategy: longest_non_null  # More detail is usually better
    min_length: 10

  - field: industry
    strategy: most_trusted_source
    source_priority: [erp, crm]

  - field: annual_revenue
    strategy: most_recent
    max_age_days: 365  # Don't use revenue data older than a year
```

### Step 2: Attribute-Level Conflict Resolution

```python
class SurvivorshipEngine:
    def __init__(self, rules_config):
        self.rules = rules_config

    def resolve_field(self, field_name, values_with_metadata):
        """
        values_with_metadata: list of {
            value: str,
            source: str,
            timestamp: datetime,
            data_quality_score: float
        }
        """
        rule = self.rules.get(field_name, {"strategy": "most_recent"})
        strategy = rule["strategy"]

        if strategy == "most_recent":
            return self._resolve_most_recent(values_with_metadata)
        elif strategy == "most_frequent":
            return self._resolve_most_frequent(values_with_metadata)
        elif strategy == "most_trusted_source":
            return self._resolve_most_trusted(
                values_with_metadata,
                rule["source_priority"]
            )
        elif strategy == "longest_non_null":
            return self._resolve_longest(
                values_with_metadata,
                rule.get("min_length", 0)
            )

    def _resolve_most_recent(self, values):
        """Select the most recently updated non-null value."""
        non_null = [v for v in values if v["value"] is not None]
        if not non_null:
            return None
        return max(non_null, key=lambda v: v["timestamp"])["value"]

    def _resolve_most_frequent(self, values):
        """Select the most common value."""
        non_null = [v for v in values if v["value"] is not None]
        if not non_null:
            return None
        from collections import Counter
        counts = Counter(v["value"] for v in non_null)
        most_common = counts.most_common(1)[0][0]
        return most_common

    def _resolve_most_trusted(self, values, source_priority):
        """Select the value from the most trusted source that has data."""
        non_null = [v for v in values if v["value"] is not None]
        if not non_null:
            return None
        # Order by source priority, then by recency
        for source in source_priority:
            source_values = [
                v for v in non_null if v["source"] == source
            ]
            if source_values:
                return max(
                    source_values,
                    key=lambda v: v["timestamp"]
                )["value"]
        # Fallback: most recent from any source
        return max(non_null, key=lambda v: v["timestamp"])["value"]
```

### Step 3: Merge Entity Cluster into Golden Record

```python
def create_golden_record(entity_cluster, engine):
    """
    entity_cluster: list of records belonging to the same entity.
    Returns a single golden record with survivorship decisions.
    """
    golden = {}
    history = {}

    # Get all fields from all records in the cluster
    all_fields = set()
    for record in entity_cluster:
        all_fields.update(record.keys())

    for field in all_fields:
        # Collect all values with metadata
        values_with_metadata = [
            {
                "value": record.get(field),
                "source": record["source_system"],
                "timestamp": record["load_timestamp"],
                "data_quality_score": record.get("data_quality_score", 0)
            }
            for record in entity_cluster
        ]

        # Resolve to a single value
        golden[field] = engine.resolve_field(field, values_with_metadata)

        # Preserve all source values in history
        history[field] = {
            "selected_value": golden[field],
            "all_values": values_with_metadata,
            "strategy_used": engine.rules.get(field, {}).get("strategy", "default")
        }

    return golden, history
```

### Step 4: Write Golden Records and Version History

```python
# Write golden records to Gold layer
golden_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/delta/gold/golden_records")

# Write version history (for audit and rollback)
history_df.write \
    .format("delta") \
    .mode("append") \
    .save("/delta/gold/golden_record_history")
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Golden Records** | One row per real-world entity, with best values selected |
| **Version History** | Every source value preserved with metadata — full audit trail |
| **Survivorship Metadata** | Per field: which strategy was used, which source won |
| **Conflict Flags** | Fields where sources disagreed (for stewardship review) |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark / Delta Lake** | Golden record storage | ACID, time travel for rollback |
| **Survivorship Rule Engine** | Conflict resolution | Config-driven, auditable |
| **Delta Lake Time Travel** | Version history | Query golden records as-of any point in time |
| **GraphFrames** | Entity clustering | Connected components from pairwise matches |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-13-golden-record.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_13_golden_record.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/13-golden-record-creation.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Golden record completeness | ≥ 90% | Fields with non-null values / total fields |
| Survivorship conflict rate | < 10% | Fields where sources disagree / total fields |
| Golden record accuracy | ≥ 95% (spot check) | Manual review of 100 golden records |
| Version history completeness | 100% | Every golden record field must have source traceability |

---

## When to Advance

Move to Phase 14 when:

- [ ] Survivorship rules are documented and agreed upon for all critical fields.
- [ ] Golden records are generated for all entity clusters.
- [ ] Version history is preserved (you can explain where every value came from).
- [ ] Conflict flags identify fields where sources disagree.
- [ ] Business stakeholders have validated a sample of golden records.

---

## Common Pitfalls

### 1. Blind Trust in a Single Source

**Problem**: Survivorship rules always prefer the CRM for everything. But the CRM phone number is notoriously stale; the ERP has more up-to-date contact info.

**Fix**: Define survivorship rules **per field**, not per source. The CRM might be authoritative for names and emails; the ERP for addresses and financial data; the Support system for phone numbers. Trust varies by field.

### 2. Deleting Source Values

**Problem**: After creating the golden record, the original source values are discarded. If the golden record is wrong, there's no way to reconstruct or audit.

**Fix**: Always preserve version history. The golden record is the **current best view**; the version history is the **system of record**. Use Delta Lake time travel to query golden records as they existed at any point in time.

### 3. The "Most Recent" Trap

**Problem**: A CRM record updated yesterday with `phone = NULL` (the field was cleared) overrides an ERP record from last week with `phone = +27821234567` (valid phone). "Most recent" selects NULL.

**Fix**: Add a `max_age_days` rule. Values older than N days are excluded from consideration. Most importantly, apply "most recent" only to **non-null** values. NULLs should never win unless all sources are NULL.

### 4. Golden Record Proliferation Without Governance

**Problem**: Every team creates their own golden records with different survivorship rules. The "golden customer" in Marketing has different attributes than the "golden customer" in Finance.

**Fix**: Golden records are an enterprise asset. Their survivorship rules should be governed centrally (Phase 14/15). Publish the rules, version them, and require sign-off from stakeholders before changes.

---

## Further Reading

- [Survivorship in Master Data Management](https://towardsdatascience.com/survivorship-rules-in-mdm/)
- [Delta Lake Time Travel](https://delta.io/blog/2023-01-18-delta-lake-time-travel/)
- [Conflict Resolution Strategies for Data Integration](https://dl.acm.org/doi/10.1145/1242572.1242582)

---

[:material-arrow-left: Previous: Phase 12](phase-12-embedding-matching.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 14](phase-14-data-stewardship.md)
