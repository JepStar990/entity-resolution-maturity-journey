# Phase 14: Data Stewardship

## Phase Overview

Enable human review of uncertain matches and golden record conflicts. Data stewards provide the final judgment that algorithms cannot — and their decisions create a feedback loop that continuously improves matching models and survivorship rules.

---

## Business Context

### Why This Phase Matters

No algorithm is 100% accurate. At some threshold, the cost of a wrong automated decision exceeds the cost of human review:

- **Auto-merging incorrect matches** corrupts the golden record and propagates errors to every downstream system.
- **Rejecting true matches** means the organization misses opportunities (fraud detection, customer 360, supply chain optimization).

Data stewards are the **human-in-the-loop** that catches edge cases, resolves ambiguities, and provides the labeled data that makes algorithms better over time.

### Capability Unlocked

A governed, continuously improving entity resolution system where human expertise augments automated matching.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Uncertain matches (80–95% confidence) from Phases 10–12 |
| **Source** | Golden records with field-level conflicts from Phase 13 |
| **Volume** | Typically 1–5% of all candidate pairs (the "hard cases") |

---

## Processing Logic

### Step 1: Prioritize the Review Queue

```python
def calculate_priority(match_pair):
    """
    Higher score = review sooner.
    Factors:
    - Impact: How many downstream systems consume this entity?
    - Confidence: How close to the decision boundary? (closer = more need review)
    - Conflict severity: How many fields disagree?
    - Age: How long has this been waiting?
    """
    impact_score = match_pair.get("downstream_consumers", 1)  # 1-10
    uncertainty_score = 1 - abs(0.5 - match_pair["confidence"]) * 2  # 0 at 50%
    conflict_score = min(match_pair.get("conflict_count", 0) / 10, 1.0)
    age_score = min(match_pair.get("hours_in_queue", 0) / 72, 1.0)

    priority = (
        0.3 * impact_score
        + 0.3 * uncertainty_score
        + 0.2 * conflict_score
        + 0.2 * age_score
    )
    return priority
```

### Step 2: Present to Stewards

```python
def build_steward_view(match_pair):
    """
    Construct a side-by-side comparison for human review.
    Highlights differences and provides context.
    """
    return {
        "pair_id": match_pair["id"],
        "confidence": match_pair["confidence"],
        "match_reason": match_pair.get("top_features", []),

        "entity_a": {
            "name": match_pair["record_a"]["company_name"],
            "address": match_pair["record_a"]["address"],
            "phone": match_pair["record_a"]["phone"],
            "email": match_pair["record_a"]["email"],
            "source": match_pair["record_a"]["source_system"],
        },
        "entity_b": {
            "name": match_pair["record_b"]["company_name"],
            "address": match_pair["record_b"]["address"],
            "phone": match_pair["record_b"]["phone"],
            "email": match_pair["record_b"]["email"],
            "source": match_pair["record_b"]["source_system"],
        },

        "differences": {
            field: {
                "value_a": match_pair["record_a"].get(field),
                "value_b": match_pair["record_b"].get(field),
                "similarity": match_pair["features"].get(f"{field}_similarity")
            }
            for field in ["name", "address", "phone", "email"]
            if match_pair["record_a"].get(field) != match_pair["record_b"].get(field)
        },

        "available_actions": ["approve_match", "reject_match", "split_entity", "flag_for_review"]
    }
```

### Step 3: Record Steward Decisions

```python
def record_decision(steward_id, pair_id, action, notes=None):
    """
    Record steward decision with full context for auditing
    and future model training.
    """
    decision = {
        "steward_id": steward_id,
        "pair_id": pair_id,
        "action": action,
        "notes": notes,
        "timestamp": current_timestamp(),
        "time_to_decision_seconds": calculate_decision_time(pair_id),
    }

    # Write to audit log
    decisions_df = spark.createDataFrame([decision])
    decisions_df.write \
        .format("delta") \
        .mode("append") \
        .save("/delta/audit/steward_decisions")

    # If action is "approve_match" or "reject_match",
    # this becomes a labeled training example for Phase 10
    if action in ["approve_match", "reject_match"]:
        label = 1 if action == "approve_match" else 0
        write_training_label(pair_id, label, steward_id)
```

### Step 4: Feedback Loop for Model Retraining

```python
def trigger_retraining_check():
    """
    Check if enough new labels have accumulated to warrant retraining.
    """
    new_labels_count = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM delta.`/delta/audit/steward_decisions`
        WHERE timestamp > current_timestamp() - INTERVAL 7 DAYS
        AND action IN ('approve_match', 'reject_match')
    """).collect()[0]["cnt"]

    if new_labels_count >= 500:  # Threshold: enough new data
        # Trigger Phase 10 model retraining
        trigger_mlflow_retraining_job(new_labels_since=datetime.now() - timedelta(days=7))

    # Also analyze patterns in steward overrides
    overrides = spark.sql("""
        SELECT field_name, COUNT(*) as override_count
        FROM delta.`/delta/audit/steward_decisions`
        WHERE action = 'split_entity'
        GROUP BY field_name
        ORDER BY override_count DESC
    """)
    # If stewards consistently override the same survivorship rule,
    # flag it for review (Phase 13 rule update)
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Resolved Matches** | Confirmed or rejected by stewards |
| **Training Labels** | New labeled data for ML model retraining |
| **Audit Log** | Full history: who decided what, when, and why |
| **Rule Feedback** | Survivorship rules flagged for update based on steward patterns |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **Custom Steward UI** | Review interface | Side-by-side comparison, batch actions |
| **Delta Lake** | Audit log | Immutable, time-travelable decision history |
| **MLflow** (built into Fabric) | Model retraining | Trigger retraining when enough new labels accumulate |
| **Apache Kafka** | Review queue | Durable, ordered task distribution to stewards |
| **Fabric Data Pipelines** | Orchestration | Scheduled retraining checks, SLA monitoring |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-14-stewardship.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_14_stewardship.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/14-data-stewardship.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Steward agreement rate | ≥ 90% | Agreement between stewards on same tasks (inter-rater reliability) |
| Time to decision | < 5 minutes per task | Time from assignment to resolution |
| Queue backlog | < 500 tasks | Open review tasks at any time |
| Feedback labels per month | ≥ 500 | New training labels generated monthly |
| Model improvement from feedback | F1 +0.5% per quarter | Pre vs post retraining F1 on held-out test set |

---

## When to Advance

Move to Phase 15 when:

- [ ] A steward review UI exists with side-by-side comparison.
- [ ] Review tasks are prioritized by business impact.
- [ ] Steward decisions are logged with full audit trail.
- [ ] Feedback loop is active: steward decisions flow back to model retraining.
- [ ] Steward agreement rate is measured and acceptable (≥ 90%).
- [ ] SLA for review time is defined and monitored.

---

## Common Pitfalls

### 1. The Steward Bottleneck

**Problem**: Every match below 95% confidence goes to human review. With 1M records and a 5% review rate, that's 50,000 review tasks. A team of 3 stewards at 5 minutes per task takes 1,389 hours.

**Fix**: Tier the review. Only the highest-impact, closest-to-boundary cases go to humans. Use active learning: prioritize cases where the model is most uncertain AND the business impact is highest. Consider crowdsourcing or distributed stewardship for large volumes.

### 2. Steward Fatigue

**Problem**: After reviewing 200 near-identical "John Smith vs John Smith" tasks, the steward starts mindlessly clicking "Approve" without reading. Error rate spikes.

**Fix**: Rotate task types. Mix match review with golden record review and data quality investigation. Batch similar tasks for efficiency but limit batch size to prevent fatigue. Monitor per-steward metrics (time per decision, agreement rate) and flag outliers.

### 3. Steward Decisions Not Feeding Back

**Problem**: Stewards diligently review and label thousands of pairs. The labels are stored but never used to retrain the model. Six months later, the model makes the same mistakes.

**Fix**: Automate the feedback loop. When new labeled pairs cross a threshold (e.g., 500 new labels), automatically trigger a model retraining job. Measure whether the retrained model makes fewer review-queue mistakes. Close the loop.

### 4. Ignoring Steward Expertise

**Problem**: The system treats all stewards equally. But Jane has 10 years of experience in the supply chain domain, while Bob started last week. Jane's decisions should carry more weight.

**Fix**: Weight steward labels by expertise. Track per-steward accuracy on known test cases. Use weighted voting for tasks reviewed by multiple stewards. Route the hardest cases to the most experienced stewards.

---

## Further Reading

- [Human-in-the-Loop Machine Learning](https://www.manning.com/books/human-in-the-loop-machine-learning)
- [Active Learning for Entity Resolution](https://arxiv.org/abs/1806.04989)
- [Inter-Rater Reliability Metrics](https://en.wikipedia.org/wiki/Inter-rater_reliability)

---

[:material-arrow-left: Previous: Phase 13](phase-13-golden-record-creation.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 15](phase-15-master-data-management.md)
