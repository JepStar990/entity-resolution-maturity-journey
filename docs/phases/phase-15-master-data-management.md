# Phase 15: Master Data Management (MDM) Distribution

## Phase Overview

Distribute trusted golden records to enterprise consumers — CRM, ERP, analytics dashboards, AI/ML applications, and data warehouses. This is the "last mile" of the entity resolution journey: making trusted data available where it's needed, in the format it's needed, with guaranteed freshness and SLAs.

---

## Business Context

### Why This Phase Matters

A perfect golden record that no one can access provides zero business value. MDM distribution turns the output of Phases 1–14 into operational impact:

- **CRM**: Sales team sees the complete customer profile, not fragments from three systems.
- **ERP**: Finance has a single supplier master with correct payment details.
- **Analytics**: Dashboards show accurate customer counts (no double-counting).
- **AI/ML**: Models train on clean, deduplicated, enriched data.
- **Compliance**: Regulators see a single version of truth for KYC/AML.

### Capability Unlocked

Enterprise-wide trusted data consumption with defined contracts, SLAs, and monitoring.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Golden records from Phase 13, validated by Phase 14 stewards |
| **Entities** | Customers, suppliers, products, locations — any mastered domain |
| **Quality** | Governed, stewarded, trusted |

---

## Processing Logic

### Step 1: MDM API Design

```python
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import pyspark.sql.functions as F

app = FastAPI(title="MDM API", version="1.0.0")


@app.get("/api/v1/entities/{entity_type}/{entity_id}")
async def get_entity(
    entity_type: str,
    entity_id: str,
    include_history: bool = False,
    as_of: Optional[str] = None
):
    """
    Retrieve a golden record by entity type and ID.

    - entity_type: 'customer', 'supplier', 'product'
    - entity_id: The golden record ID
    - include_history: Include all source values?
    - as_of: Point-in-time query (ISO 8601 timestamp)
    """
    if entity_type not in ["customer", "supplier", "product"]:
        raise HTTPException(status_code=400, detail="Unknown entity type")

    # Point-in-time query using Delta Lake time travel
    if as_of:
        df = spark.sql(f"""
            SELECT * FROM delta.`/delta/gold/golden_records/{entity_type}`
            TIMESTAMP AS OF '{as_of}'
            WHERE entity_id = '{entity_id}'
        """)
    else:
        df = spark.sql(f"""
            SELECT * FROM delta.`/delta/gold/golden_records/{entity_type}`
            WHERE entity_id = '{entity_id}'
        """)

    result = df.toPandas().to_dict(orient="records")
    if not result:
        raise HTTPException(status_code=404, detail="Entity not found")

    return result[0]


@app.get("/api/v1/entities/{entity_type}/search")
async def search_entities(
    entity_type: str,
    q: str = Query(..., min_length=2),
    limit: int = Query(20, le=100),
    offset: int = 0
):
    """
    Full-text search across golden records.
    """
    # Hybrid: exact match on IDs + full-text on names
    results = spark.sql(f"""
        SELECT *, 
            CASE
                WHEN entity_id = '{q}' THEN 1.0
                WHEN company_name ILIKE '%{q}%' THEN 0.8
                ELSE 0.5
            END as relevance
        FROM delta.`/delta/gold/golden_records/{entity_type}`
        WHERE company_name ILIKE '%{q}%'
           OR entity_id = '{q}'
        ORDER BY relevance DESC
        LIMIT {limit} OFFSET {offset}
    """)

    return results.toPandas().to_dict(orient="records")
```

### Step 2: Event Stream Distribution

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_golden_record_change(entity_type, entity_id, change_type, old_values, new_values):
    """
    Publish CDC event whenever a golden record is created, updated, or merged.
    """
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": f"golden_record_{change_type}",  # created, updated, merged, split
        "entity_type": entity_type,
        "entity_id": entity_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "changes": {
            "old": old_values,
            "new": new_values,
        },
        "source": "mdm_pipeline"
    }

    producer.send(
        f"mdm.{entity_type}.events",
        key=entity_id.encode('utf-8'),
        value=event
    )
```

### Step 3: Data Contracts

```yaml
# mdm_api_config.yml
data_contracts:
  customer_golden:
    freshness_sla: "1 hour"  # Golden records updated within 1 hour of source change
    completeness_sla: "95%"  # 95% of required fields non-null
    availability_sla: "99.9%"  # API uptime
    consumers:
      - system: "CRM"
        fields: [entity_id, name, phone, email, address]
        sync_method: "api_pull"  # CRM pulls from MDM API
        sync_frequency: "15 minutes"
      - system: "ERP"
        fields: [entity_id, name, tax_id, payment_terms]
        sync_method: "event_push"  # MDM pushes changes via Kafka
      - system: "Analytics"
        fields: [all]
        sync_method: "bulk_export"  # Nightly full export to data warehouse
        sync_frequency: "24 hours"
```

### Step 4: SLA Monitoring

```python
def monitor_sla(data_contract):
    """
    Check SLA compliance and alert on violations.
    """
    # Check freshness: when was the last golden record update?
    last_update = spark.sql(f"""
        SELECT MAX(updated_at) as last_update
        FROM delta.`/delta/gold/golden_records/{data_contract['entity']}`
    """).collect()[0]["last_update"]

    hours_since_update = (datetime.now() - last_update).total_seconds() / 3600

    if hours_since_update > 1:  # Freshness SLA breached
        send_alert(
            severity="P1",
            message=f"Golden record freshness SLA breached: "
                    f"{hours_since_update:.1f}h since last update (SLA: 1h)"
        )

    # Check completeness: % of required fields populated
    completeness = spark.sql(f"""
        SELECT
            AVG(CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END) as name_pct,
            AVG(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) as email_pct,
            AVG(CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END) as phone_pct
        FROM delta.`/delta/gold/golden_records/{data_contract['entity']}`
    """).collect()[0]

    if completeness["name_pct"] < 0.95:
        send_alert(severity="P2", message="Name completeness below 95% SLA")
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **REST API** | Golden record CRUD + search endpoints |
| **Event Stream** | CDC events for real-time consumers |
| **Bulk Export** | Nightly full export to data warehouse |
| **SLA Dashboard** | Freshness, completeness, availability per contract |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **FastAPI** | REST API | High-performance, async, auto-generated OpenAPI docs |
| **GraphQL (Strawberry / Graphene)** | Alternative API | Flexible field selection for diverse consumers |
| **Apache Kafka** | Event streaming | Durable, replayable CDC events |
| **Apache Avro / Protobuf** | Data contracts | Schema enforcement on event streams |
| **Delta Lake** | Bulk export source | Time travel for point-in-time exports |
| **Prometheus + Grafana** | SLA monitoring | Metrics collection and alerting |
| **Apache Airflow** | Orchestration | Scheduled bulk exports, SLA checks |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-15-mdm-distribution.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_15_mdm_distribution.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/15-mdm-distribution.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| API availability | 99.9% | Uptime tracking |
| API latency (p99) | < 500ms | Response time at 99th percentile |
| Event delivery latency | < 10 seconds | Time from golden record update to Kafka message |
| Data freshness | < 1 hour | Time since last golden record update |
| Data completeness | ≥ 95% | Non-null values in required fields |

---

## When to Advance

The MDM pipeline is mature when:

- [ ] All planned entity types are mastered and distributed.
- [ ] All downstream consumers are onboarded with data contracts.
- [ ] SLA monitoring is active with automated alerting.
- [ ] The MDM API has > 99.9% availability over a rolling 30-day window.
- [ ] Consumer satisfaction is measured (NPS or similar).

This is the final phase — but the journey doesn't end. MDM is a continuous improvement cycle:

```
Sources → Pipeline → Golden Records → Consumers
                 ↑                        |
                 └── Feedback ── Stewards ─┘
```

---

## Common Pitfalls

### 1. One-Size-Fits-All API

**Problem**: A single REST endpoint returns all 50 fields of the golden record. The CRM needs 5 fields; the data warehouse needs all 50. Everyone gets 50 fields over the wire, wasting bandwidth and exposing fields that consumers shouldn't see.

**Fix**: Field-level access control. Use GraphQL for flexible field selection, or REST with `?fields=name,phone,email`. Define consumer-specific views that expose only the fields each consumer needs and is authorized to see.

### 2. Point-to-Point Integration Sprawl

**Problem**: Every consumer builds a custom integration. CRM pulls via JDBC, ERP subscribes to Kafka, Analytics runs nightly CSV exports. 12 consumers = 12 different integration patterns = 12 failure modes.

**Fix**: The MDM hub provides a unified distribution layer. Consumers integrate with the MDM API/event stream, not directly with the underlying storage. This decouples consumer needs from storage implementation.

### 3. SLA Theater

**Problem**: SLAs are defined ("99.9% availability") but never monitored or enforced. When the API goes down, no one is paged. When data is stale, no one notices.

**Fix**: Implement SLA monitoring as code. Every SLA in the data contract must have a corresponding Prometheus metric, Grafana dashboard panel, and PagerDuty alert rule. SLAs without monitoring are wishes, not commitments.

### 4. The "Build It and They Will Come" Fallacy

**Problem**: The MDM platform is deployed with perfect golden records and a beautiful API. Six months later, no one is using it. Consumers never migrated from their legacy data sources.

**Fix**: MDM adoption is a change management challenge, not just a technology challenge. Onboard consumers one at a time. Show them the value — "Your CRM will have 40% more phone numbers and 25% fewer duplicates." Provide a migration path and deprecation timeline for legacy sources. Celebrate and publicize adoption wins.

---

## Further Reading

- [DAMA Guide to the Data Management Body of Knowledge (DMBOK)](https://www.dama.org/cpages/body-of-knowledge)
- [Building Microservices (Data Distribution Patterns)](https://www.oreilly.com/library/view/building-microservices/9781492034018/)
- [API Design Patterns](https://www.manning.com/books/api-design-patterns)
- [Data Contracts: From Concept to Implementation](https://datacontracts.com/)

---

## :trophy: Congratulations

You've completed the 15-phase Entity Resolution Maturity Journey.

[:material-arrow-left: Previous: Phase 14](phase-14-data-stewardship.md) &nbsp;|&nbsp; [:material-home: Back to Overview](overview.md)
