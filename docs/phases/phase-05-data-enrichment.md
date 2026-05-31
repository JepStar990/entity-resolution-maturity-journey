# Phase 5: Data Enrichment

## Phase Overview

Augment standardized records with additional context from reference data sources — postal code lookups, industry classifications, firmographic data, and geocoding. Enriched records contain more attributes, which improves matching accuracy in later phases.

---

## Business Context

### Why This Phase Matters

Standardized records often contain the minimum viable information — an email, a name, a phone number. But for accurate entity resolution, more context is better:

- Two "John Smith" records in the same city might be different people — but if both have `industry = "Software"` and `company_size = "50–200"`, they're more likely the same.
- A record with `postal_code = "2001"` alone tells you little. With `city = "Johannesburg"`, `province = "Gauteng"`, `country = "South Africa"`, you have geographic context for blocking and matching.

Enrichment reduces false positives in matching by giving algorithms more signals to work with.

### Capability Unlocked

Context-rich records that support high-confidence matching. The enrichment data itself may reveal duplicates (two records with the same enriched attributes are more likely to be the same entity).

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Silver Standardized table from Phase 4 |
| **Schema** | Standardized, quality-gated columns |
| **Gaps** | Missing context — postal codes without city names, ticker symbols without industry |

---

## Processing Logic

### Step 1: Define Reference Data Sources

```yaml
# enrichment_sources.yml
reference_sources:
  postal_codes:
    type: delta_table
    path: /delta/reference/postal_codes
    key: postal_code
    columns: [city, province, country, latitude, longitude]
    scd_type: 1  # Overwrite on change

  industry_codes:
    type: delta_table
    path: /delta/reference/industry_codes
    key: sic_code
    columns: [industry_name, sector, sub_sector]
    scd_type: 2  # Track history

  company_info:
    type: api
    endpoint: https://api.companydb.com/v2/companies
    key: company_ticker
    cache_ttl_hours: 24
    columns: [company_name, industry, market_cap, employee_count]
```

### Step 2: Join Reference Data

```python
def enrich_postal_codes(spark, df):
    """Enrich records with postal code reference data."""
    postal_ref = spark.read.format("delta").load("/delta/reference/postal_codes")

    return df.join(
        postal_ref,
        on="postal_code",
        how="left"
    ).withColumn("_postal_enriched",
        col("city").isNotNull() & col("province").isNotNull()
    )


def enrich_industry(spark, df):
    """Add industry classification based on SIC/NAICS codes."""
    industry_ref = spark.read.format("delta").load("/delta/reference/industry_codes")

    return df.join(
        industry_ref,
        on="sic_code",
        how="left"
    )
```

### Step 3: API-Based Enrichment with Caching

```python
def enrich_from_api(spark, df):
    """Call external API for firmographic enrichment."""
    tickers_to_lookup = df \
        .filter(col("company_ticker").isNotNull()) \
        .filter(col("_api_enriched").isNull()) \
        .select("company_ticker").distinct()

    results = []
    for row in tickers_to_lookup.collect():
        cached = cache_lookup(row.company_ticker)
        if cached:
            results.append(cached)
        else:
            response = api_client.get_company(row.company_ticker)
            cache_store(row.company_ticker, response)
            results.append(response)

    enrichment_df = spark.createDataFrame(results)
    return df.join(enrichment_df, on="company_ticker", how="left")
```

### Step 4: Handle Slowly-Changing Dimensions (SCD)

```python
# SCD Type 2: track history when reference data changes
# Each reference record has effective_date and expiration_date
def join_scd_type2(df, ref_df, key_col):
    return df.join(
        ref_df,
        (df[key_col] == ref_df[key_col])
        & (df["load_date"] >= ref_df["effective_date"])
        & (df["load_date"] < ref_df["expiration_date"]),
        how="left"
    )
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Storage** | Enriched Silver Delta table |
| **New Columns** | city, province, country, industry, sector, latitude, longitude, company_name, market_cap |
| **Enrichment Metadata** | `_postal_enriched`, `_industry_enriched`, `_api_enriched` flags |
| **Example** | `John Smith` + postal lookup → `John Smith, Johannesburg, Gauteng, South Africa` |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **Delta Lake** | Reference data storage | Versioned, queryable reference data |
| **Redis / Memcached** | API response cache | Avoid redundant API calls |
| **PySpark join()** | Bulk enrichment | Optimized distributed joins |
| **SCD Type 2** | Historical accuracy | Correct enrichment for point-in-time queries |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-05-enrichment.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_05_enrichment.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/05-data-enrichment.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Postal code match rate | ≥ 90% | Records with valid postal → city mapping |
| Industry code match rate | ≥ 85% | Records with valid SIC → industry mapping |
| API enrichment rate | ≥ 70% | Records enriched via external API |
| Enrichment staleness | < 24 hours | Time since reference data was refreshed |

---

## When to Advance

Move to Phase 6 when:

- [ ] Key reference data sources are identified and integrated.
- [ ] Enrichment runs automatically after standardization.
- [ ] API-based enrichments are cached with appropriate TTLs.
- [ ] SCD strategy is chosen and implemented for each reference source.
- [ ] Enrichment metrics show meaningful coverage improvement.

---

## Common Pitfalls

### 1. Enrichment Without Caching

**Problem**: Every record triggers an API call. A batch of 1 million records makes 1 million API calls. Costs explode, rate limits hit, and batch time goes from minutes to days.

**Fix**: Cache aggressively. For postal codes, industry codes, and other slowly-changing reference data, use Delta Lake tables (batch refresh daily). For APIs, cache responses by key with a TTL appropriate to the data's change frequency.

### 2. Joining on Dirty Keys

**Problem**: The reference table has `postal_code = "2001"` but your data has `postal_code = " 2001 "` (with whitespace). The join silently produces nulls.

**Fix**: Standardize join keys before enrichment. Trim, normalize case, and validate formats. Post-enrichment, measure the match rate and alert if it drops below threshold.

### 3. SCD Mismatch

**Problem**: A company changes its industry classification from "Technology" to "Financial Services." Records enriched before the change show "Technology." Records enriched after show "Financial Services." Matching logic sees different industries and splits the entity.

**Fix**: Choose the right SCD strategy. SCD Type 1 (overwrite) ensures consistent enrichment for all records. SCD Type 2 (track history) preserves point-in-time accuracy. For matching enrichment, Type 1 is usually correct — you want the **current** value for matching, not the historic value.

### 4. Missing Reference Data Governance

**Problem**: A postal code lookup table is managed by an intern in Excel. When the intern leaves, the table goes stale, and enrichment quality degrades silently over months.

**Fix**: Assign ownership for each reference data source. Document refresh frequency, source of truth, and fallback behavior. Set up freshness alerts — if a reference table hasn't been updated in N days, alert the owner.

---

## Further Reading

- [Slowly Changing Dimensions (Kimball)](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Data Enrichment Best Practices](https://www.dataversity.net/data-enrichment-best-practices/)
- [Delta Lake Merge for SCD](https://delta.io/blog/2022-05-19-slowly-changing-data-scd/)

---

[:material-arrow-left: Previous: Phase 4](phase-04-standardization.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 6](phase-06-exact-deduplication.md)
