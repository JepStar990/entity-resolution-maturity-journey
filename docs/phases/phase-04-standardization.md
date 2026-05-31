# Phase 4: Standardization

## Phase Overview

Transform inconsistent data into a unified, canonical format. Names, phone numbers, addresses, dates, and categorical values are normalized so that semantically identical values become syntactically identical. This phase alone often eliminates 30–50% of apparent duplicates.

---

## Business Context

### Why This Phase Matters

Data from different source systems represents the same real-world entity in different ways:

| Variation | Examples |
|-----------|----------|
| Names | `john smith`, `John Smith`, `JOHN SMITH`, `Smith, John` |
| Phones | `0821234567`, `+27821234567`, `27 82 123 4567`, `(082) 123-4567` |
| Addresses | `123 Main St.`, `123 Main Street`, `123 MAIN STR` |
| Dates | `01/02/2026`, `2026-01-02`, `Jan 2, 2026`, `20260102` |
| Case/Whitespace | `  ACME   INC.  `, `Acme Inc.`, `acme inc` |

If you attempt fuzzy matching on unstandardized data, you waste compute on variations that are trivially resolvable. Standardization is the cheapest, highest-ROI step in the entity resolution pipeline.

### Capability Unlocked

Consistent, predictable data formats across all source systems. Downstream matching engines compare apples to apples.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Silver table from Phase 3 (quality-gated) |
| **Schema** | Enforced and quality-checked |
| **Content** | Valid values in inconsistent formats |
| **Volume** | Passed quality gates |

---

## Processing Logic

### Name Standardization

```python
from pyspark.sql.functions import (
    trim, lower, initcap, regexp_replace, translate
)

def standardize_name(df, col_name):
    return df \
        .withColumn(col_name, trim(col(col_name))) \
        .withColumn(col_name, regexp_replace(col(col_name), r'\s+', ' ')) \
        .withColumn(col_name, initcap(col(col_name)))  # John Smith
```

### Phone Standardization (E.164 Format)

```python
def standardize_phone(df, col_name, default_country="27"):
    # Strip all non-digit characters
    df = df.withColumn("_phone_digits",
        regexp_replace(col(col_name), r'[^\d]', ''))

    # Add country code if missing
    df = df.withColumn(col_name,
        when(col("_phone_digits").startswith("0"),
            concat(lit(f"+{default_country}"),
                   expr("substring(_phone_digits, 2)")))
        .when(~col("_phone_digits").startswith("+"),
            concat(lit("+"), col("_phone_digits")))
        .otherwise(col("_phone_digits"))
    ).drop("_phone_digits")

    return df
```

### Address Standardization

```python
# Common abbreviations → canonical forms
ADDRESS_MAP = {
    r'\bSt\b\.?$': 'Street',
    r'\bStr\b\.?$': 'Street',
    r'\bAve?\b\.?$': 'Avenue',
    r'\bRd\b\.?$': 'Road',
    r'\bBlvd?\b\.?$': 'Boulevard',
    r'\bDr\b\.?$': 'Drive',
    r'\bLn\b\.?$': 'Lane',
    r'\bApt\b\.?': 'Apartment',
    r'\bSte?\b\.?': 'Suite',
    r'\bFlr?\b\.?': 'Floor',
}

def standardize_address(df, col_name):
    for pattern, replacement in ADDRESS_MAP.items():
        df = df.withColumn(col_name,
            regexp_replace(col(col_name), pattern, replacement))
    return df
```

### Unicode Normalization (NFKC)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import unicodedata

@udf(StringType())
def normalize_unicode(text):
    if text is None:
        return None
    return unicodedata.normalize('NFKC', text)

# Normalize: ™ → TM, ½ → 1/2, ﬁ → fi, Ｃ → C (fullwidth)
df = df.withColumn("company_name", normalize_unicode(col("company_name")))
```

### Date Normalization

```python
from pyspark.sql.functions import to_date, date_format

# Parse multiple formats, output ISO 8601
def standardize_date(df, col_name):
    return df \
        .withColumn(col_name,
            coalesce(
                to_date(col(col_name), "yyyy-MM-dd"),
                to_date(col(col_name), "MM/dd/yyyy"),
                to_date(col(col_name), "dd/MM/yyyy"),
                to_date(col(col_name), "yyyyMMdd"),
                to_date(col(col_name), "MMM dd, yyyy"),
            )
        ) \
        .withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd"))
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Storage** | Silver Standardized Delta table |
| **Schema** | Same columns as input, but values are now canonical |
| **Example** | `John Smith`, `+27821234567`, `123 Main Street`, `2026-01-02` |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **PySpark UDFs** | Custom transformations | Complex regex, Unicode normalization |
| **PySpark built-in functions** | Simple transformations | `trim`, `initcap`, `to_date`, `regexp_replace` |
| **unicodedata (Python stdlib)** | Unicode normalization | NFKC/NFKD for consistent character representation |
| **libpostal** (optional) | Address parsing | ML-trained, multi-language address standardization |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-04-standardization.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_04_standardization.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/04-standardization.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Name format compliance | ≥ 98% | Names in Title Case / total names |
| Phone E.164 compliance | ≥ 95% | Phones matching E.164 regex / total phones |
| Date ISO 8601 compliance | ≥ 99% | Dates parseable as yyyy-MM-dd / total dates |
| Null introduction rate | 0% | Standardization should never introduce nulls |

---

## When to Advance

Move to Phase 5 when:

- [ ] Standardization rules are defined for all entity-matching columns (name, phone, address, date).
- [ ] Standardization runs automatically after quality checks.
- [ ] Edge cases are documented (e.g., international phone formats, multi-word last names).
- [ ] Standardization rules are tested with representative data from all source systems.

---

## Common Pitfalls

### 1. Over-Aggressive Normalization

**Problem**: `initcap("mcdonald")` → `Mcdonald`. But it should be `McDonald`. `O'brien` → `O'brien` (correct) but `O'Brien` is also valid.

**Fix**: Name standardization is harder than it looks. Use lookup tables for known exceptions. Don't squash case distinctions that carry semantic meaning (e.g., `iOS` vs `IOS`). When in doubt, be conservative.

### 2. Silent Phone Number Loss

**Problem**: A regex that strips "all non-digit characters" destroys the `+` prefix, making international numbers indistinguishable from local ones. `+27821234567` → `27821234567` (ambiguous: country code 27 or 278?).

**Fix**: Always preserve the `+` prefix for E.164 compliance. The `+` is semantically significant.

### 3. English-Only Standardization

**Problem**: Address standardization rules designed for US/UK addresses (ending with "Street", "Road", etc.) fail silently on addresses from other countries.

**Fix**: Scope standardization to known locales. Use a country column to select the appropriate standardization rules. If the country is unknown, apply only generic transformations (trim, collapse whitespace, Unicode normalization).

### 4. Standardization Without Testing

**Problem**: A standardization rule that handles 95% of cases but silently corrupts 5% is deployed without testing.

**Fix**: Build a test suite with known edge cases. For each standardization rule, define 10–20 input/output pairs. Run these tests in CI on every rule change. The `examples/` directory contains test patterns you can adapt.

---

## Further Reading

- [E.164: The International Public Telecommunication Numbering Plan](https://www.itu.int/rec/T-REC-E.164/)
- [Unicode Normalization Forms](https://unicode.org/reports/tr15/)
- [libpostal: International Address Parsing](https://github.com/openvenues/libpostal)
- [Falsehoods Programmers Believe About Names](https://www.kalzumeus.com/2010/06/17/falsehoods-programmers-believe-about-names/)

---

[:material-arrow-left: Previous: Phase 3](phase-03-data-quality-rules.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 5](phase-05-data-enrichment.md)
