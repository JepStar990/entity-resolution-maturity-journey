# Use Cases

This reference catalogs the business domains that entity resolution and master data management (MDM) solve, with concrete scenarios, entity resolution challenges unique to each domain, and a mapping to the relevant phases of the maturity model.

---

## Customer 360 / Customer Data Platform (CDP)

### The Problem

A single customer interacts with your organization through multiple touchpoints -- website, mobile app, in-store POS, call center, loyalty program, social media, and third-party marketplaces. Each system stores a partial, often inconsistent view of that customer. Marketing sends promotions to the same person three times under three variations of their name. Support cannot see purchase history because the CRM and e-commerce platform use different customer IDs. Analytics reports inflate customer counts by 20-40% because duplicates are never resolved.

### Concrete Scenario

| Source System | Name | Email | Phone | Address |
|---------------|------|-------|-------|---------|
| CRM (Sales) | Jonathan M. Smith | jsmith@acme.com | +1-555-0123 | 123 Main St, Springfield, IL 62701 |
| E-Commerce | Jon Smith | jonathan.smith@gmail.com | 555-0123 | 123 Main Street |
| Support Desk | Johnny Smith | -- | (555) 555-0123 | 123 Main, Springfield |
| Loyalty Program | J. Michael Smith | jsmith@acme.com | +1 (555) 0123 | -- |
| In-Store POS | Jon Smtih | -- | 5555550123 | 123 Main St, Springfield IL |

These five records represent **one person**. But without entity resolution, they appear as five distinct customers -- inflating marketing costs, fragmenting the customer experience, and corrupting analytics.

### Entity Resolution Challenges

| Challenge | Description |
|-----------|-------------|
| **Nickname variants** | Jonathan / Jon / Johnny all refer to the same person. String distance alone cannot distinguish nicknames from different people. |
| **Typographical errors** | "Smtih" for "Smith" -- Jaro-Winkler catches this; exact matching does not. |
| **Email multiplicity** | One person uses work email, personal email, and a throwaway. Email alone is neither unique nor stable. |
| **Phone format chaos** | Seven formats for the same number across systems. Without E.164 standardization (Phase 4), phone matching fails. |
| **Address variation** | Street / St, with/without city, ZIP present/absent. Token-based comparison (Jaccard) handles this; exact matching does not. |
| **Name ordering** | "Jonathan M. Smith" vs "J. Michael Smith" -- middle initial vs middle name, different ordering conventions. |

### How the Maturity Model Solves It

| Phase | What It Does for Customer 360 |
|-------|------------------------------|
| **Phase 1-2** | Ingest from CRM, e-commerce, support, loyalty, POS, and marketing platforms. Validate schemas so a missing `customer_id` in one source does not silently corrupt downstream matching. |
| **Phase 3** | Quality-gate each source. Flag records with null names, invalid email formats, or impossible phone numbers before they enter the matching pipeline. |
| **Phase 4** | Standardize: E.164 for phones, Title Case for names, USPS abbreviations for addresses, lowercase for emails. Without this, even exact matching fails. |
| **Phase 5** | Enrich with third-party data: append firmographics for B2B customers, geocoding for addresses, email verification services. |
| **Phase 6** | Exact dedup on composite business keys (email + phone hash). Catches the easy 60-70% of duplicates. |
| **Phase 7** | Fuzzy matching on name, email, phone, and address. Jaro-Winkler for names, Jaccard for addresses. Catches typos and formatting differences. |
| **Phase 8** | Block by ZIP code or email domain to scale pairwise comparisons. Without blocking, 10 million customer records produce 50 trillion candidate pairs. |
| **Phase 9-10** | Train an XGBoost model on human-labeled pairs. Features: name similarity, email similarity, phone similarity, address Jaccard, geocode distance. Produces calibrated match probabilities. |
| **Phase 11** | Resolve the hard cases with an LLM: "Jonathan M. Smith at 123 Main St" vs "J. Michael Smith at 123 Main Street" -- the LLM understands that these are the same person, that "Michael" is the middle name, and that "Street"/"St" is an abbreviation, not a different address. |
| **Phase 12** | Embed customer profiles as vectors. At new-customer-creation time, search for similar existing profiles in sub-second time. Prevents duplicate creation at the point of entry. |
| **Phase 13** | Merge matched records into a golden customer record. Survivorship rules: prefer CRM for name (sales team knows the customer), e-commerce for shipping address (verified by delivery), most recent phone across all sources. |
| **Phase 14** | Data stewards review match pairs with 0.4-0.6 confidence. Their decisions -- "these are the same person" or "these are different people" -- become training data for the next model iteration. |
| **Phase 15** | Distribute the golden customer record to CRM, marketing automation, analytics, and support systems via REST API and event streams. Every system now sees the same customer. |

### Key Metric

**Duplicate rate reduction**: Most organizations reduce duplicate customer records from 15-40% to under 2% by Phase 10, and under 0.5% by Phase 13 with stewardship.

---

## Anti-Money Laundering (AML) & Know Your Customer (KYC)

### The Problem

Financial institutions are required by law to verify customer identities and monitor transactions for suspicious activity. A single individual or shell company may appear across accounts, geographies, and lines of business under different names, addresses, and identifiers. Regulations (BSA/AML in the US, 6AMLD in the EU, FICA in South Africa) impose severe penalties for failure to detect this -- fines in the billions of dollars, personal liability for compliance officers, and loss of banking licenses.

### Concrete Scenario

A corporate entity structured as follows:

| Source | Entity Name | Address | Registration # | Account # |
|--------|-------------|---------|----------------|-----------|
| Corporate Account | Acme Holdings Ltd | 1 Embankment, London EC4A | UK-12345678 | 40091234 |
| Trading Desk A | ACME Holdings Limited | One Embankment, London | GB-1234-5678 | 40098765 |
| Subsidiary Account | Acme Trading (UK) Ltd | 1 Embankment Pl, London | UK-87654321 | 50012345 |
| Offshore Branch | Acme Holdings Ltd (Cayman) | PO Box 456, Grand Cayman | KY-998877 | 70054321 |
| Correspondent Bank | ACME HLDGS | 1 The Embankment, London EC4A 7AB | -- | 900112233 |

These five accounts represent **one corporate group** potentially structuring transactions to avoid reporting thresholds or to obscure beneficial ownership. The entity resolution challenge is harder than consumer CDP: corporate names have no canonical form, registration numbers follow different formats per jurisdiction, and addresses can be deliberately obscured.

### Entity Resolution Challenges

| Challenge | Description |
|-----------|-------------|
| **Legal entity name variation** | Ltd / Limited / Ltd. / Incorporated / Inc / Corp / Corporation -- dozens of legal suffixes across jurisdictions. "Acme Holdings Ltd" and "ACME Holdings Limited" are the same entity; string distance alone struggles with suffix normalization. |
| **Multi-jurisdiction registration numbers** | UK-12345678 vs GB-1234-5678 vs a Cayman Islands registration -- no global ID exists. Cross-referencing requires understanding of per-country identifier formats. |
| **Address deliberate obfuscation** | "1 Embankment" vs "One Embankment" vs "1 The Embankment" vs a PO Box in a secrecy jurisdiction. In AML, address variation may be adversarial, not accidental. |
| **Shell company nesting** | Acme Holdings owns Acme Trading. Are they the same entity? No. But are they part of the same risk exposure? Yes. Entity resolution must distinguish "same entity" from "related entity." |
| **Transliteration and non-Latin scripts** | A Russian entity appearing in Cyrillic in one system and Latin transliteration in another. Multiple transliteration standards exist. |
| **Sanctions list matching** | Must match against OFAC, UN, EU, and UK sanctions lists with strict false-negative penalties. A missed match means facilitating sanctioned transactions. |

### How the Maturity Model Solves It

| Phase | What It Does for AML/KYC |
|-------|--------------------------|
| **Phase 1-2** | Ingest from core banking, trade finance, correspondent banking, and KYC onboarding systems. Schema validation catches the account without a registration number. |
| **Phase 3** | Quality rules: every corporate entity record must have at minimum a name and at least one identifier (reg number, tax ID, or LEI). Records failing this are quarantined for remediation before entering the AML pipeline. |
| **Phase 4** | Standardize legal suffixes (Ltd → Limited, Inc → Incorporated), normalize addresses with postal authority reference data, normalize registration numbers to per-country canonical formats. |
| **Phase 5** | Enrich with LEI (Legal Entity Identifier) from GLEIF, SWIFT BIC codes, and ultimate beneficial ownership (UBO) registries. External reference data is critical for AML -- you cannot resolve corporate entities from internal data alone. |
| **Phase 7** | Fuzzy matching on entity names, with custom weighting: legal suffix differences are weighted near-zero (Ltd vs Limited adds no distance), but root name differences (Acme vs Apex) are weighted heavily. |
| **Phase 8** | Block by country + first three characters of normalized name. Reduces the comparison space for a global bank with 50 million corporate records. |
| **Phase 10** | ML model trained on compliance-team-labeled pairs. Features: name token overlap, reg number edit distance, address geocode proximity, LEI match, industry code similarity. |
| **Phase 11** | LLM reasoning for the hardest cases: "Is Acme Holdings Ltd (UK-12345678) the same entity as ACME Holdings Limited (GB-1234-5678)?" The LLM can parse registration number formats, understand corporate naming conventions, and reason about jurisdiction-specific suffix rules. |
| **Phase 13** | Golden record creation with full audit trail: which source records contributed, which identifiers were consolidated, what the survivorship rules were. Regulators require traceability from golden record back to source. |
| **Phase 14** | Compliance officers (not generic data stewards) review uncertain matches. Their domain expertise -- understanding corporate structures, shell company patterns, and jurisdiction-specific naming -- is irreplaceable. Every review decision is logged immutably for audit. |
| **Phase 15** | Distribute golden entity records to transaction monitoring systems, sanctions screening engines, and regulatory reporting. Real-time API: when a new account is opened, check against the golden entity registry before approval. |

### Key Metric

**False negative rate on sanctions screening**: Below 0.01% -- meaning fewer than 1 in 10,000 sanctioned entities is missed. This is a regulatory requirement, not a business optimization.

---

## Fraud Detection

### The Problem

Fraudsters deliberately create variations of identities to avoid detection. Unlike accidental duplicates (typos, system migrations), fraudulent identity variations are adversarial -- designed to evade exact and fuzzy matching while appearing legitimate to human reviewers.

### Concrete Scenarios

#### Synthetic Identity Fraud

A fraudster constructs an identity from real and fabricated data:

| Account | Name | SSN/SIN | DOB | Address | Phone |
|---------|------|---------|-----|---------|-------|
| Credit Card A | John Williams | 123-45-6789 | 1985-03-15 | 456 Oak Ave, Apt 2B | 555-1001 |
| Personal Loan B | John M. Williams | 123-45-6789 | 03/15/1985 | 456 Oak Avenue #2B | 555-1002 |
| Auto Loan C | J. Williams | 987-65-4321 | 1985-03-15 | 789 Pine Rd | 555-1003 |

Accounts A and B share a social security number -- likely the same person. Account C shares a name pattern and DOB but uses a different SSN and address. This is the classic synthetic identity pattern: a core of real PII (name, DOB) combined with fabricated or borrowed identifiers (SSN, address, phone) to create a "new" identity that builds credit history before "busting out."

#### Claims Fraud

| Claim | Name | Address | Incident Date | Vehicle VIN | Policy # |
|-------|------|---------|---------------|-------------|----------|
| Auto Claim 1 | Robert Chen | 12 Elm St, Unit 3 | 2026-01-15 | 1HGBH41JXMN109186 | POL-001 |
| Auto Claim 2 | Bob Chen | 12 Elm Street, #3 | 2026-01-16 | 1HGBH41JXMN109186 | POL-002 |
| Auto Claim 3 | R. Chen | 12 Elm St, Apt 3 | 2026-01-17 | WDBJF65F9VA123456 | POL-003 |

Same person, same address, two claims on different policies for the same vehicle (POL-001, POL-002) -- likely duplicate claims fraud. Claim 3 uses a different vehicle but the same person and address on a third policy -- possible staged-accident pattern.

### Entity Resolution Challenges

| Challenge | Description |
|-----------|-------------|
| **Adversarial variation** | Unlike accidental duplicates, fraudsters intentionally vary names, addresses, and identifiers. The variation is designed to maximize distance from other records while preserving enough truth to pass verification checks. |
| **Velocity patterns** | Fraudulent identities often appear in bursts -- multiple accounts opened in a short window from the same IP or device. Entity resolution must incorporate temporal and behavioral signals, not just attribute similarity. |
| **Identity compartmentalization** | A fraud ring uses one real SSN across five synthetic identities, each with a different name and address. Resolving the SSN cluster is the key to detection. |
| **Graph-based detection** | Fraud is a network problem. A shares a phone with B, B shares an address with C, C shares a device ID with A. Pairwise matching alone misses this. Connected components (Phase 8, transitive closure) are essential. |
| **False positive cost asymmetry** | A false positive (merging two innocent people) causes customer harm and regulatory action. A false negative (missing a fraud link) causes financial loss. The threshold must balance both, weighted by consequence. |

### How the Maturity Model Solves It

| Phase | What It Does for Fraud Detection |
|-------|----------------------------------|
| **Phase 1-3** | Ingest from account opening, loan origination, claims, and transaction systems. Quality rules flag accounts with mismatched name/SSN combinations or impossible DOBs. |
| **Phase 4** | Standardize all identifiers. E.164 for phones. USPS CASS for addresses including apartment/unit normalization ("Apt 2B" vs "#2B" vs "Unit 2B" -- all the same). This closes the easiest evasion technique. |
| **Phase 6** | Exact dedup on government ID (SSN/SIN). Any two records sharing the same SSN are the same person -- period. This is the anchor that fraud detection builds from. |
| **Phase 7** | Fuzzy matching on name + DOB combinations. Jaro-Winkler for names, with DOB as a hard constraint (different DOB = different person, regardless of name similarity). |
| **Phase 8** | Blocking PLUS connected components. Block by SSN, phone, address, device ID, IP address, and email. Then compute transitive closure across all blocking dimensions. A single shared attribute links two identities into the same fraud graph. |
| **Phase 10** | ML model trained on confirmed fraud cases. Features go beyond attribute similarity: account opening velocity, time-between-applications, IP/device overlap, credit bureau inquiry patterns. The model learns fraud-specific patterns that rule-based systems miss. |
| **Phase 12** | Embedding search for identity graphs. Embed the full identity (all attributes, all linked accounts, all behavioral patterns) and search for similar identity graphs -- not just similar records. |
| **Phase 14** | Fraud analysts review high-risk clusters. Their output (confirmed fraud / false alarm) retrains the model, adapting to new fraud patterns as they emerge. |
| **Phase 15** | Real-time fraud scoring API. At account opening or claim submission, query the golden entity registry and return a risk score within 200ms. |

### Key Metric

**Fraud detection lift**: Entity resolution typically surfaces 15-30% more fraudulent applications than attribute-only rules, because it connects identities that appear unrelated in isolation.

---

## Supplier & Vendor Master Data Management

### The Problem

Procurement and finance organizations manage tens of thousands of suppliers across ERP systems, procurement platforms, contract management tools, and invoice processing systems. The same supplier appears under different names, tax IDs, and remittance addresses. This causes duplicate payments, missed volume discounts, compliance violations, and inflated supplier counts that distort spend analysis.

### Concrete Scenario

| Source | Supplier Name | Tax ID | Address | Bank Account |
|--------|---------------|--------|---------|--------------|
| ERP (SAP) | International Business Machines | 13-0871985 | 1 New Orchard Rd, Armonk, NY 10504 | CHASE-IBM-001 |
| Procurement (Ariba) | IBM Corporation | 13-0871985 | One New Orchard Road, Armonk NY | CHASE-IBM-001 |
| AP Invoices | IBM Corp. | -- | 1 New Orchard Rd, Armonk | CITI-IBM-002 |
| Contracts | IBM Global Services | 13-0871985 | 1 New Orchard Rd, Armonk, NY | -- |

Four records, one supplier. Without MDM, procurement cannot calculate total spend with IBM, AP might pay the same invoice twice (once to CHASE-IBM-001, once to CITI-IBM-002), and contract renewals happen without visibility into existing agreements.

### Entity Resolution Challenges

| Challenge | Description |
|-----------|-------------|
| **Corporate name abbreviation** | International Business Machines / IBM Corporation / IBM Corp. / IBM Global Services. The abbreviation "IBM" bears zero string similarity to "International Business Machines" -- fuzzy matching fails entirely. Requires semantic matching or a known-abbreviations dictionary. |
| **Legal vs trading name** | "International Business Machines" is the legal name; "IBM" is the trading name. Both are correct. Neither is an alias. Entity resolution must link legal and trading names. |
| **Parent-child relationships** | "IBM Global Services" is a division, not a separate legal entity. Tax ID links it to the parent. Entity resolution must model hierarchical relationships, not just "same entity" / "different entity." |
| **Tax ID as anchor** | The US EIN (13-0871985) is a strong, stable identifier. But it is often missing from invoices and contracts. Matching must work without it. |
| **Bank account changes** | A supplier changes their bank. The old bank account must be linked to the supplier record for audit but not used for new payments. SCD Type 2 (Phase 5) tracks this history. |
| **Sanctions and ESG** | Suppliers must be screened against sanctions lists, ESG violations, and forced-labor registries. A supplier appearing under a different name in a sanctions list is a compliance failure. |

### How the Maturity Model Solves It

| Phase | What It Does for Supplier MDM |
|-------|-------------------------------|
| **Phase 4** | Standardize legal suffixes, normalize addresses with postal authority data, normalize tax IDs to per-country formats (EIN in US, VAT in EU, TRN elsewhere). |
| **Phase 5** | Enrich with DUNS numbers, LEI, industry codes (NAICS/SIC), and parent-company hierarchies from third-party providers (D&B, Bureau van Dijk). External enrichment is critical -- internal data alone is incomplete. |
| **Phase 7** | Fuzzy matching on supplier name with custom token weighting. The token "IBM" and the phrase "International Business Machines" have zero string similarity -- Phase 11 (LLM) or a known-abbreviations dictionary must handle this. |
| **Phase 10** | ML model using tax ID match as a near-deterministic signal. Features: name token overlap, tax ID edit distance, address geocode distance, bank account match, industry code match. |
| **Phase 11** | LLM resolves acronyms, abbreviations, and legal-vs-trading-name distinctions. "IBM Corp." is recognized as the same entity as "International Business Machines Corporation" because the LLM knows this from its training data. |
| **Phase 13** | Golden supplier record with survivorship rules: tax ID from ERP (authoritative), bank details from AP (most current), address from procurement (verified by purchase orders), name from contracts (legal accuracy). |
| **Phase 14** | Procurement specialists review uncertain matches. Their domain knowledge -- understanding corporate structures, supplier relationships, and industry conventions -- is essential for the hardest cases. |
| **Phase 15** | Distribute golden supplier records to ERP, procurement, AP, and risk systems. Real-time API for supplier onboarding: when a buyer wants to add a new supplier, check if it already exists. |

### Key Metric

**Duplicate supplier rate**: Organizations typically find 8-15% of their supplier master is duplicates. Each duplicate supplier represents a risk of duplicate payment, missed volume discount, or compliance gap.

---

## Healthcare Patient Matching

### The Problem

A patient visits Hospital A for an emergency, follows up at Clinic B, gets lab work at Lab C, and fills prescriptions at Pharmacy D. Each provider creates a record in their EHR system. Without accurate patient matching, clinicians lack a complete medical history -- allergies, medications, prior diagnoses, and test results are fragmented across systems. The consequence is not wasted marketing spend; it is patient harm.

### Concrete Scenario

| Facility | Patient Name | DOB | MRN | Sex |
|----------|-------------|-----|-----|-----|
| Hospital A (ER) | Maria Elena Gonzales | 1987-03-12 | HOSP-A-55432 | F |
| Clinic B (Follow-up) | Maria Gonzalez | 03/12/1987 | CLIN-B-88765 | F |
| Lab C (Blood Work) | Maria E. Gonzalez-Rodriguez | 1987-03-12 | -- | Female |
| Pharmacy D (Rx) | Maria Gonzales | 3/12/87 | -- | F |
| Hospital A (Readmission) | M. Gonzales | 03/12/1987 | HOSP-A-78219 | F |

Five encounters, one patient. At the readmission, Hospital A creates a new medical record number (MRN) because it does not recognize Maria as the same patient from the prior visit. The ER physician does not see the follow-up notes from Clinic B, the lab results from Lab C, or the current medications from Pharmacy D. The patient is asked redundant questions, duplicate tests are ordered, and -- in the worst case -- a drug interaction is missed.

### Entity Resolution Challenges

| Challenge | Description |
|-----------|-------------|
| **Zero error tolerance** | A false positive (merging two different patients' records) can cause catastrophic harm: wrong blood type, wrong allergies, wrong medications. Healthcare matching operates at 99.9%+ precision requirements, far beyond any other domain. |
| **Hispanic naming conventions** | Maria Elena Gonzales may also use Maria Gonzalez, Maria Gonzalez-Rodriguez (adding spouse's surname), or M. Gonzales. Compound given names, paternal/maternal surnames, and hyphenation create combinatorial name variation. |
| **No universal patient identifier** | The US has no national patient ID. Each facility assigns its own medical record number (MRN). Cross-facility matching relies entirely on demographic attributes. |
| **Demographic data quality** | Patient demographics are collected under stress (emergency, pain, language barriers). Names are misspelled, DOBs are approximated, addresses are outdated. The data is inherently noisy in ways that customer data is not. |
| **Newborn and pediatric matching** | Newborns have no name at birth ("Baby Boy Smith"), share a DOB with siblings (twins, triplets), and change names (formal naming days later). Patient matching for newborns is extraordinarily difficult. |
| **Transgender name changes** | A patient's name and sex marker may change between encounters. The matching system must link records across this change without flagging it as fraud or error. |
| **Privacy regulations** | HIPAA (US), GDPR (EU), POPIA (SA) impose strict constraints on data sharing. Cross-facility matching often requires patient consent or operates within a Health Information Exchange (HIE) framework. |

### How the Maturity Model Solves It

| Phase | What It Does for Patient Matching |
|-------|-----------------------------------|
| **Phase 4** | Standardize to healthcare-specific formats: HL7/FHIR for demographics, LOINC for lab codes, RxNorm for medications, SNOMED CT for diagnoses. Date normalization: all DOBs to ISO 8601 (YYYY-MM-DD) regardless of source format. |
| **Phase 5** | Enrich with MPI (Master Patient Index) data from the HIE or national patient registry. Geocode addresses. Append census tract data for demographic context. |
| **Phase 7** | Fuzzy matching with healthcare-specific algorithms. Jaro-Winkler for names. DOB as a weighted constraint (exact match = strong positive signal, 1-day difference = possible typo, 1-year difference = strong negative signal). Sex as a constraint but not a hard rule (accommodates transgender patients and data entry errors). |
| **Phase 8** | Block by DOB + Soundex of last name. For a regional HIE with 10 million patients, this reduces comparisons from 50 trillion to millions. Blocking strategy must be conservative -- a block that misses a true match is a clinical risk. |
| **Phase 10** | ML model trained on clinically validated match pairs. Features: name token Jaccard, name phonetic similarity (Soundex, NYSIIS), DOB edit distance, address geocode distance, sex match, phone match. The model must be calibrated for high precision (prefer false negatives over false positives). |
| **Phase 11** | LLM for semantic name understanding: "Maria Elena Gonzales" vs "Maria Gonzalez-Rodriguez" -- the LLM understands Hispanic naming conventions (Elena is middle name, Gonzalez is paternal, Rodriguez is maternal/spouse surname) and that these are the same person. This is beyond any string-distance algorithm. |
| **Phase 13** | Golden patient record with **clinical survivorship rules** distinct from business survivorship: for allergies, any record listing an allergy takes precedence (no false negatives on allergies). For medications, most recent across all sources. For diagnoses, aggregate (no information loss). Every field-level merge decision is auditable. |
| **Phase 14** | Clinical data stewards (medical records professionals) review uncertain matches. Their review incorporates clinical judgment -- would merging these records create a patient safety risk? The review queue is prioritized by clinical risk, not by match uncertainty alone. |
| **Phase 15** | Distribute the golden patient record through the HIE. FHIR API for point-of-care access. EMPI (Enterprise Master Patient Index) synchronization across facilities. |

### Key Metric

**Duplicate medical record rate**: Best-practice organizations achieve below 0.5% duplicate rate. The industry average is 8-12%. The cost of a single duplicate is estimated at $1,950 (duplicate tests, redundant procedures, extended length of stay). The cost of a single false-positive merge is incalculable (patient harm).

---

## Product Catalog Unification

### The Problem

Retailers, manufacturers, and marketplaces manage product catalogs spanning millions of SKUs sourced from thousands of suppliers. The same product arrives with different names, descriptions, attributes, and identifiers across purchase orders, supplier catalogs, warehouse systems, e-commerce platforms, and marketplace listings. Without product unification, inventory is fragmented, search results are incomplete, price comparison fails, and procurement cannot negotiate volume discounts.

### Concrete Scenario

| Source | Product Name | SKU / MPN | Price | Category |
|--------|-------------|-----------|-------|----------|
| Supplier A Catalog | iPhone 15 Pro Max 256GB | APPL-IP15PM-256 | $1,199.00 | Smartphones |
| Supplier B Catalog | Apple iPhone 15 Pro Max (256GB) | IP15PM-256GB-BLK | $1,189.00 | Mobile Phones |
| Warehouse WMS | IPHN 15 PRO MAX 256G BLK | WMS-998877 | -- | Electronics |
| E-Commerce (Web) | Apple iPhone 15 Pro Max 256GB - Black Titanium | SKU-554433 | $1,249.99 | Cell Phones |
| Marketplace Listing | iPhone 15 Pro Max | -- | $1,229.00 | Smartphones |

Five records, one physical product. Without product unification: inventory shows five separate SKUs with five stock levels, search for "iPhone 15 Pro Max 256GB" returns partial results, procurement pays five different prices to three suppliers, and analytics cannot answer "how many iPhone 15 Pro Max units did we sell?"

### Entity Resolution Challenges

| Challenge | Description |
|-----------|-------------|
| **No universal product identifier** | UPC/EAN/GTIN exists for many products but not all. Supplier-specific SKUs and internal warehouse codes must be cross-referenced. MPN (Manufacturer Part Number) is the closest to a stable identifier but is often missing or inconsistently formatted. |
| **Abbreviation explosion** | "iPhone 15 Pro Max 256GB Black Titanium" can become "IPHN 15 PRO MAX 256G BLK" in a character-limited warehouse system. The abbreviation compresses 49 characters to 27, obliterating string similarity. |
| **Attribute ordering and optionality** | Some sources include color, some do not. Some list storage before color, some after. Some include carrier information. Matching must be robust to missing and reordered attributes. |
| **Bundle and kit resolution** | Is "iPhone 15 Pro Max + AirPods" a different product from "iPhone 15 Pro Max" sold separately? Yes. But some sources list the bundle under the base product SKU. |
| **Generational disambiguation** | "iPhone 15 Pro Max" vs "iPhone 14 Pro Max" vs "iPhone 15 Pro" -- these are different products. A matching system must be sensitive to the numeric model and tier indicators. |
| **Color and variant matching** | "Black Titanium" vs "BLK" vs "Space Gray" vs "Midnight" -- colors are described with marketing names that change per generation. "Space Gray" in iPhone 13 may be the equivalent of "Black Titanium" in iPhone 15, but they describe different products. |

### How the Maturity Model Solves It

| Phase | What It Does for Product Unification |
|-------|--------------------------------------|
| **Phase 4** | Standardize product attributes into a canonical schema: brand (normalize "Apple" / "APPLE" / "apple"), model (iPhone 15), variant (Pro Max), storage (256 GB), color (Black Titanium). Each attribute is parsed and normalized independently. |
| **Phase 5** | Enrich with GS1 product registry (GTIN lookup), manufacturer specification databases, and product taxonomy services. A GTIN match is deterministic -- same GTIN = same product. |
| **Phase 7** | Fuzzy matching on product name tokens with domain-specific weighting. Model tokens (15, Pro, Max) are weighted heavily -- confusing a 15 for a 14 is a different product. Color tokens are weighted lightly -- "Black" vs "BLK" is a minor variation. |
| **Phase 10** | ML model using structured product attributes as features. Each attribute (brand, model, variant, storage, color, carrier) becomes a feature with a domain-specific similarity function. The model learns that "256GB" matches "256G" but not "128G." |
| **Phase 11** | LLM for semantic product understanding: "Is 'iPhone 15 Pro Max 256GB' the same product as 'IPHN 15 PRO MAX 256G BLK'?" The LLM knows that "IPHN" is an abbreviation for "iPhone," that "256G" equals "256GB," and that "BLK" (Black) is a color variant of the same base product. This is a perfect LLM use case: high semantic content, extreme surface-form variation. |
| **Phase 12** | Embed product titles and descriptions as vectors. Sub-second similarity search at product ingestion time. A new supplier catalog arrives with 50,000 products -- embed each title and search against the existing 2-million-product catalog. Products with cosine similarity > 0.95 are candidates for matching. |
| **Phase 13** | Golden product record: survivorship prefers supplier data for technical specs (MPN, dimensions, weight), marketing data for descriptions and images, and warehouse data for inventory attributes (bin location, handling requirements). |

### Key Metric

**SKU consolidation ratio**: Organizations typically reduce their product master by 10-20% after deduplication. This translates directly to inventory accuracy, search quality, and procurement leverage.

---

## Regulatory Compliance & Audit

### The Problem

Regulations across jurisdictions require organizations to maintain accurate, complete, and auditable records of their critical data entities. GDPR Article 17 (Right to Erasure) requires an organization to delete all records relating to an individual on request. CCAR/DFAST stress testing requires banks to report aggregate exposure across all counterparties. SOX requires accurate financial reporting with traceable data lineage. Failure is not measured in lost revenue but in regulatory fines, consent decrees, and criminal liability.

### Concrete Scenarios

#### GDPR Right to Erasure

A customer submits a deletion request. The organization must find and delete every record for this person across all systems -- CRM, marketing, support, analytics, backups, logs. Without entity resolution, the organization cannot answer the question: "Is there a record for this person anywhere in our systems?" A partial deletion is a GDPR violation.

#### CCAR Counterparty Exposure

A bank must report its total exposure to "Acme Holdings Ltd" -- the sum of all loans, credit lines, derivatives, and securities across all desks and legal entities. If "Acme Holdings Ltd" and "ACME Holdings Limited" are treated as separate counterparties, the bank under-reports its exposure and fails the stress test.

### How the Maturity Model Solves It

| Phase | What It Does for Compliance |
|-------|----------------------------|
| **Phase 13** | Golden records are the system of record for compliance. "This is the complete set of records for this entity, across all source systems." Every merge is versioned with Delta Lake time travel, so the golden record as-of any date can be reconstructed. |
| **Phase 14** | Immutable audit log of every stewardship decision: who reviewed, when, what they decided, and why. This is not optional -- regulators demand it. |
| **Phase 15** | Compliance APIs: GDPR deletion endpoint that accepts a golden entity ID and orchestrates deletion across all source systems. Counterparty exposure endpoint that aggregates by golden entity. |

### Key Metric

**GDPR deletion completeness**: 100% of records for the requesting individual must be deleted within 30 days. Entity resolution is the prerequisite -- you cannot delete what you cannot find.

---

## Insurance Claims

### The Problem

Insurance carriers process millions of claims across lines of business (auto, property, life, health, workers' compensation). The same claimant, provider, vehicle, or property may appear across multiple claims and policies. Without entity resolution, fraud goes undetected, subrogation opportunities are missed, and reserves are set incorrectly due to fragmented claim views.

### Concrete Scenario: Auto Claims

| Claim | Claimant Name | Vehicle VIN | Policy # | Claim Amount | Date |
|-------|--------------|-------------|----------|-------------|------|
| Auto-Claim-001 | David L. Chen | 1HGBH41JXMN109186 | POL-AUTO-1001 | $4,500 | 2026-01-15 |
| Auto-Claim-002 | David Chen | 1HGBH41JXMN109186 | POL-AUTO-1002 | $3,200 | 2026-01-15 |
| Auto-Claim-003 | Dave Chen | 1HGBH41JXMN109186 | POL-RENT-2001 | $800 | 2026-01-16 |

Same person, same vehicle, same day, three policies. Claims 001 and 002 are duplicate claims for the same incident on different auto policies. Claim 003 adds a rental car claim for the same incident. Without entity resolution, all three are paid -- a $8,500 loss where $4,500 was the legitimate exposure.

### How the Maturity Model Solves It

| Phase | What It Does for Insurance |
|-------|---------------------------|
| **Phase 6** | Exact dedup on VIN (vehicle), property address, or SSN. A VIN appearing in two claims on the same date is a near-certain duplicate or fraud indicator. |
| **Phase 8** | Connected components across claim graph: claimant → VIN, claimant → address, VIN → policy, policy → other claimants. A fraud ring of 20 staged accidents involving 40 claimants, 15 vehicles, and 10 body shops emerges from the graph structure. |
| **Phase 10** | ML model predicting claim fraud probability from entity resolution features: number of linked claims, number of linked policies, number of linked claimants, temporal clustering, provider overlap. |
| **Phase 14** | SIU (Special Investigations Unit) investigators review flagged claim clusters. Their determinations feed back into the ML model. |
| **Phase 15** | Real-time claim triage API: at first notice of loss (FNOL), query the entity registry and return a risk score and linked-claims summary to the adjuster within seconds. |

### Key Metric

**Claim linkage rate**: What percentage of claims are linked to the correct claimant, policy, vehicle, and incident. Best practice exceeds 99%. Below 95%, duplicate payments and missed fraud are statistically guaranteed.

---

## Identity Resolution for Marketing & Advertising

### The Problem

A consumer interacts with a brand across devices (phone, tablet, laptop), channels (web, app, email, social), and identities (logged-in user, cookie, device ID, email, phone number). Marketing teams must resolve these fragments into a unified identity graph to suppress existing customers from acquisition campaigns, personalize content, and measure cross-channel attribution.

### Concrete Scenario

| Touchpoint | Identifier | Device | Channel | Action |
|------------|-----------|--------|---------|--------|
| Anonymous web visit | Cookie: abc123 | Desktop Chrome | Web | Browses product pages |
| Email click | jsmith@gmail.com | Desktop Chrome | Email | Clicks promotional email |
| App login | User ID: U-998877 | iPhone 15 | Mobile App | Adds item to cart |
| In-store purchase | Loyalty #: LYL-554433 | -- | POS | Purchases with loyalty card |
| Retargeting ad click | IDFA: 1a2b3c4d | iPhone 15 | Social Media | Clicks retargeting ad |

Five touchpoints, one consumer. Without identity resolution, the marketing team: (a) serves acquisition ads to an existing customer (wasted spend), (b) cannot attribute the in-store purchase to the email campaign (under-reported ROAS), and (c) sends a cart-abandonment email for a product already purchased in-store (customer annoyance).

### Entity Resolution Challenges

| Challenge | Description |
|-----------|-------------|
| **Deterministic vs probabilistic identity** | jsmith@gmail.com on two devices is deterministically the same person. But cookie abc123 and IDFA 1a2b3c4d are probabilistically linked (same household IP, same geolocation, similar browsing patterns). Identity graphs mix deterministic and probabilistic edges. |
| **Household vs individual** | A shared device (family iPad) belongs to a household, not an individual. Identity resolution must distinguish household-level identity from individual-level identity. |
| **Cookie churn and ITP** | Third-party cookies are deprecated. Safari ITP and Firefox ETP delete first-party cookies after 7 days. Identity graphs that depend on cookies are decaying. First-party authenticated identity (email, phone, loyalty number) is the durable alternative. |
| **Scale** | Identity graphs for consumer brands reach hundreds of millions of nodes and billions of edges. The resolution infrastructure must handle this scale. |
| **Consent and privacy** | CCPA opt-out, GDPR consent withdrawal, and "Do Not Sell" requests must propagate across the identity graph. An identity resolved for marketing must be resolvable for deletion. |

### How the Maturity Model Solves It

| Phase | What It Does for Identity Graphs |
|-------|----------------------------------|
| **Phase 1** | Ingest from web (cookies, page views), mobile (IDFA/AAID, app events), email (opens, clicks), CRM (customer records), POS (transactions), and ad platforms (impressions, clicks). |
| **Phase 4** | Standardize all identifiers. Normalize emails (lowercase, strip dots in Gmail addresses). Normalize phones (E.164). Normalize addresses. Without this, deterministic matching on email fails because "John.Smith@gmail.com" and "johnsmith@gmail.com" look like different emails. |
| **Phase 5** | Enrich with third-party identity providers (LiveRamp, Neustar, Epsilon) that provide cross-device identity graphs derived from ad-tech data. |
| **Phase 6** | Deterministic resolution: same email, same phone, same loyalty number → same person. This is the anchor. Every deterministic link is a ground-truth edge in the identity graph. |
| **Phase 8** | Connected components across the identity graph. A shares a device with B, B shares an email with C → A, B, and C are in the same identity cluster. Transitive closure expands the graph from deterministic anchors to probabilistic edges. |
| **Phase 10** | ML model for probabilistic identity: given two identifiers (cookie + device ID), predict whether they belong to the same person. Features: IP overlap, geolocation proximity, temporal patterns, browsing similarity, app usage similarity. |
| **Phase 15** | Identity graph API: resolve(identifier) → identity cluster. 200ms SLA. Used by ad servers (suppress existing customers), personalization engines (unified profile), and attribution systems (cross-channel measurement). |

### Key Metric

**Identity match rate**: What percentage of touchpoints are resolved to a known identity. Best-practice brands achieve 70-90% depending on authentication rates. Each percentage point improvement in match rate translates directly to marketing efficiency.

---

## Cross-Cutting Patterns

### The Maturity Curve Applies Universally

Every use case above follows the same progression: you cannot skip foundation. A bank doing AML cannot start at Phase 10 (ML matching) without Phases 1-5 (ingestion, validation, quality, standardization, enrichment). The ML model will fail on unstandardized phone numbers and unvalidated tax IDs. The pattern is:

```
Foundation (1-5) → Matching (6-12) → Trust (13-15)
```

This is true for customers, patients, suppliers, products, and counterparties alike.

### When to Specialize vs. Generalize

| Scenario | Recommendation |
|----------|---------------|
| Single entity type (customer-only) | Build one pipeline. Specialize deeply. |
| Multiple entity types (customer + supplier + product) | Build a common framework with entity-type-specific configuration. The phases are identical; the matching rules, survivorship rules, and steward workflows differ. |
| Cross-entity resolution (customer linked to supplier) | Rare. Usually indicates a data model problem rather than an entity resolution problem. A person who is both a customer and a supplier is two separate golden records with a relationship link, not a single merged entity. |

### Scaling by Use Case

| Use Case | Typical Record Volume | Matching Latency Requirement | Precision Requirement |
|----------|----------------------|------------------------------|-----------------------|
| Customer 360 (B2C) | 1M - 500M | Batch (hours) or real-time (ms) | 95-99% |
| Customer 360 (B2B) | 100K - 10M | Batch (hours) | 95-99% |
| AML / KYC | 1M - 50M | Batch (hours) with real-time onboarding | 99.9%+ (false negatives) |
| Fraud Detection | 1M - 100M | Real-time (200ms) at decision point | Balanced (cost-weighted) |
| Supplier MDM | 10K - 1M | Batch (hours) | 98-99% |
| Patient Matching | 1M - 100M | Real-time (500ms) at point of care | 99.9%+ (false positive) |
| Product Unification | 100K - 50M | Batch (hours) | 95-99% |
| Insurance Claims | 100K - 10M | Real-time (seconds) at FNOL | 95-99% |
| Identity Graphs | 10M - 500M nodes | Real-time (200ms) ad serving | 90-95% (probabilistic edges) |

---

## Further Reading

- [Phase Overview](../phases/overview.md) -- Full maturity model summary
- [Architecture](../architecture.md) -- System context and design principles
- [Medallion Architecture](medallion-architecture.md) -- Storage layer design
- [Technology Stack](tech-stack.md) -- Tooling decisions
- [Bibliography](bibliography.md) -- Foundational papers per domain
