# Contributing

Thank you for considering contributing to the Entity Resolution Maturity Journey.

---

## How to Contribute

### Reporting Issues

Use the [issue templates](.github/ISSUE_TEMPLATE/) to report:

- **Documentation bugs**: Inaccuracies, broken links, unclear explanations.
- **Phase proposals**: Suggest a new phase or a revision to an existing phase.

### Pull Requests

1. Fork the repository.
2. Create a feature branch: `git checkout -b docs/your-change`.
3. Make your changes following the conventions below.
4. Open a pull request against the `main` branch.

---

## Writing Conventions

### Style

- **Language**: American English.
- **Tone**: Professional but approachable. Target a technical reader with 2+ years of data engineering experience.
- **Headings**: Sentence case ("Business context", not "Business Context").
- **Voice**: Active voice. "The pipeline ingests data" not "Data is ingested by the pipeline."

### Phase Documentation Template

Each phase document should follow this structure:

1. **Phase Overview** — One paragraph summary
2. **Business Context** — Why this matters, what capability it unlocks
3. **Input Data State** — Table describing incoming data
4. **Processing Logic** — Step-by-step with code examples
5. **Output Data State** — Table describing outgoing data
6. **Key Technologies** — Table with technology, role, rationale
7. **Diagram** — Embedded Mermaid diagram
8. **Example Code Reference** — Links to `examples/src/` and `examples/notebooks/`
9. **Quality Metrics** — Table with metric, target, measurement
10. **When to Advance** — Checklist of completion criteria
11. **Common Pitfalls** — 2–4 mistakes with fixes
12. **Further Reading** — Links to papers, docs, blog posts
13. **Navigation** — Previous/Next links

### Mermaid Diagrams

- **File naming**: `phase-NN-short-description.mmd` (e.g., `phase-07-fuzzy-matching.mmd`).
- **Direction**: `flowchart LR` for pipeline flows, `flowchart TD` for process flows.
- **Color coding**:
  - Bronze nodes/layers: `#cd7f32`
  - Silver nodes/layers: `#a8a8a8`
  - Gold nodes/layers: `#ffd700`
  - Source/Input: `#4a90d9`
  - Error/Alert: `#c62828`
  - Success: `#2e7d32`
  - Purple/Metadata: `#6a1b9a`
- **Include diagrams using snippets**: `--8<-- "diagrams/filename.mmd"` (requires `docs/` relative path).

### Code Examples

- **Python style**: [PEP 8](https://peps.python.org/pep-0008/) with 4-space indentation.
- **Type hints**: Required on all function signatures.
- **Docstrings**: Google style.
- **Spark best practices**:
  - Prefer DataFrame API over RDD.
  - Do not use `.collect()` in examples (production-unsafe).
  - Use `.filter()` and `.select()` over SQL strings for readability.
  - Always close Spark sessions or use context managers.

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
docs: add Phase 7 fuzzy matching documentation
feat: add PySpark code example for Phase 10 ML matching
fix: correct Levenshtein threshold in Phase 7 example
chore: update mkdocs-material to 9.5.x
ci: add deploy-docs workflow
```

---

## Testing Your Changes

### Documentation

```bash
# Build the docs (strict mode catches broken links)
mkdocs build --strict

# Serve locally
mkdocs serve
```

### Code Examples

```bash
# Check Python syntax
python -m py_compile examples/src/phase_01_ingestion.py

# Run the example (if Spark is available)
python examples/src/phase_01_ingestion.py
```

---

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). Please read it before participating.
