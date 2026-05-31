# Plan: Entity Resolution Maturity Journey Repository

## Context

Create a documentation-first GitHub repository that captures a 15-phase data pipeline maturity model for entity resolution and master data management (MDM). The model progresses from basic data ingestion through to enterprise-grade AI-powered matching and MDM distribution. Target stack: Python + PySpark. Diagrams in Mermaid for native GitHub rendering. All commits must be authored under JepStar990 with no Claude/AI traces.

## Repo Name

`entity-resolution-maturity-journey`

## Repository Structure

```
entity-resolution-maturity-journey/
├── .github/
│   ├── workflows/deploy-docs.yml
│   └── ISSUE_TEMPLATE/
│       ├── phase-proposal.md
│       └── documentation-bug.md
├── docs/
│   ├── index.md
│   ├── architecture.md
│   ├── getting-started.md
│   ├── phases/
│   │   ├── overview.md
│   │   └── phase-01 through phase-15 .md files
│   ├── diagrams/
│   │   ├── master-pipeline.mmd
│   │   ├── data-flow-architecture.mmd
│   │   ├── maturity-curve.mmd
│   │   ├── bronze-silver-gold.mmd
│   │   ├── tech-stack-map.mmd
│   │   └── phase-01 through phase-15 .mmd files
│   ├── reference/
│   │   ├── glossary.md
│   │   ├── tech-stack.md
│   │   ├── bibliography.md
│   │   ├── medallion-architecture.md
│   │   └── decision-log.md
│   ├── assets/images/
│   └── stylesheets/extra.css
├── examples/
│   ├── config/ (9 YAML config stubs)
│   ├── src/ (15 phase modules + utils/)
│   └── notebooks/ (16 Jupyter notebooks)
├── mkdocs.yml
├── requirements-docs.txt
├── README.md
├── CONTRIBUTING.md
├── CODE_OF_CONDUCT.md
├── LICENSE (Apache 2.0)
├── CHANGELOG.md
├── .gitignore
├── .markdownlint.json
├── .pre-commit-config.yaml
└── .editorconfig
```

## Key Design Decisions

1. **MkDocs + Material for MkDocs** — Python-native (pip install), built-in Mermaid support via `pymdownx.superfences`, one-command deploy (`mkdocs gh-deploy`), dark/light mode, excellent search. Zero Node.js/Ruby dependency.

2. **Mermaid diagrams** — Native GitHub + MkDocs rendering. Strategy:
   - 1 master pipeline diagram (all 15 phases, color-coded by medallion layer) → `index.md` + `architecture.md`
   - 4 cross-cutting diagrams: data flow architecture, maturity curve, bronze/silver/gold overlay, tech-to-phase matrix
   - 15 per-phase diagrams showing data states, transforms, decision gates, and storage for each phase
   - Consistent visual language: square boxes = data states, rounded = processing, cylinders = storage, diamonds = decisions

3. **Documentation-first, not code-first** — The `examples/` directory contains annotated, illustrative PySpark snippets (not a runnable app). Each `phase_XX_*.py` exposes `run(spark, df, config) -> df`. Heavily commented, type-hinted, config-driven.

4. **Narrative structure** — Three acts:
   - Act I: Foundation (Phases 1-5) — "Getting Data Under Control"
   - Act II: Matching Mastery (Phases 6-12) — "Finding the Same Entity"
   - Act III: Operationalization (Phases 13-15) — "Making It Real"

5. **Consistent phase doc template** — Each phase file has 12 sections: Overview, Business Context, Input Data State, Processing Logic, Output Data State, Key Technologies, Mermaid Diagram, Example Code Reference, Quality Metrics, When to Advance, Common Pitfalls, Further Reading.

## Implementation Steps

### Step 1: Initialize repo structure
- Create the repo directory
- `git init` with user.name/email confirmed
- Write `.gitignore`, `.editorconfig`, `LICENSE`, `README.md` (stub)
- First commit

### Step 2: Configure documentation tooling
- Write `mkdocs.yml` with full nav, theme, plugins, Mermaid fences
- Write `requirements-docs.txt`
- Write `.markdownlint.json`, `.pre-commit-config.yaml`
- Write `docs/stylesheets/extra.css`

### Step 3: Create cross-cutting diagrams
- `master-pipeline.mmd` — 15 nodes in 3 medallion rows, color-coded
- `data-flow-architecture.mmd` — sources → pipeline → consumers with feedback loops
- `maturity-curve.mmd` — xychart with 3 capability lines
- `bronze-silver-gold.mmd` — block diagram with phase placement
- `tech-stack-map.mmd` — technology-to-phase matrix

### Step 4: Write core docs
- `docs/index.md` — landing page with master diagram, persona cards, quick-nav
- `docs/architecture.md` — system context, medallion architecture, tech decisions
- `docs/getting-started.md` — prerequisites, persona-based reading paths
- `docs/phases/overview.md` — summary table, maturity curve, persona guides

### Step 5: Write all 15 phase docs
- Each follows the 12-section template
- Each includes its phase-specific Mermaid diagram via `--8<--` snippet import
- Each links to corresponding code examples

### Step 6: Create reference section
- `glossary.md`, `tech-stack.md`, `bibliography.md`, `medallion-architecture.md`, `decision-log.md`

### Step 7: Write code scaffolding
- 15 phase modules in `examples/src/`
- 4 utility modules (`spark_session.py`, `delta_helpers.py`, `metrics.py`, `logging_config.py`)
- 9 config stubs in `examples/config/`
- 16 notebooks in `examples/notebooks/`

### Step 8: Write supporting files
- `README.md` (finalized with badges, table, master diagram)
- `CONTRIBUTING.md` (style guide, Mermaid conventions, PR process)
- `CODE_OF_CONDUCT.md`, `CHANGELOG.md`
- `.github/workflows/deploy-docs.yml`
- `.github/ISSUE_TEMPLATE/` files

### Step 9: Deploy
- Create GitHub repo via `gh`
- Push all commits to `main`
- Deploy docs via `mkdocs gh-deploy`

## Git Practices
- All commits: `JepStar990 <zwiswamuridili990@gmail.com>`
- Conventional commits: `docs:`, `feat:`, `fix:`, `chore:`, `ci:`
- No co-author trailers, no Claude/AI mentions anywhere
- Commit granularity: one logical unit per commit (one phase, one diagram set, etc.)

## Verification
- `mkdocs build --strict` succeeds with no warnings
- All Mermaid diagrams render correctly (check on GitHub after push)
- All internal links resolve (MkDocs strict mode catches broken links)
- `README.md` renders correctly on GitHub (check Mermaid, badges, table)
- Code examples have valid Python syntax (`python -m py_compile` on each `.py` file)
- No "Claude", "Anthropic", "Co-Authored-By", or AI-related strings anywhere in the repo
