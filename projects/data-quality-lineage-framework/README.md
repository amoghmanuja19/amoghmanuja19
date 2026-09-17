# Data Quality & Lineage Framework

A lightweight Python framework for making data pipelines **observable, testable and easier to operate**.

## Architecture

```text
Source Tables / Files
        ↓
   Ingestion Layer
        ↓
  Quality Gate
  ├─ schema checks
  ├─ null checks
  ├─ uniqueness
  ├─ accepted values
  └─ freshness
        ↓
 Transformation Layer
        ↓
  Curated Dataset
        ↓
 Lineage + Audit Metrics
```

## Quality Rules

The framework represents checks as configuration rather than hard-coded pipeline logic. A failed quality gate can stop publication of a curated dataset while preserving audit information for investigation.

Example checks:

- Required columns exist
- Primary key is unique
- Required fields are not null
- Numeric values are within expected ranges
- Categorical values belong to an approved set
- Dataset freshness meets an SLA

## Lineage Model

```text
raw.orders
    │
    ├──→ staging.orders_clean
    │          │
    │          └──→ mart.daily_sales
    │
    └──→ audit.orders_quality_results
```

## Repository Structure

```text
data-quality-lineage-framework/
├── README.md
└── src/
    └── quality_checks.py
```

## What This Demonstrates

**Data Quality • Observability • Lineage • SLA Monitoring • Auditability • Python • SQL**
