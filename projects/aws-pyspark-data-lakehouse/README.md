# AWS PySpark Data Lakehouse

A production-style batch data engineering pipeline demonstrating **S3 → PySpark → Parquet → curated analytics** patterns.

## Architecture

```text
Raw CSV / JSON
      ↓
Amazon S3 — Raw Zone
      ↓
PySpark ETL
  ├─ schema validation
  ├─ cleansing
  ├─ deduplication
  ├─ business rules
  └─ partitioning
      ↓
Amazon S3 — Curated Zone (Parquet)
      ↓
Athena / Redshift / BI
```

## Engineering Patterns

- Explicit schemas instead of inference
- Idempotent processing using deterministic record keys
- Null handling and type standardization
- Duplicate removal with window functions
- Partition-aware Parquet output
- Incremental processing by ingestion date
- Audit metrics for input/output/rejected records
- Separation of raw, curated and rejected data

## Repository Structure

```text
aws-pyspark-data-lakehouse/
├── README.md
└── src/
    ├── transform_orders.py
    └── sql/
        └── analytics.sql
```

## Run Locally

```bash
spark-submit src/transform_orders.py --input ./data/orders.csv --output ./output/orders
```

The implementation is cloud-portable: the same transformation pattern can be pointed at `s3://...` paths when executed on EMR or AWS Glue.

## What This Demonstrates

**PySpark • Spark SQL • AWS S3 • Parquet • Incremental ETL • Data Quality • Partitioning • Lakehouse Design**
