# Kafka → Spark Structured Streaming → Snowflake

A production-style streaming data pipeline showing how event data can move from **Kafka through Spark Structured Streaming into a Snowflake analytics layer**.

## Architecture

```text
Producers / Applications
          ↓
       Apache Kafka
          ↓
Spark Structured Streaming
  ├─ JSON parsing
  ├─ schema enforcement
  ├─ watermarking
  ├─ event-time windows
  └─ aggregations
          ↓
   Curated event stream
          ↓
      Snowflake
          ↓
   Analytics / BI
```

## Key Engineering Concepts

- Event-time processing instead of processing-time assumptions
- Watermarks for late-arriving events
- Stateful windowed aggregations
- Checkpointing for fault tolerance
- Explicit schemas for streaming payloads
- Dead-letter handling for malformed events
- Micro-batch delivery into an analytical warehouse
- Separation of ingestion, transformation and serving layers

## Example Event

```json
{
  "event_id": "evt-10021",
  "device_id": "device-77",
  "event_type": "machine_status",
  "event_ts": "2026-09-15T10:30:00Z",
  "value": 81.4
}
```

## Repository Structure

```text
kafka-spark-streaming-snowflake/
├── README.md
└── src/
    ├── streaming_job.py
    └── sql/
        └── serving.sql
```

## Run Locally

The transformation code is designed for Spark Structured Streaming and can be connected to a local Kafka cluster or cloud Kafka service by changing the bootstrap configuration.

```bash
spark-submit src/streaming_job.py
```

For a production deployment, the same pattern can run on EMR/Databricks and write to Snowflake through the Snowflake Spark connector.

## What This Demonstrates

**Kafka • Spark Structured Streaming • Event Time • Watermarking • Checkpointing • Snowflake • Streaming Analytics**
