"""Kafka to Spark Structured Streaming example.

The job parses machine events using an explicit schema, handles late events
with event-time watermarks, and maintains a five-minute rolling event count.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("device_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("value", DoubleType(), True),
])


def build_stream(spark: SparkSession, bootstrap_servers: str, topic: str):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .filter(F.col("event_id").isNotNull())
    )

    return (
        events.withWatermark("event_ts", "10 minutes")
        .groupBy(
            F.window("event_ts", "5 minutes"),
            F.col("event_type"),
        )
        .agg(
            F.count("event_id").alias("event_count"),
            F.avg("value").alias("avg_value"),
        )
    )


def main():
    spark = SparkSession.builder.appName("KafkaMachineEvents").getOrCreate()
    result = build_stream(spark, "localhost:9092", "machine-events")

    query = (
        result.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "./checkpoints/machine-events")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
