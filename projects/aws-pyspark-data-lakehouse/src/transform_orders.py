"""Cloud-portable PySpark batch transformation example.

The job reads orders, validates the schema, standardizes values, removes
duplicate order events, derives business metrics and writes partitioned
Parquet output. Input/output can be local paths or s3:// URIs.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_ts", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("updated_at", TimestampType(), True),
])


def transform(spark: SparkSession, input_path: str):
    df = spark.read.option("header", True).schema(ORDER_SCHEMA).csv(input_path)

    cleaned = (
        df.withColumn("status", F.upper(F.trim("status")))
        .withColumn("amount", F.round(F.col("amount"), 2))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("amount") >= 0)
    )

    # Keep the latest version of an order event.
    window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
    deduped = (
        cleaned.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    return (
        deduped.withColumn("order_date", F.to_date("order_ts"))
        .withColumn("order_month", F.date_format("order_ts", "yyyy-MM"))
        .withColumn(
            "net_amount",
            F.when(F.col("status") == "CANCELLED", F.lit(0.0)).otherwise(F.col("amount")),
        )
    )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("OrdersLakehouseETL").getOrCreate()
    result = transform(spark, args.input)

    (result.write.mode("overwrite").partitionBy("order_date").parquet(args.output))
    spark.stop()


if __name__ == "__main__":
    main()
