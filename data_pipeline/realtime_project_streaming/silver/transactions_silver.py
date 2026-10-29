from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
import json
from pyspark.sql.functions import col
from pyspark.sql.types import *

@dp.table(
    name="realtime_project.silver.transactions",
    comment="Parsed and cleaned transactions data"
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table("realtime_project.bronze.transactions")

    schema = F.StructType([
        StructField("transaction_id", F.StringType()),
        StructField("customer_id", F.StringType()),
        StructField("card_number", F.StringType()),
        StructField("merchant_id", F.StringType()),
        StructField("merchant_name", F.StringType()),
        StructField("merchant_category", F.StringType()),
        StructField("amount", F.StringType()),
        StructField("currency", F.StringType()),
        StructField("transaction_type", F.StringType()),
        StructField("payment_channel", F.StringType()),
        StructField("device_id", F.StringType()),
        StructField("city", F.StringType()),
        StructField("country", F.StringType()),
        StructField("transaction_timestamp", TimestampType()),
        StructField("is_international", F.StringType()),
        StructField("status", F.StringType())
    ])

    transformed_df = bronze_df.select(
        F.from_json(col("value"), schema).alias("data"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        F.col("data.*"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return transformed_df
