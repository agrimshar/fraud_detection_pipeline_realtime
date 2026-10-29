from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="realtime_project.gold.high_value_transactions_alert",
    comment="Alert details where a transaction has been performed which is higher than the limit configured by customer"
)
def high_value_transactions_alert() -> DataFrame:
    customers = spark.read.table("realtime_project.silver.customers")
    transactions = spark.readStream.table("realtime_project.silver.transactions")

    joined_df = (transactions.join(customers, transactions.customer_id == customers.customer_id, "left")
        .filter(F.col("amount") > F.col("transaction_limit"))
        .select(
            F.concat_ws("-", F.lit("ALERT"), F.col("transaction_id")).alias("alert_id"),
            F.lit("HIGH VALUE TRANSACTION").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),
            transactions.transaction_id,
            transactions.customer_id,
            customers.email.alias("customer_email"),
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
            transactions.amount.alias("transaction_amount"),
            customers.transaction_limit,
            transactions.currency,
            transactions.merchant_name,
            transactions.merchant_category,
            transactions.transaction_type,
            transactions.payment_channel,
            transactions.city,
            transactions.country,
            transactions.is_international,
            transactions.transaction_timestamp,
            transactions.status
        )
    )
    
    return joined_df

