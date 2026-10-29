from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
import json
from pyspark.sql.functions import col

@dp.table(
    name="realtime_project.bronze.transactions",
    comment="Transactions raw stream data ingested by Kafka"
)
def transactions_bronze() -> DataFrame:
    kafka_connection_json = dbutils.secrets.get(scope="realtime_project_scope", key="KAFKA_CONNECTION_DETAILS")
    kafka_config=json.loads(kafka_connection_json)

    bootstrap_servers=kafka_config['KAKFA_BOOSTRAP_SERVERS']
    api_key=kafka_config['KAFKA_API_KEY']
    api_secret=kafka_config['KAFKA_API_SECRET']
    topic=kafka_config['KAFKA_TOPIC_NAME']
    
    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

    sample_stream = (spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_servers)
        .option('subscribe', topic)
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'PLAIN')
        .option('kafka.sasl.jaas.config', jaas_config)
        .option('startingOffsets', 'earliest')
        .load()
    )

    parsed_stream = sample_stream.select(
        col('key').cast('string'),
        col('value').cast('string'),
        col('topic'),
        col('partition'),
        col('offset'),
        col('timestamp'),
        col('timestampType'),
        F.current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_stream

