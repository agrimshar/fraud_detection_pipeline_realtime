# Databricks notebook source
import json
kafka_connection_json = dbutils.secrets.get(scope="realtime_project_scope", key="KAFKA_CONNECTION_DETAILS")
kafka_config=json.loads(kafka_connection_json)

bootstrap_servers=kafka_config['KAKFA_BOOSTRAP_SERVERS']
api_key=kafka_config['KAFKA_API_KEY']
api_secret=kafka_config['KAFKA_API_SECRET']
topic=kafka_config['KAFKA_TOPIC_NAME']

# COMMAND ----------

jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

# COMMAND ----------

sample_batch = (spark.read.format('kafka')
                .option('kafka.bootstrap.servers', bootstrap_servers)
                .option('subscribe', topic)
                .option('kafka.security.protocol', 'SASL_SSL')
                .option('kafka.sasl.mechanism', 'PLAIN')
                .option('kafka.sasl.jaas.config', jaas_config)
                .option('startingOffsets', 'earliest')
                .load()
)

# COMMAND ----------

sample_batch.count()

# COMMAND ----------

display(sample_batch)

# COMMAND ----------

from pyspark.sql.functions import col
parsed_batch = sample_batch.select(
    col('key').cast('string'),
    col('value').cast('string'),
    col('topic'),
    col('partition'),
    col('offset'),
    col('timestamp'),
    col('timestampType')
)

# COMMAND ----------

display(parsed_batch)

# COMMAND ----------

parsed_batch.write.mode('overwrite').saveAsTable('realtime_project.bronze.transactions_batch_test')

# COMMAND ----------

sample_stream = (spark.readStream.format('kafka')
                .option('kafka.bootstrap.servers', bootstrap_servers)
                .option('subscribe', topic)
                .option('kafka.security.protocol', 'SASL_SSL')
                .option('kafka.sasl.mechanism', 'PLAIN')
                .option('kafka.sasl.jaas.config', jaas_config)
                .option('startingOffsets', 'earliest')
                .load()
)

# COMMAND ----------

from pyspark.sql.functions import col
parsed_stream = sample_stream.select(
    col('key').cast('string'),
    col('value').cast('string'),
    col('topic'),
    col('partition'),
    col('offset'),
    col('timestamp'),
    col('timestampType')
)

# COMMAND ----------

streaming_query = (parsed_stream.writeStream.format('delta')
                    .outputMode('Append')
                    .option('checkpointLocation', '/Volumes/realtime_project/source/transactions/checkpoint/')
                    .trigger(availableNow = True)
                    .toTable('realtime_project.bronze.transactions_streaming_test')
)

print('Query start with query id: ', streaming_query.id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from realtime_project.bronze.transactions_streaming_test