# Databricks notebook source
secret_scope_name = 'realtime_project_scope'

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

api_url = ctx.apiUrl().getOrElse(None)
api_token = ctx.apiToken().getOrElse(None)

print(api_url)
print(api_token)

import requests
import json

DATABRICKS_INSTANCE = api_url
DATABRICKS_TOKEN = api_token

scope_name = secret_scope_name
backend_type = 'DATABRICKS'

# COMMAND ----------

url = f'{DATABRICKS_INSTANCE}/api/2.0/secrets/put'

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": secret_scope_name,
    "key": secret_key,
    "string_value": secret_value
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully.")
else:
    print("Failed to create secret scope.")
    print("Status code: ", response.status_code)
    print("Response:", response.text)

# COMMAND ----------

KAFKA_BOOTSTRAP_SERVERS='#'
KAKFA_API_KEY='#'
KAFKA_API_SECRET='#'
KAFKA_TOPIC_NAME='#'

KAFKA_CONNECTION_DETAILS = json.dumps({
    "KAKFA_BOOSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
    "KAFKA_TOPIC_NAME": KAFKA_TOPIC_NAME,
    "KAFKA_API_KEY": KAKFA_API_KEY,
    "KAFKA_API_SECRET": KAFKA_API_SECRET
})

# COMMAND ----------

scope = secret_scope_name
secret_key = 'KAFKA_CONNECTION_DETAILS'
secret_value = KAFKA_CONNECTION_DETAILS

url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print(f"Secret {secret_key} created successfully in scope {scope}")
else:
    print("Failed to create secret.")
    print("Status: ", response.status_code)
    print("Response: ", response.text)


# COMMAND ----------

try:
    retrieved_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key='KAFKA_CONNECTION_DETAILS'
    )

    print("Secret retrieved successfully.")

    parsed = json.loads(retrieved_json)
    print("Parsed JSON:")
    print(parsed)

except Exception as e:
    print("Secret verification failed:")
    print(str(e))