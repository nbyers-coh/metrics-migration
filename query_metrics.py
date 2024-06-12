import base64
import pyarrow as pa
import time
import os
import requests

headers = {"Authorization": "Bearer " + os.getenv("dbt_metrics_graphql_token")}
url = "https://semantic-layer.cloud.cityofhope.getdbt.com/api/graphql"
environment_id = "41"
query_id_request = f"""
mutation {{
  createQuery(
    environmentId: {environment_id}
    metrics: [{{name: "daily_inpatient_admissions"}}]
  ) {{
    queryId  
  }}
}}
"""
print(query_id_request)

gql_query_id_response = requests.post(
    url,
    json={"query": query_id_request},
    headers=headers,
  )

query_id = gql_query_id_response.json()["data"]["createQuery"]["queryId"]

query_result_request = f"""
{{
  query(environmentId: {environment_id}, queryId: "{query_id}") {{
    sql
    status
    error
    arrowResult
  }}
}}
"""

print(query_result_request)

while True:
  gql_response = requests.post(
    url,
    json={"query": query_result_request},
    headers=headers,
  )
  print(gql_response.json())
  if gql_response.json()["data"]["query"]["status"] in ["FAILED", "SUCCESSFUL"]:
    break
  # Set an appropriate interval between polling requests
  time.sleep(1)

def to_arrow_table(byte_string: str) -> pa.Table:
  """Get a raw base64 string and convert to an Arrow Table."""
  with pa.ipc.open_stream(base64.b64decode(byte_string)) as reader:
    return pa.Table.from_batches(reader, reader.schema)

arrow_table = to_arrow_table(gql_response.json()["data"]["query"]["arrowResult"])

# Perform whatever functionality is available, like convert to a pandas table.
print(arrow_table.to_pandas())