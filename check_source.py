from google.cloud import bigquery

client = bigquery.Client(project="neogen-ga4-export")

query = """
SELECT
  MIN(_TABLE_SUFFIX) AS earliest_shard,
  MAX(_TABLE_SUFFIX) AS latest_shard,
  COUNT(DISTINCT _TABLE_SUFFIX) AS total_shards
FROM `neogen-ga4-export.analytics_331328809.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20251201' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
"""

print("Checking GA4 source table shards...")
df = client.query(query).result().to_dataframe()
print(df.to_string(index=False))
