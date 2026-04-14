from google.cloud import bigquery

client = bigquery.Client(project="neogen-ga4-export")

query = """
SELECT
  PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS date,
  device.web_info.hostname,
  COUNT(*) AS event_count
FROM `neogen-ga4-export.analytics_331328809.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260301' AND '20260331'
GROUP BY 1, 2
ORDER BY event_count DESC
LIMIT 20
"""

print("Checking March hostnames...")
df = client.query(query).result().to_dataframe()
print(df.to_string(index=False))
