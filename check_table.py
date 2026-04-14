from google.cloud import bigquery

client = bigquery.Client(project="neogen-ga4-export")

query = """
SELECT
  MIN(date) AS earliest_date,
  MAX(date) AS latest_date,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT date) AS distinct_dates
FROM `neogen-ga4-export.funnelPurchase_table.segment_funnel_kpis_final_v2`
"""

print("Checking table date range...")
df = client.query(query).result().to_dataframe()
print(df.to_string(index=False))

# Check if March data exists
query_march = """
SELECT
  date,
  SUM(overall_sessions) AS total_sessions,
  SUM(overall_users) AS total_users
FROM `neogen-ga4-export.funnelPurchase_table.segment_funnel_kpis_final_v2`
WHERE date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY date
ORDER BY date
"""

print("\n--- March 2026 breakdown ---")
df_march = client.query(query_march).result().to_dataframe()
if df_march.empty:
    print("No March data found in the table.")
else:
    print(df_march.to_string(index=False))
