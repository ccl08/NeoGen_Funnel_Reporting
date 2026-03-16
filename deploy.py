from google.cloud import bigquery

client = bigquery.Client(project="neogen-ga4-export")

with open("main.sql") as f:
    query = f.read()

print("Deploying table to BigQuery...")
job = client.query(query)
job.result()

print("Done. Table written to: neogen-ga4-export.funnelPurchase_table.segment_funnel_kpis_final")
