from google.cloud import bigquery

client = bigquery.Client(project="neogen-ga4-export")

with open("kpi_funnel.sql") as f:
    query = f.read()

print("Deploying kpi_funnel table to BigQuery...")
job = client.query(query)
job.result()

print("Done. Table written to: neogen-ga4-export.funnelPurchase_table.kpi_funnel")
