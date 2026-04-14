from google.cloud import bigquery

client = bigquery.Client(project="neogen-ga4-export")

with open("kpi_funnel_v2.sql") as f:
    query = f.read()

print("Deploying kpi_funnel_v2 table to BigQuery...")
print(f"Date filter in query: {query[query.index('_TABLE_SUFFIX BETWEEN'):query.index('AND device')]}")
job = client.query(query)
job.result()

# Check child jobs for errors
child_jobs = list(client.list_jobs(parent_job=job))
for i, child in enumerate(child_jobs):
    print(f"  Child job {i}: state={child.state}, errors={child.errors}")

print(f"\nJob state: {job.state}")
print(f"Total bytes processed: {job.total_bytes_processed}")
print("Done. Table written to: neogen-ga4-export.funnelPurchase_table.segment_funnel_kpis_final_v2")
