import re
from google.cloud import bigquery
from tabulate import tabulate

client = bigquery.Client(project="neogen-ga4-export")

with open("main.sql") as f:
    query = f.read()

# Strip CREATE OR REPLACE TABLE so it runs as a plain SELECT
query = re.sub(r"CREATE OR REPLACE TABLE\s+`[^`]+`\s+AS\s*", "", query, flags=re.IGNORECASE)

print("Running query...")
job = client.query(query)

# Script job: fetch results from the last child job
job.result()
child_jobs = list(client.list_jobs(parent_job=job))

if child_jobs:
    results = child_jobs[-1].result()
else:
    results = job.result()

print("\n--- Preview (first 20 rows) ---\n")
df = results.to_dataframe()

# Drop long URL columns and truncate remaining text columns for readability
df = df.drop(columns=["page_location", "page_title"], errors="ignore")
str_cols = df.select_dtypes(include="object").columns
df[str_cols] = df[str_cols].apply(lambda col: col.str.slice(0, 30))

print(tabulate(df.head(20), headers="keys", tablefmt="rounded_outline", showindex=False))
