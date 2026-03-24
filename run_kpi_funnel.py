import re
from google.cloud import bigquery
from tabulate import tabulate

client = bigquery.Client(project="neogen-ga4-export")

with open("kpi_funnel.sql") as f:
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

print("\n--- Preview (first row per segment) ---\n")
df = results.to_dataframe()
row = df.head(1).iloc[0]

segments = {
    "Overall":  "overall",
    "PDP":      "pdp",
    "Search":   "search",
    "PCP":      "pcp",
    "Accounts": "accounts",
}

metrics = ["sessions", "atc", "view_cart", "checkout", "purchase"]

table = []
for seg_label, seg_key in segments.items():
    table.append([
        seg_label,
        row.get(f"{seg_key}_sessions", "-"),
        row.get(f"{seg_key}_atc",      "-"),
        row.get(f"{seg_key}_view_cart","-"),
        row.get(f"{seg_key}_checkout", "-"),
        row.get(f"{seg_key}_purchase", "-"),
    ])

print(f"Date: {row['date']}  |  Market: {row['market_id']}  |  Region: {row['priority_region']}\n")
print(tabulate(table, headers=["Segment", "Sessions", "ATC", "View Cart", "Checkout", "Purchase"], tablefmt="rounded_outline"))
