import re
from google.cloud import bigquery
from tabulate import tabulate

client = bigquery.Client(project="neogen-ga4-export")

with open("kpi_funnel_v2.sql") as f:
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

print("\n--- Preview (aggregated totals) ---\n")
df = results.to_dataframe()

segments = {
    "Overall":   "overall",
    "PDP":       "pdp",
    "Search":    "search",
    "PCP":       "pcp",
    "Accounts":  "accounts",
    "Solutions": "solutions",
}

# Sum across all dimension rows to get totals
session_cols = []
user_cols = []
for seg_key in segments.values():
    session_cols += [f"{seg_key}_sessions", f"{seg_key}_atc_sessions", f"{seg_key}_view_cart_sessions", f"{seg_key}_checkout_sessions", f"{seg_key}_purchase_sessions"]
    user_cols += [f"{seg_key}_users", f"{seg_key}_atc_users", f"{seg_key}_purchase_users"]

totals = df[session_cols + user_cols].sum()

# Session-level funnel
session_table = []
for seg_label, seg_key in segments.items():
    session_table.append([
        seg_label,
        f"{totals[f'{seg_key}_sessions']:,.0f}",
        f"{totals[f'{seg_key}_atc_sessions']:,.0f}",
        f"{totals[f'{seg_key}_view_cart_sessions']:,.0f}",
        f"{totals[f'{seg_key}_checkout_sessions']:,.0f}",
        f"{totals[f'{seg_key}_purchase_sessions']:,.0f}",
    ])

print("SESSION-LEVEL FUNNEL")
print(tabulate(session_table, headers=["Segment", "Sessions", "ATC", "View Cart", "Checkout", "Purchase"], tablefmt="rounded_outline"))

# User-level funnel
user_table = []
for seg_label, seg_key in segments.items():
    user_table.append([
        seg_label,
        f"{totals[f'{seg_key}_users']:,.0f}",
        f"{totals[f'{seg_key}_atc_users']:,.0f}",
        f"{totals[f'{seg_key}_purchase_users']:,.0f}",
    ])

print("\nUSER-LEVEL FUNNEL")
print(tabulate(user_table, headers=["Segment", "Users", "ATC Users", "Purchase Users"], tablefmt="rounded_outline"))
