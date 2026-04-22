"""Run the KPI funnel pipeline ad-hoc against BigQuery.

Executes 01_session_facts.sql then 02_kpi_funnel_v2.sql as a single
multi-statement script. Same script the daily schedule runs.

Usage:
    python run.py
"""
from pathlib import Path

from google.cloud import bigquery

PROJECT_ID = "neogen-ga4-export"
SQL_FILES = ["01_session_facts.sql", "02_kpi_funnel_v2.sql"]


def build_script() -> str:
    here = Path(__file__).parent
    parts = []
    for fname in SQL_FILES:
        sql = (here / fname).read_text().strip()
        if not sql.endswith(";"):
            sql += ";"
        parts.append(f"-- === {fname} ===\n{sql}")
    return "\n\n".join(parts)


def main():
    client = bigquery.Client(project=PROJECT_ID)
    script = build_script()

    print(f"Submitting multi-statement script ({len(SQL_FILES)} files)...")
    job = client.query(script)
    job.result()

    print(f"Done. Job id: {job.job_id}")
    for child in client.list_jobs(parent_job=job):
        stmt = (child.query or "").strip().split("\n", 1)[0][:80]
        print(f"  - {child.job_id}  {stmt}")


if __name__ == "__main__":
    main()
