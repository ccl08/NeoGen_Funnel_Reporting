"""Deploy the KPI funnel pipeline as a daily BigQuery scheduled query.

Concatenates 01_session_facts.sql + 02_kpi_funnel_v2.sql into a single
multi-statement script, then upserts the scheduled query by display name.
Idempotent: safe to run repeatedly.

Usage:
    python deploy.py
"""
from pathlib import Path

from google.cloud import bigquery_datatransfer
from google.protobuf import field_mask_pb2, struct_pb2

PROJECT_ID = "neogen-ga4-export"
LOCATION = "us"
SCHEDULE = "every day 06:00"
DISPLAY_NAME = "KPI Funnel V2 - Segment Funnel KPIs Final"

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


def find_config(client, parent: str, display_name: str):
    for cfg in client.list_transfer_configs(parent=parent):
        if cfg.display_name == display_name:
            return cfg
    return None


def main():
    client = bigquery_datatransfer.DataTransferServiceClient()
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"

    params = struct_pb2.Struct()
    params["query"] = build_script()

    existing = find_config(client, parent, DISPLAY_NAME)

    if existing:
        updated = bigquery_datatransfer.TransferConfig(
            name=existing.name,
            display_name=DISPLAY_NAME,
            data_source_id="scheduled_query",
            schedule=SCHEDULE,
            params=params,
        )
        response = client.update_transfer_config(
            transfer_config=updated,
            update_mask=field_mask_pb2.FieldMask(paths=["params", "schedule", "display_name"]),
        )
        print(f"Updated scheduled query: {response.name}")
    else:
        new = bigquery_datatransfer.TransferConfig(
            display_name=DISPLAY_NAME,
            data_source_id="scheduled_query",
            schedule=SCHEDULE,
            params=params,
        )
        response = client.create_transfer_config(parent=parent, transfer_config=new)
        print(f"Created scheduled query: {response.name}")

    print(f"Schedule: {response.schedule}")
    print(f"Next run: {response.next_run_time}")


if __name__ == "__main__":
    main()
