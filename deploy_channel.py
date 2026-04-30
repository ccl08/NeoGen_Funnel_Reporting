"""Deploy channel_ecomm_Vfinal as a daily BigQuery scheduled query.

Runs 30 minutes after the KPI Funnel V2 schedule so session_facts
has been rebuilt first (channel_ecomm_Vfinal reads from it).
Idempotent: safe to run repeatedly.

Usage:
    python deploy_channel.py
"""
from pathlib import Path

from google.cloud import bigquery_datatransfer
from google.protobuf import field_mask_pb2, struct_pb2

PROJECT_ID = "neogen-ga4-export"
LOCATION = "us"
SCHEDULE = "every day 06:30"
DISPLAY_NAME = "Channel eCom V-Final - Channel-level Funnel"

SQL_FILES = ["channel_ecomm_Vfinal.sql"]


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
