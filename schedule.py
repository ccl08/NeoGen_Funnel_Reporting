from google.cloud import bigquery_datatransfer
from google.protobuf import struct_pb2

project_id = "neogen-ga4-export"
location   = "us"

with open("main.sql") as f:
    query = f.read()

client = bigquery_datatransfer.DataTransferServiceClient()
parent = f"projects/{project_id}/locations/{location}"

params = struct_pb2.Struct()
params["query"] = query

transfer_config = bigquery_datatransfer.TransferConfig(
    display_name="ATC_Report - Segmentation",
    data_source_id="scheduled_query",
    schedule="every day 06:00",
    params=params,
)

response = client.create_transfer_config(
    parent=parent,
    transfer_config=transfer_config,
)

print(f"Scheduled query created: {response.name}")
print(f"Next run: {response.next_run_time}")
