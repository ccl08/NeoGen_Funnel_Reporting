from google.cloud import bigquery_datatransfer
from google.protobuf import struct_pb2, field_mask_pb2

client = bigquery_datatransfer.DataTransferServiceClient()

config_name = "projects/157766755027/locations/us/transferConfigs/69bac718-0000-2848-ad7f-001a114cfa14"

with open("main.sql") as f:
    query = f.read()

params = struct_pb2.Struct()
params["query"] = query

transfer_config = bigquery_datatransfer.TransferConfig(
    name=config_name,
    params=params,
)

response = client.update_transfer_config(
    transfer_config=transfer_config,
    update_mask=field_mask_pb2.FieldMask(paths=["params"]),
)

print(f"Scheduled query updated: {response.name}")
