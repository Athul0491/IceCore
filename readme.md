## Running the Go Server

### Prerequisites
- Go 1.23+
- Docker Desktop
- `grpcurl`

### Start PostgreSQL
```bash
docker compose up -d postgres
```
### Run the server (powershell command)
```bash
$env:PG_CONN_STRING="host=127.0.0.1 port=5432 dbname=metadata user=metadata_user password=metadata_pass"
go run ./cmd/server
```

## gRPC Reflection
### Reflection is enabled, so you can inspect the API without passing proto files every time.

List services
```bash
grpcurl -plaintext 127.0.0.1:50051 list
```

### Describe the metadata service
```bash
grpcurl -plaintext 127.0.0.1:50051 describe metadata.MetadataService
```

## Example API Calls
### Create a table

`create_table.json`
```json
{
  "table_name": "events",
  "schema_json": "{\"fields\":[{\"name\":\"event_id\",\"type\":\"long\"},{\"name\":\"ts\",\"type\":\"timestamp\"}]}",
  "partition_spec": "month",
  "properties": {
    "owner": "data-eng"
  }
}
```
#### Windows CMD:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/CreateTable < create_table.json
```
### Get table metadata
`get_table.json`
```json
{
    "table_names":"events"
}
```
#### Windows CMD:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/GetTableMetadata < get_table.json
```

### Commit a snapshot
`commit_snapshot.json`
```json
{
  "table_name": "events",
  "parent_snapshot_id": 0,
  "operation": "append",
  "new_partitions": [
    {
      "partition_key": "month=2025-01",
      "data_file_path": "s3://bucket/events/month=2025-01/part-0.parquet",
      "row_count": 100000,
      "size_bytes": 15000000,
      "file_format": "parquet"
    }
  ]
}
```
#### Windows CMD:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/CommitSnapshot < commit_snapshot.json
```
### Get partitions
`get_partitions.json`
```json
{
  "table_name": "events"
}
```
#### Windows CMD:
`grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/GetPartitions < get_partitions.json`

### Get a Snapshot
`get_snapshot.json`
```json
{
  "table_name": "events",
  "snapshot_id": 1
}
```
#### Windows CMD:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/GetSnapshot < get_snapshot.json
```

### List a Snapshot
`list_snapshots.json`
```json
{
  "table_name": "events",
  "limit": 10
}
```
#### Windows CMD:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/ListSnapshots < list_snapshots.json
```

