# IceLog

IceLog is a Go-based gRPC metadata control plane for table catalogs, schema evolution, partition tracking, snapshots, and transactions.

## What It Provides
- Table lifecycle operations: create, rename, drop, list, and get metadata.
- Snapshot lifecycle: commit, fetch, and list snapshots.
- Partition metadata management.
- Transaction primitives: begin, commit, and abort.
- gRPC reflection enabled for fast API discovery with `grpcurl`.

## Project Layout
```text
cmd/server/                 # Server entrypoint
internal/server/            # gRPC service implementation
internal/catalog/           # Catalog, schema, and partition management
internal/transaction/       # MVCC and transaction management
internal/db/                # Postgres client and models
internal/cache/             # In-memory LRU cache
proto/                      # Protobuf definitions
gen/metadata/               # Generated gRPC/protobuf code
scripts/grpc/               # Example grpcurl payloads and helper scripts
```

## Prerequisites
- Go 1.23+
- Docker Desktop (for local PostgreSQL)
- `protoc` (only needed if you regenerate protobuf code)
- `grpcurl`

## Quick Start

### 1. Start PostgreSQL
```bash
docker compose up -d postgres
```

### 2. Run the server
PowerShell:
```powershell
$env:PG_CONN_STRING="host=127.0.0.1 port=5432 dbname=metadata user=metadata_user password=metadata_pass"
go run ./cmd/server
```

Alternative using Makefile:
```bash
make run
```

Server default gRPC endpoint:
```text
127.0.0.1:50051
```

## Configuration
Runtime configuration is loaded from environment variables in `internal/config/config.go`.

At minimum, set:
- `PG_CONN_STRING`

Common runtime values shown on startup include:
- gRPC address
- Postgres pool size
- cache capacity
- transaction timeout

## API Discovery With Reflection
List available services:
```bash
grpcurl -plaintext 127.0.0.1:50051 list
```

Describe the metadata service:
```bash
grpcurl -plaintext 127.0.0.1:50051 describe metadata.MetadataService
```

List methods:
```bash
grpcurl -plaintext 127.0.0.1:50051 list metadata.MetadataService
```

## Example Calls

The repository already includes JSON request payloads in `scripts/grpc/`.

### Windows CMD helper
Use the helper script:
```bat
scripts\grpc\windows\call.cmd CreateTable scripts\grpc\create_table.json
```

### Direct grpcurl examples
Create table:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/CreateTable < scripts/grpc/create_table.json
```

Get table metadata:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/GetTableMetadata < scripts/grpc/get_table.json
```

Alter table schema:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/AlterTable < scripts/grpc/alter_schema.json
```

Rename table:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/AlterTable < scripts/grpc/rename_table.json
```

Drop table:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/DropTable < scripts/grpc/drop_table.json
```

List tables:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/ListTables < scripts/grpc/list_tables.json
```

Commit snapshot:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/CommitSnapshot < scripts/grpc/commit_snapshot.json
```

Get partitions:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/GetPartitions < scripts/grpc/get_partitions.json
```

Get partition stats:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/GetPartitionStats < scripts/grpc/get_partition_stats.json
```

Get snapshot:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/GetSnapshot < scripts/grpc/get_snapshot.json
```

List snapshots:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/ListSnapshots < scripts/grpc/list_snapshots.json
```

Begin transaction:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/BeginTransaction < scripts/grpc/begin_txn.json
```

Commit transaction:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/CommitTransaction < scripts/grpc/commit_txn.json
```

Abort transaction:
```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/AbortTransaction < scripts/grpc/abort_txn.json
```

More payloads are available in `scripts/grpc/` for:
- ad-hoc variations of these requests

## Development Commands
Generate protobuf code:
```bash
make proto
```

Run the server:
```bash
make run
```

Tidy dependencies:
```bash
make tidy
```

