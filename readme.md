# IceLog

IceLog is a Go + gRPC metadata control plane for lakehouse-style table catalogs. It tracks table schemas, partition files, immutable snapshots, and lightweight transaction state in PostgreSQL, then serves that metadata through a typed gRPC API.

Built to demonstrate backend systems work: concurrency control, snapshot isolation concepts, cache invalidation, durable metadata storage, Dockerized local development, and CI-backed tests.

## Highlights

- gRPC metadata service with protobuf-generated Go clients and server stubs.
- PostgreSQL-backed catalog for tables, schemas, snapshots, partitions, and transactions.
- Immutable snapshot commits with stale-parent conflict detection.
- Point-in-time partition reads by snapshot ID.
- Per-table shared/exclusive locking for concurrent metadata access.
- In-memory LRU cache for hot partition metadata, with invalidation on writes.
- Unit and integration tests, including cache behavior, MVCC lifecycle, schema validation, pagination, and stale snapshot conflicts.
- Docker Compose setup for local PostgreSQL plus GitHub Actions for unit tests, integration tests, and Docker image build.

## Architecture

```text
gRPC clients
    |
    v
internal/server       Request validation, response mapping, MetadataService
    |
    v
internal/catalog      Table lifecycle, schema evolution, snapshots, partitions
    |
    +--> internal/transaction   Active transactions, pinned snapshots, MVCC checks
    +--> internal/cache         Generic LRU cache for partition metadata
    +--> internal/lock          Per-table read/write locks
    |
    v
internal/db           PostgreSQL persistence layer
```

PostgreSQL stores:

- `tables`: table definitions, schema JSON, properties, current snapshot pointer
- `schema_history`: versioned schema changes
- `snapshots`: immutable snapshot records
- `partitions`: file-level partition metadata and visibility windows
- `transactions`: transaction lifecycle and read snapshot tracking

## Core Flows

**Read path**

1. Resolve the requested snapshot, or use the current table snapshot.
2. Check the partition cache for `table:snapshot`.
3. On cache miss, load visible partitions from PostgreSQL.
4. Return file paths, row counts, sizes, schema, and table properties.

**Write path**

1. Acquire the table's exclusive metadata lock.
2. Validate that the request's parent snapshot is still current.
3. Insert a new immutable snapshot and partition changes in one DB transaction.
4. Move the table's current snapshot pointer.
5. Invalidate cached partition metadata for that table.

## API Surface

The protobuf service is defined in [proto/metadata_service.proto](proto/metadata_service.proto).

Main RPC groups:

- Tables: `CreateTable`, `GetTableMetadata`, `AlterTable`, `DropTable`, `ListTables`
- Partitions: `GetPartitions`, `GetPartitionStats`
- Snapshots: `CommitSnapshot`, `GetSnapshot`, `ListSnapshots`
- Transactions: `BeginTransaction`, `CommitTransaction`, `AbortTransaction`

## Project Layout

```text
cmd/server/              Server entrypoint
internal/server/         gRPC service implementation
internal/catalog/        Catalog, schema, snapshot, and partition logic
internal/transaction/    MVCC and transaction state
internal/db/             PostgreSQL client and models
internal/cache/          Generic LRU cache
internal/lock/           Per-table lock manager
proto/                   Protobuf definitions
gen/metadata/            Generated protobuf/gRPC code
tests/                   PostgreSQL-backed integration tests
scripts/                 DB init SQL and grpcurl payloads
```

## Quick Start

Run PostgreSQL and the gRPC server:

```bash
docker compose up --build
```

The server listens on:

```text
127.0.0.1:50051
```

Run only PostgreSQL, then start the server locally:

```bash
docker compose up -d --wait postgres
make run
```

Equivalent PowerShell setup:

```powershell
$env:PG_CONN_STRING="host=127.0.0.1 port=5432 dbname=metadata user=metadata_user password=metadata_pass"
go run ./cmd/server
```

## Testing

Unit tests:

```bash
make test-unit
```

Integration tests:

```bash
make test-integration
```

All tests:

```bash
make test
```

Without Make:

```bash
go test -v ./cmd/... ./gen/... ./internal/...
docker compose up -d --wait postgres
go test -v ./tests
```

## Example Calls

Reflection is enabled:

```bash
grpcurl -plaintext 127.0.0.1:50051 list metadata.MetadataService
```

Create a table:

```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/CreateTable < scripts/grpc/create_table.json
```

Commit a snapshot:

```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/CommitSnapshot < scripts/grpc/commit_snapshot.json
```

Read partition metadata:

```bash
grpcurl -plaintext -d @ 127.0.0.1:50051 metadata.MetadataService/GetPartitions < scripts/grpc/get_partitions.json
```

More request payloads are available in [scripts/grpc](scripts/grpc).

## Development

Generate protobuf code:

```bash
make proto
```

Run the server locally:

```bash
make run
```

Tidy dependencies:

```bash
make tidy
```

## Current Limits

- Partition spec evolution is intentionally not implemented yet.
- Transactions are lightweight metadata transactions, not a distributed transaction protocol.
- `ListTables` computes visible partition counts per table and can be batch-optimized later.
