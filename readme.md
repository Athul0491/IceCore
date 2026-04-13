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

### 1. Run the full stack with Docker Compose
```bash
docker compose up --build
```

This starts PostgreSQL, waits for it to become healthy, builds the Go gRPC
server image, and starts the metadata server on:

```text
127.0.0.1:50051
```

On Windows with Docker Desktop and WSL enabled, run the same command from this
repository directory in PowerShell or a WSL shell.

### Alternative: Start only PostgreSQL and run the server locally
```bash
docker compose up -d postgres
```

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

### Running Tests
Unit tests do not require Docker or PostgreSQL:

```bash
make test-unit
```

Integration tests use the PostgreSQL service from Docker Compose:

```bash
make test-integration
```

Run both:

```bash
make test
```

If Make is not installed on Windows, the equivalent commands are:

```powershell
go test -v ./cmd/... ./gen/... ./internal/...
docker compose up -d --wait postgres
go test -v ./tests
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

## How It Works

IceLog does not store table data itself. Instead, it stores metadata about tables, partitions, immutable snapshots, and transaction state, then serves that metadata over gRPC to clients such as query engines, ETL jobs, or admin tooling.

### Typical Read Flow

A typical read flow looks like this:

1. A client requests metadata for a table or asks for visible partitions.
2. IceLog resolves the requested snapshot, or falls back to the table's current snapshot.
3. The service checks the in-memory LRU cache for partition metadata.
4. On a cache miss, it loads the visible partitions from PostgreSQL.
5. IceLog returns the exact file paths and metadata needed to read the table at that point in time.

### Typical Write Flow

A typical write flow looks like this:

1. A client commits a new snapshot with added and/or deleted partitions.
2. IceLog validates that the parent snapshot still matches the table's current snapshot.
3. The service writes a new immutable snapshot record and associated partition changes.
4. The table's current snapshot pointer is updated transactionally.
5. Cached partition metadata for that table is invalidated.

### Benefits

This architecture gives the system:

- immutable snapshot history
- point-in-time table views
- optimistic concurrency on snapshot commits
- fast repeated partition lookups via caching

### Architecture

IceLog is organized into a few core layers:

#### gRPC API layer
`internal/server/`

The gRPC server exposes the MetadataService defined in `proto/metadata_service.proto`. It handles request validation, response shaping, and delegation to the catalog, partition, snapshot, and transaction layers.

#### Catalog and schema management
`internal/catalog/`

This layer owns:

- table creation, rename, deletion, and listing
- schema retrieval and schema evolution
- partition metadata lookup
- snapshot commit orchestration

#### Transaction and MVCC tracking
`internal/transaction/`

This layer tracks:

- active transactions
- pinned read snapshots
- transaction expiration and cleanup
- parent snapshot validation during snapshot commits

#### Persistence layer
`internal/db/`

PostgreSQL is the durable metadata store for:

- tables
- schema history
- snapshots
- partitions
- transaction rows

#### Cache and locking
`internal/cache/`, `internal/lock/`

IceLog uses:

- an in-memory LRU cache for hot partition metadata
- per-table read/write locks for concurrency control
- cache invalidation on metadata mutation

### Request Flow

#### Read path

For `GetTableMetadata` or `GetPartitions`:

1. Acquire a shared lock for the table.
2. Resolve the requested snapshot or current snapshot.
3. Check the in-memory cache for table:snapshot.
4. On cache miss, query PostgreSQL for visible partitions.
5. Return partition metadata and aggregate stats.

#### Write path

For `CommitSnapshot`:

1. Acquire an exclusive lock for the table.
2. Read the current table snapshot.
3. Validate the provided parent snapshot.
4. Insert a new snapshot row.
5. Insert new partitions and/or mark deleted ones.
6. Update the table's current snapshot pointer.
7. Invalidate cache entries for that table.

### Concurrency Model

IceLog uses per-table locking plus optimistic snapshot validation.

- Reads take a shared lock and can proceed concurrently.
- Metadata mutations take an exclusive lock per table.
- Snapshot commits succeed only if the provided parent snapshot is still current.

This avoids conflicting table mutations while still allowing concurrent reads.

### Storage Model

The PostgreSQL schema stores:

- `tables`: table definitions, schema, properties, current snapshot pointer
- `schema_history`: historical schema versions
- `snapshots`: immutable snapshot records
- `partitions`: partition/file metadata with visibility over snapshots
- `transactions`: transaction lifecycle and read snapshot tracking

### Current Limitations

- `AlterTable` does not yet support partition spec evolution.
- Transaction lifecycle is table-aware at begin time, but transaction semantics are still lightweight compared to a full distributed transaction protocol.
- `ListTables` uses per-table count queries for visible partitions, which is correct but not yet batch-optimized.
- Integration tests currently cover the main happy paths, but not every error case.

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

