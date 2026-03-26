package tests

import (
	"context"
	"net"
	"testing"
	"time"

	metadata "github.com/Athul0491/IceCore/gen/metadata"
	"github.com/Athul0491/IceCore/internal/server"
	"github.com/Athul0491/IceCore/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupTestServer(t *testing.T) (metadata.MetadataServiceClient, func()) {
	t.Helper()

	ctx := context.Background()
	if err := testutil.ResetDB(ctx); err != nil {
		t.Fatalf("reset db: %v", err)
	}

	cfg := testutil.TestConfig()

	svc, err := server.New(cfg)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	metadata.RegisterMetadataServiceServer(grpcServer, svc)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			// test shutdown can trigger errors, ignore here
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}

	client := metadata.NewMetadataServiceClient(conn)

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		lis.Close()
		svc.Close()
	}

	return client, cleanup
}

func TestCreateTableAndGetMetadata(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createResp, err := client.CreateTable(ctx, &metadata.CreateTableRequest{
		TableName:     "events",
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"},{"name":"ts","type":"timestamp"}]}`,
		PartitionSpec: "month",
		Properties: map[string]string{
			"owner": "data-eng",
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if !createResp.GetSuccess() {
		t.Fatalf("CreateTable unsuccessful: %s", createResp.GetErrorMsg())
	}

	metaResp, err := client.GetTableMetadata(ctx, &metadata.TableRequest{
		TableName: "events",
	})
	if err != nil {
		t.Fatalf("GetTableMetadata failed: %v", err)
	}

	if metaResp.GetTableName() != "events" {
		t.Fatalf("expected tableName=events, got %q", metaResp.GetTableName())
	}
	if metaResp.GetSchemaVersion() != 1 {
		t.Fatalf("expected schemaVersion=1, got %d", metaResp.GetSchemaVersion())
	}
	if metaResp.GetProperties()["owner"] != "data-eng" {
		t.Fatalf("expected owner=data-eng, got %q", metaResp.GetProperties()["owner"])
	}
}

func TestCommitSnapshotAndGetPartitions(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.CreateTable(ctx, &metadata.CreateTableRequest{
		TableName:     "events",
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"}]}`,
		PartitionSpec: "month",
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	commitResp, err := client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:        "events",
		ParentSnapshotId: 0,
		Operation:        "append",
		NewPartitions: []*metadata.PartitionInfo{
			{
				PartitionKey: "month=2025-01",
				DataFilePath: "s3://bucket/events/month=2025-01/part-0.parquet",
				RowCount:     100000,
				SizeBytes:    15000000,
				FileFormat:   "parquet",
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitSnapshot failed: %v", err)
	}
	if !commitResp.GetSuccess() {
		t.Fatalf("CommitSnapshot unsuccessful: %s", commitResp.GetErrorMsg())
	}
	if commitResp.GetSnapshotId() != 1 {
		t.Fatalf("expected snapshotId=1, got %d", commitResp.GetSnapshotId())
	}

	partResp, err := client.GetPartitions(ctx, &metadata.PartitionRequest{
		TableName: "events",
	})
	if err != nil {
		t.Fatalf("GetPartitions failed: %v", err)
	}

	if len(partResp.GetPartitions()) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(partResp.GetPartitions()))
	}
	p := partResp.GetPartitions()[0]
	if p.GetPartitionKey() != "month=2025-01" {
		t.Fatalf("unexpected partitionKey: %q", p.GetPartitionKey())
	}
	if p.GetSnapshotId() != 1 {
		t.Fatalf("expected snapshotId=1, got %d", p.GetSnapshotId())
	}
}

func TestListSnapshots(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.CreateTable(ctx, &metadata.CreateTableRequest{
		TableName:     "events",
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"}]}`,
		PartitionSpec: "month",
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	_, err = client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:        "events",
		ParentSnapshotId: 0,
		Operation:        "append",
		NewPartitions: []*metadata.PartitionInfo{
			{
				PartitionKey: "month=2025-01",
				DataFilePath: "s3://bucket/events/month=2025-01/part-0.parquet",
				RowCount:     100000,
				SizeBytes:    15000000,
				FileFormat:   "parquet",
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitSnapshot failed: %v", err)
	}

	resp, err := client.ListSnapshots(ctx, &metadata.ListSnapshotsRequest{
		TableName: "events",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}

	if len(resp.GetSnapshots()) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(resp.GetSnapshots()))
	}
	if resp.GetSnapshots()[0].GetSnapshotId() != 1 {
		t.Fatalf("expected snapshotId=1, got %d", resp.GetSnapshots()[0].GetSnapshotId())
	}
}

func TestTransactionLifecycle(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.CreateTable(ctx, &metadata.CreateTableRequest{
		TableName:     "events",
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"}]}`,
		PartitionSpec: "month",
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	_, err = client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:        "events",
		ParentSnapshotId: 0,
		Operation:        "append",
		NewPartitions: []*metadata.PartitionInfo{
			{
				PartitionKey: "month=2025-01",
				DataFilePath: "s3://bucket/events/month=2025-01/part-0.parquet",
				RowCount:     100000,
				SizeBytes:    15000000,
				FileFormat:   "parquet",
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitSnapshot failed: %v", err)
	}

	beginResp, err := client.BeginTransaction(ctx, &metadata.TransactionRequest{
		ClientId:  "spark-driver-1",
		TableName: "events",
		Isolation: metadata.IsolationLevel_SNAPSHOT,
	})
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	if beginResp.GetTxnId() == 0 {
		t.Fatalf("expected non-zero txn id")
	}
	if beginResp.GetReadSnapshotId() != 1 {
		t.Fatalf("expected readSnapshotId=1, got %d", beginResp.GetReadSnapshotId())
	}

	commitResp, err := client.CommitTransaction(ctx, &metadata.CommitRequest{
		TxnId: beginResp.GetTxnId(),
	})
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}
	if !commitResp.GetSuccess() {
		t.Fatalf("CommitTransaction unsuccessful: %s", commitResp.GetErrorMsg())
	}
}

func TestAbortTransaction(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.CreateTable(ctx, &metadata.CreateTableRequest{
		TableName:     "events",
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"}]}`,
		PartitionSpec: "month",
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	beginResp, err := client.BeginTransaction(ctx, &metadata.TransactionRequest{
		ClientId:  "spark-driver-1",
		TableName: "events",
		Isolation: metadata.IsolationLevel_SNAPSHOT,
	})
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	abortResp, err := client.AbortTransaction(ctx, &metadata.AbortRequest{
		TxnId: beginResp.GetTxnId(),
	})
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}
	if !abortResp.GetSuccess() {
		t.Fatalf("AbortTransaction unsuccessful: %s", abortResp.GetErrorMsg())
	}
}

func TestCreateTableDuplicateFails(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &metadata.CreateTableRequest{
		TableName:     "events",
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"}]}`,
		PartitionSpec: "month",
	}

	firstResp, err := client.CreateTable(ctx, req)
	if err != nil {
		t.Fatalf("first CreateTable failed: %v", err)
	}
	if !firstResp.GetSuccess() {
		t.Fatalf("first CreateTable unsuccessful: %s", firstResp.GetErrorMsg())
	}

	secondResp, err := client.CreateTable(ctx, req)
	if err != nil {
		t.Fatalf("second CreateTable failed: %v", err)
	}
	if secondResp.GetSuccess() {
		t.Fatalf("expected duplicate CreateTable to fail")
	}
	if secondResp.GetErrorMsg() == "" {
		t.Fatalf("expected duplicate CreateTable to return an error message")
	}
}

func TestGetTableMetadataMissingTable(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.GetTableMetadata(ctx, &metadata.TableRequest{
		TableName: "does_not_exist",
	})
	if err == nil {
		t.Fatalf("expected GetTableMetadata to fail for missing table")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", st.Code())
	}
}

func TestGetPartitionsMissingTable(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.GetPartitions(ctx, &metadata.PartitionRequest{
		TableName: "does_not_exist",
	})
	if err == nil {
		t.Fatalf("expected GetPartitions to fail for missing table")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", st.Code())
	}
}

func TestCommitSnapshotBadParentFails(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.CreateTable(ctx, &metadata.CreateTableRequest{
		TableName:     "events",
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"}]}`,
		PartitionSpec: "month",
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// first valid snapshot
	firstCommit, err := client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:        "events",
		ParentSnapshotId: 0,
		Operation:        "append",
		NewPartitions: []*metadata.PartitionInfo{
			{
				PartitionKey: "month=2025-01",
				DataFilePath: "s3://bucket/events/month=2025-01/part-0.parquet",
				RowCount:     100000,
				SizeBytes:    15000000,
				FileFormat:   "parquet",
			},
		},
	})
	if err != nil {
		t.Fatalf("first CommitSnapshot failed: %v", err)
	}
	if !firstCommit.GetSuccess() {
		t.Fatalf("first CommitSnapshot unsuccessful: %s", firstCommit.GetErrorMsg())
	}

	// stale parent id should fail now that current snapshot is 1
	secondCommit, err := client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:        "events",
		ParentSnapshotId: 0,
		Operation:        "append",
		NewPartitions: []*metadata.PartitionInfo{
			{
				PartitionKey: "month=2025-02",
				DataFilePath: "s3://bucket/events/month=2025-02/part-0.parquet",
				RowCount:     200000,
				SizeBytes:    25000000,
				FileFormat:   "parquet",
			},
		},
	})
	if err != nil {
		t.Fatalf("second CommitSnapshot RPC failed unexpectedly: %v", err)
	}
	if secondCommit.GetSuccess() {
		t.Fatalf("expected stale-parent CommitSnapshot to fail")
	}
	if secondCommit.GetErrorMsg() == "" {
		t.Fatalf("expected stale-parent CommitSnapshot to return an error message")
	}
}

func TestBeginTransactionMissingTable(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.BeginTransaction(ctx, &metadata.TransactionRequest{
		ClientId:  "spark-driver-1",
		TableName: "does_not_exist",
		Isolation: metadata.IsolationLevel_SNAPSHOT,
	})
	if err == nil {
		t.Fatalf("expected BeginTransaction to fail for missing table")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", st.Code())
	}
}

func TestCommitTransactionUnknownTxnFails(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CommitTransaction(ctx, &metadata.CommitRequest{
		TxnId: 999999,
	})
	if err != nil {
		t.Fatalf("CommitTransaction RPC failed unexpectedly: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatalf("expected CommitTransaction on unknown txn to fail")
	}
	if resp.GetErrorMsg() == "" {
		t.Fatalf("expected error message for unknown txn")
	}
}
