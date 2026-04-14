package tests

import (
	"context"
	"net"
	"strings"
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

func createTable(t *testing.T, ctx context.Context, client metadata.MetadataServiceClient, tableName string) {
	t.Helper()

	resp, err := client.CreateTable(ctx, &metadata.CreateTableRequest{
		TableName:     tableName,
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"}]}`,
		PartitionSpec: "month",
	})
	if err != nil {
		t.Fatalf("CreateTable(%s) failed: %v", tableName, err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("CreateTable(%s) unsuccessful: %s", tableName, resp.GetErrorMsg())
	}
}

func makePartition(key string) *metadata.PartitionInfo {
	return &metadata.PartitionInfo{
		PartitionKey: key,
		DataFilePath: "s3://bucket/events/" + key + "/part-0.parquet",
		RowCount:     100000,
		SizeBytes:    15000000,
		FileFormat:   "parquet",
	}
}

func commitSnapshot(
	t *testing.T,
	ctx context.Context,
	client metadata.MetadataServiceClient,
	tableName string,
	parentSnapshotID uint64,
	partitions ...*metadata.PartitionInfo,
) *metadata.SnapshotResponse {
	t.Helper()

	resp, err := client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:        tableName,
		ParentSnapshotId: parentSnapshotID,
		Operation:        "append",
		NewPartitions:    partitions,
	})
	if err != nil {
		t.Fatalf("CommitSnapshot(%s) failed: %v", tableName, err)
	}
	return resp
}

func tableNames(tables []*metadata.TableSummary) string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		if table == nil {
			continue
		}
		names = append(names, table.GetTableName())
	}
	return strings.Join(names, ",")
}

func partitionKeys(partitions []*metadata.PartitionInfo) string {
	keys := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		if partition == nil {
			continue
		}
		keys = append(keys, partition.GetPartitionKey())
	}
	return strings.Join(keys, ",")
}

func TestTableNamesWithNilAndEmptyInput(t *testing.T) {
	tests := []struct {
		name   string
		tables []*metadata.TableSummary
		want   string
	}{
		{
			name: "nil slice",
		},
		{
			name:   "empty slice",
			tables: []*metadata.TableSummary{},
		},
		{
			name: "nil table",
			tables: []*metadata.TableSummary{
				nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tableNames(tt.tables); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestPartitionKeysHandlesNilAndEmptyInput(t *testing.T) {
	tests := []struct {
		name       string
		partitions []*metadata.PartitionInfo
		want       string
	}{
		{
			name: "nil slice",
		},
		{
			name:       "empty slice",
			partitions: []*metadata.PartitionInfo{},
		},
		{
			name: "nil partition",
			partitions: []*metadata.PartitionInfo{
				nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := partitionKeys(tt.partitions); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
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

func TestListTablesPagination(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events_a")
	createTable(t, ctx, client, "events_b")
	createTable(t, ctx, client, "events_c")

	defaultedPage, err := client.ListTables(ctx, &metadata.ListTablesRequest{
		PageSize:  2,
		PageToken: "not-a-number",
	})
	if err != nil {
		t.Fatalf("ListTables with invalid page token failed: %v", err)
	}

	firstPage, err := client.ListTables(ctx, &metadata.ListTablesRequest{
		PageSize: 2,
	})
	if err != nil {
		t.Fatalf("first ListTables failed: %v", err)
	}
	if tableNames(defaultedPage.GetTables()) != tableNames(firstPage.GetTables()) ||
		defaultedPage.GetNextPageToken() != firstPage.GetNextPageToken() {
		t.Fatalf("expected invalid ListTables page token to default to first page")
	}
	if len(firstPage.GetTables()) != 2 {
		t.Fatalf("expected 2 tables on first page, got %d", len(firstPage.GetTables()))
	}
	if firstPage.GetTables()[0].GetTableName() != "events_a" || firstPage.GetTables()[1].GetTableName() != "events_b" {
		t.Fatalf("unexpected first page order: %v", firstPage.GetTables())
	}
	if firstPage.GetNextPageToken() == "" {
		t.Fatalf("expected next page token on first page")
	}

	secondPage, err := client.ListTables(ctx, &metadata.ListTablesRequest{
		PageSize:  2,
		PageToken: firstPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("second ListTables failed: %v", err)
	}
	if len(secondPage.GetTables()) != 1 {
		t.Fatalf("expected 1 table on second page, got %d", len(secondPage.GetTables()))
	}
	if secondPage.GetTables()[0].GetTableName() != "events_c" {
		t.Fatalf("unexpected second page table: %q", secondPage.GetTables()[0].GetTableName())
	}
	if secondPage.GetNextPageToken() != "" {
		t.Fatalf("expected no next page token on final partial page, got %q", secondPage.GetNextPageToken())
	}
}

func TestGetPartitionsPagination(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")
	commitResp := commitSnapshot(
		t,
		ctx,
		client,
		"events",
		0,
		makePartition("month=2025-01"),
		makePartition("month=2025-02"),
		makePartition("month=2025-03"),
	)
	if !commitResp.GetSuccess() {
		t.Fatalf("CommitSnapshot unsuccessful: %s", commitResp.GetErrorMsg())
	}

	defaultedPage, err := client.GetPartitions(ctx, &metadata.PartitionRequest{
		TableName: "events",
		PageSize:  2,
		PageToken: "not-a-number",
	})
	if err != nil {
		t.Fatalf("GetPartitions with invalid page token failed: %v", err)
	}

	firstPage, err := client.GetPartitions(ctx, &metadata.PartitionRequest{
		TableName: "events",
		PageSize:  2,
	})
	if err != nil {
		t.Fatalf("first GetPartitions failed: %v", err)
	}
	if partitionKeys(defaultedPage.GetPartitions()) != partitionKeys(firstPage.GetPartitions()) ||
		defaultedPage.GetNextPageToken() != firstPage.GetNextPageToken() {
		t.Fatalf("expected invalid GetPartitions page token to default to first page")
	}
	if len(firstPage.GetPartitions()) != 2 {
		t.Fatalf("expected 2 partitions on first page, got %d", len(firstPage.GetPartitions()))
	}
	if firstPage.GetPartitions()[0].GetPartitionKey() != "month=2025-01" ||
		firstPage.GetPartitions()[1].GetPartitionKey() != "month=2025-02" {
		t.Fatalf("unexpected first page partitions: %v", firstPage.GetPartitions())
	}
	if firstPage.GetNextPageToken() == "" {
		t.Fatalf("expected next page token on first page")
	}

	secondPage, err := client.GetPartitions(ctx, &metadata.PartitionRequest{
		TableName: "events",
		PageSize:  2,
		PageToken: firstPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("second GetPartitions failed: %v", err)
	}
	if len(secondPage.GetPartitions()) != 1 {
		t.Fatalf("expected 1 partition on second page, got %d", len(secondPage.GetPartitions()))
	}
	if secondPage.GetPartitions()[0].GetPartitionKey() != "month=2025-03" {
		t.Fatalf("unexpected second page partition: %q", secondPage.GetPartitions()[0].GetPartitionKey())
	}
	if secondPage.GetPartitions()[0].GetPartitionKey() == firstPage.GetPartitions()[0].GetPartitionKey() ||
		secondPage.GetPartitions()[0].GetPartitionKey() == firstPage.GetPartitions()[1].GetPartitionKey() {
		t.Fatalf("expected no duplicate partitions across pages")
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

	createTable(t, ctx, client, "events")

	// first valid snapshot
	firstCommit := commitSnapshot(t, ctx, client, "events", 0, makePartition("month=2025-01"))
	if !firstCommit.GetSuccess() {
		t.Fatalf("first CommitSnapshot unsuccessful: %s", firstCommit.GetErrorMsg())
	}
	if firstCommit.GetSnapshotId() != 1 {
		t.Fatalf("expected first snapshot id 1, got %d", firstCommit.GetSnapshotId())
	}

	currentParentCommit := commitSnapshot(t, ctx, client, "events", firstCommit.GetSnapshotId(), makePartition("month=2025-02"))
	if !currentParentCommit.GetSuccess() {
		t.Fatalf("current-parent CommitSnapshot unsuccessful: %s", currentParentCommit.GetErrorMsg())
	}
	if currentParentCommit.GetSnapshotId() != 2 {
		t.Fatalf("expected second snapshot id 2, got %d", currentParentCommit.GetSnapshotId())
	}

	// stale parent id should fail now that current snapshot is 2
	secondCommit := commitSnapshot(t, ctx, client, "events", firstCommit.GetSnapshotId(), makePartition("month=2025-03"))
	if secondCommit.GetSuccess() {
		t.Fatalf("expected stale-parent CommitSnapshot to fail")
	}
	if secondCommit.GetErrorMsg() == "" {
		t.Fatalf("expected stale-parent CommitSnapshot to return an error message")
	}
	if !strings.Contains(secondCommit.GetErrorMsg(), "current=2") {
		t.Fatalf("expected stale-parent error to mention current snapshot, got %q", secondCommit.GetErrorMsg())
	}

	snapshotsResp, err := client.ListSnapshots(ctx, &metadata.ListSnapshotsRequest{
		TableName: "events",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(snapshotsResp.GetSnapshots()) != 2 {
		t.Fatalf("expected failed stale commit not to create a snapshot, got %d snapshots", len(snapshotsResp.GetSnapshots()))
	}

	partResp, err := client.GetPartitions(ctx, &metadata.PartitionRequest{
		TableName: "events",
	})
	if err != nil {
		t.Fatalf("GetPartitions failed: %v", err)
	}
	if len(partResp.GetPartitions()) != 2 {
		t.Fatalf("expected failed stale commit not to add partitions, got %d partitions", len(partResp.GetPartitions()))
	}
	for _, part := range partResp.GetPartitions() {
		if part.GetPartitionKey() == "month=2025-03" {
			t.Fatalf("expected stale commit partition not to be visible")
		}
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
