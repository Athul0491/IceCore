package tests

import (
	"context"
	"encoding/json"
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

func TestAlterTablePartitionSpec(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")

	// Get the initial spec (version 1, identity on "month")
	specResp, err := client.GetPartitionSpec(ctx, &metadata.GetPartitionSpecRequest{
		TableName: "events",
	})
	if err != nil {
		t.Fatalf("GetPartitionSpec failed: %v", err)
	}
	if specResp.GetSpecVersion() != 1 {
		t.Fatalf("expected spec version 1, got %d", specResp.GetSpecVersion())
	}
	if len(specResp.GetSpec().GetFields()) == 0 {
		t.Fatalf("expected at least one field in initial spec")
	}

	// Add a second field (field_id=2)
	alterResp, err := client.AlterTable(ctx, &metadata.AlterTableRequest{
		TableName: "events",
		Alteration: &metadata.AlterTableRequest_NewPartitionSpec{
			NewPartitionSpec: &metadata.PartitionSpecProto{
				Fields: []*metadata.PartitionField{
					{FieldId: 1, SourceColumn: "month", Transform: metadata.PartitionTransform_IDENTITY},
					{FieldId: 2, SourceColumn: "region", Transform: metadata.PartitionTransform_IDENTITY},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AlterTable (spec) failed: %v", err)
	}
	if !alterResp.GetSuccess() {
		t.Fatalf("AlterTable (spec) unsuccessful: %s", alterResp.GetErrorMsg())
	}

	// Current spec should now be version 2
	specV2, err := client.GetPartitionSpec(ctx, &metadata.GetPartitionSpecRequest{
		TableName: "events",
	})
	if err != nil {
		t.Fatalf("GetPartitionSpec v2 failed: %v", err)
	}
	if specV2.GetSpecVersion() != 2 {
		t.Fatalf("expected spec version 2, got %d", specV2.GetSpecVersion())
	}
	if len(specV2.GetSpec().GetFields()) != 2 {
		t.Fatalf("expected 2 fields in spec v2, got %d", len(specV2.GetSpec().GetFields()))
	}

	// ListPartitionSpecs should return 2 entries
	listResp, err := client.ListPartitionSpecs(ctx, &metadata.ListPartitionSpecsRequest{TableName: "events"})
	if err != nil {
		t.Fatalf("ListPartitionSpecs failed: %v", err)
	}
	if len(listResp.GetSpecs()) != 2 {
		t.Fatalf("expected 2 spec history entries, got %d", len(listResp.GetSpecs()))
	}
}

func TestAlterTablePartitionSpecRemoveFieldFails(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")

	// Attempt to alter spec removing the initial field (field_id 1 missing)
	resp, err := client.AlterTable(ctx, &metadata.AlterTableRequest{
		TableName: "events",
		Alteration: &metadata.AlterTableRequest_NewPartitionSpec{
			NewPartitionSpec: &metadata.PartitionSpecProto{
				Fields: []*metadata.PartitionField{
					{FieldId: 2, SourceColumn: "region", Transform: metadata.PartitionTransform_IDENTITY},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AlterTable RPC failed unexpectedly: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatalf("expected removing a field to fail, but it succeeded")
	}
	if resp.GetErrorMsg() == "" {
		t.Fatalf("expected error message for field removal")
	}
}

func TestAlterTablePartitionSpecChangeSourceColumnFails(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")

	resp, err := client.AlterTable(ctx, &metadata.AlterTableRequest{
		TableName: "events",
		Alteration: &metadata.AlterTableRequest_NewPartitionSpec{
			NewPartitionSpec: &metadata.PartitionSpecProto{
				Fields: []*metadata.PartitionField{
					{FieldId: 1, SourceColumn: "year", Transform: metadata.PartitionTransform_IDENTITY},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AlterTable RPC failed unexpectedly: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatalf("expected changing source_column to fail, but it succeeded")
	}
}

func TestCommitSnapshotStampsSpecID(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")
	snap := commitSnapshot(t, ctx, client, "events", 0, makePartition("month=2025-01"))
	if !snap.GetSuccess() {
		t.Fatalf("CommitSnapshot failed: %s", snap.GetErrorMsg())
	}

	listResp, err := client.GetManifestList(ctx, &metadata.GetManifestListRequest{
		TableName:  "events",
		SnapshotId: snap.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetManifestList failed: %v", err)
	}
	if len(listResp.GetManifests()) == 0 {
		t.Fatalf("expected at least one manifest")
	}
	for _, m := range listResp.GetManifests() {
		if m.GetPartitionSpecId() != 1 {
			t.Fatalf("expected partition_spec_id=1, got %d", m.GetPartitionSpecId())
		}
	}
}

func TestAlterSpecThenCommitUsesNewSpecID(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")

	// Alter spec to v2
	alterResp, err := client.AlterTable(ctx, &metadata.AlterTableRequest{
		TableName: "events",
		Alteration: &metadata.AlterTableRequest_NewPartitionSpec{
			NewPartitionSpec: &metadata.PartitionSpecProto{
				Fields: []*metadata.PartitionField{
					{FieldId: 1, SourceColumn: "month", Transform: metadata.PartitionTransform_IDENTITY},
					{FieldId: 2, SourceColumn: "region", Transform: metadata.PartitionTransform_IDENTITY},
				},
			},
		},
	})
	if err != nil || !alterResp.GetSuccess() {
		t.Fatalf("AlterTable failed: %v / %s", err, alterResp.GetErrorMsg())
	}

	snap := commitSnapshot(t, ctx, client, "events", 0, makePartition("month=2025-01"))
	if !snap.GetSuccess() {
		t.Fatalf("CommitSnapshot failed: %s", snap.GetErrorMsg())
	}

	listResp, err := client.GetManifestList(ctx, &metadata.GetManifestListRequest{
		TableName:  "events",
		SnapshotId: snap.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetManifestList failed: %v", err)
	}
	for _, m := range listResp.GetManifests() {
		if m.GetPartitionSpecId() != 2 {
			t.Fatalf("expected partition_spec_id=2 after spec alter, got %d", m.GetPartitionSpecId())
		}
	}
}

func TestGetPartitionSpecByVersion(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")

	// Alter to v2
	alterResp, err := client.AlterTable(ctx, &metadata.AlterTableRequest{
		TableName: "events",
		Alteration: &metadata.AlterTableRequest_NewPartitionSpec{
			NewPartitionSpec: &metadata.PartitionSpecProto{
				Fields: []*metadata.PartitionField{
					{FieldId: 1, SourceColumn: "month", Transform: metadata.PartitionTransform_IDENTITY},
					{FieldId: 2, SourceColumn: "region", Transform: metadata.PartitionTransform_IDENTITY},
				},
			},
		},
	})
	if err != nil || !alterResp.GetSuccess() {
		t.Fatalf("AlterTable failed: %v / %s", err, alterResp.GetErrorMsg())
	}

	// spec_version=0 → current (v2)
	current, err := client.GetPartitionSpec(ctx, &metadata.GetPartitionSpecRequest{
		TableName: "events",
	})
	if err != nil {
		t.Fatalf("GetPartitionSpec (current) failed: %v", err)
	}
	if current.GetSpecVersion() != 2 {
		t.Fatalf("expected current version 2, got %d", current.GetSpecVersion())
	}

	// spec_version=1 → original
	v1, err := client.GetPartitionSpec(ctx, &metadata.GetPartitionSpecRequest{
		TableName:   "events",
		SpecVersion: 1,
	})
	if err != nil {
		t.Fatalf("GetPartitionSpec v1 failed: %v", err)
	}
	if v1.GetSpecVersion() != 1 {
		t.Fatalf("expected version 1, got %d", v1.GetSpecVersion())
	}
	if len(v1.GetSpec().GetFields()) != 1 {
		t.Fatalf("expected 1 field in v1, got %d", len(v1.GetSpec().GetFields()))
	}
}

func TestGetManifestListAfterCommit(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")
	snap := commitSnapshot(t, ctx, client, "events", 0,
		makePartition("month=2025-01"),
		makePartition("month=2025-02"),
	)
	if !snap.GetSuccess() {
		t.Fatalf("CommitSnapshot failed: %s", snap.GetErrorMsg())
	}

	resp, err := client.GetManifestList(ctx, &metadata.GetManifestListRequest{
		TableName:  "events",
		SnapshotId: snap.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetManifestList failed: %v", err)
	}
	if resp.GetManifestCount() < 1 {
		t.Fatalf("expected at least 1 manifest, got %d", resp.GetManifestCount())
	}
	if len(resp.GetManifests()) == 0 {
		t.Fatalf("expected non-empty manifests list")
	}

	var foundAdded bool
	for _, m := range resp.GetManifests() {
		if m.GetAddedFilesCount() == 2 {
			foundAdded = true
		}
		if m.GetManifestPath() == "" {
			t.Fatalf("manifest_path should not be empty")
		}
	}
	if !foundAdded {
		t.Fatalf("expected a manifest with added_files_count == 2")
	}
}

func TestGetManifestListNotFoundForMissingSnapshot(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")

	_, err := client.GetManifestList(ctx, &metadata.GetManifestListRequest{
		TableName:  "events",
		SnapshotId: 9999,
	})
	if err == nil {
		t.Fatalf("expected GetManifestList to fail for missing snapshot")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestGetManifestFileByID(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")
	snap := commitSnapshot(t, ctx, client, "events", 0, makePartition("month=2025-01"))
	if !snap.GetSuccess() {
		t.Fatalf("CommitSnapshot failed: %s", snap.GetErrorMsg())
	}

	listResp, err := client.GetManifestList(ctx, &metadata.GetManifestListRequest{
		TableName:  "events",
		SnapshotId: snap.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetManifestList failed: %v", err)
	}
	if len(listResp.GetManifests()) == 0 {
		t.Fatalf("expected at least one manifest file")
	}

	mfID := listResp.GetManifests()[0].GetManifestFileId()
	detail, err := client.GetManifest(ctx, &metadata.GetManifestRequest{
		TableName:      "events",
		ManifestFileId: mfID,
	})
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if detail.GetSnapshotId() != snap.GetSnapshotId() {
		t.Fatalf("expected snapshot_id %d, got %d", snap.GetSnapshotId(), detail.GetSnapshotId())
	}
	if detail.GetAddedFilesCount() != 1 {
		t.Fatalf("expected added_files_count 1, got %d", detail.GetAddedFilesCount())
	}

	var summaries []map[string]string
	if err := json.Unmarshal([]byte(detail.GetPartitionSummariesJson()), &summaries); err != nil {
		t.Fatalf("partition_summaries_json is not valid JSON: %v", err)
	}
}

func TestGetManifestFileNotFound(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")

	_, err := client.GetManifest(ctx, &metadata.GetManifestRequest{
		TableName:      "events",
		ManifestFileId: 99999,
	})
	if err == nil {
		t.Fatalf("expected GetManifest to fail for missing manifest")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestCommitSnapshotWritesBothManifestAndPartitions(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")
	snap := commitSnapshot(t, ctx, client, "events", 0,
		makePartition("month=2025-01"),
		makePartition("month=2025-02"),
	)
	if !snap.GetSuccess() {
		t.Fatalf("CommitSnapshot failed: %s", snap.GetErrorMsg())
	}

	partsResp, err := client.GetPartitions(ctx, &metadata.PartitionRequest{TableName: "events"})
	if err != nil {
		t.Fatalf("GetPartitions failed: %v", err)
	}
	if len(partsResp.GetPartitions()) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(partsResp.GetPartitions()))
	}

	listResp, err := client.GetManifestList(ctx, &metadata.GetManifestListRequest{
		TableName:  "events",
		SnapshotId: snap.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetManifestList failed: %v", err)
	}
	if listResp.GetManifestCount() < 1 {
		t.Fatalf("expected at least 1 manifest in list, got %d", listResp.GetManifestCount())
	}
}

func TestDeletedManifestWrittenOnOverwrite(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTable(t, ctx, client, "events")

	snap1 := commitSnapshot(t, ctx, client, "events", 0, makePartition("month=2025-01"))
	if !snap1.GetSuccess() {
		t.Fatalf("first CommitSnapshot failed: %s", snap1.GetErrorMsg())
	}

	resp, err := client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:            "events",
		ParentSnapshotId:     snap1.GetSnapshotId(),
		Operation:            "overwrite",
		DeletedPartitionKeys: []string{"month=2025-01"},
		NewPartitions:        []*metadata.PartitionInfo{makePartition("month=2025-02")},
	})
	if err != nil {
		t.Fatalf("second CommitSnapshot RPC failed: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("second CommitSnapshot failed: %s", resp.GetErrorMsg())
	}

	listResp, err := client.GetManifestList(ctx, &metadata.GetManifestListRequest{
		TableName:  "events",
		SnapshotId: resp.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetManifestList failed: %v", err)
	}

	var hasDeleted, hasAdded bool
	for _, m := range listResp.GetManifests() {
		if m.GetDeletedFilesCount() >= 1 {
			hasDeleted = true
		}
		if m.GetAddedFilesCount() >= 1 {
			hasAdded = true
		}
	}
	if !hasDeleted {
		t.Fatalf("expected a manifest with deleted_files_count >= 1")
	}
	if !hasAdded {
		t.Fatalf("expected a manifest with added_files_count >= 1")
	}
}

func makePartitionWithStats(key string, stats []*metadata.ColumnStats) *metadata.PartitionInfo {
	return &metadata.PartitionInfo{
		PartitionKey: key,
		DataFilePath: "s3://bucket/events/" + key + "/part-0.parquet",
		RowCount:     50000,
		SizeBytes:    8000000,
		FileFormat:   "parquet",
		ColumnStats:  stats,
	}
}

func TestCommitSnapshotStoresColumnStats(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupTestServer(t)
	defer cleanup()

	createTable(t, ctx, client, "events")

	stats := []*metadata.ColumnStats{
		{ColumnId: 1, NullCount: 0, NanCount: 0, ValueCount: 50000, MinValue: "100", MaxValue: "999", SizeBytes: 4000000},
		{ColumnId: 2, NullCount: 10, NanCount: 5, ValueCount: 50000, MinValue: "2024-01-01", MaxValue: "2024-12-31", SizeBytes: 4000000},
	}

	snap := commitSnapshot(t, ctx, client, "events", 0, makePartitionWithStats("month=2024-01", stats))
	if !snap.GetSuccess() {
		t.Fatalf("CommitSnapshot failed: %s", snap.GetErrorMsg())
	}

	meta, err := client.GetTableMetadata(ctx, &metadata.TableRequest{TableName: "events"})
	if err != nil {
		t.Fatalf("GetTableMetadata failed: %v", err)
	}
	if len(meta.GetPartitions()) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(meta.GetPartitions()))
	}

	got := meta.GetPartitions()[0].GetColumnStats()
	if len(got) != 2 {
		t.Fatalf("expected 2 column stats entries, got %d", len(got))
	}

	byID := map[int32]*metadata.ColumnStats{}
	for _, cs := range got {
		byID[cs.GetColumnId()] = cs
	}

	c1 := byID[1]
	if c1 == nil {
		t.Fatal("missing stats for column_id=1")
	}
	if c1.GetNullCount() != 0 || c1.GetValueCount() != 50000 || c1.GetMinValue() != "100" || c1.GetMaxValue() != "999" {
		t.Errorf("column 1 stats mismatch: %+v", c1)
	}

	c2 := byID[2]
	if c2 == nil {
		t.Fatal("missing stats for column_id=2")
	}
	if c2.GetNullCount() != 10 || c2.GetNanCount() != 5 {
		t.Errorf("column 2 null/nan count mismatch: %+v", c2)
	}
}

func TestGetColumnStatsAggregatesAcrossPartitions(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupTestServer(t)
	defer cleanup()

	createTable(t, ctx, client, "sales")

	p1 := makePartitionWithStats("month=2024-01", []*metadata.ColumnStats{
		{ColumnId: 1, NullCount: 2, NanCount: 0, ValueCount: 1000, MinValue: "10", MaxValue: "500"},
		{ColumnId: 2, NullCount: 0, NanCount: 0, ValueCount: 1000, MinValue: "alpha", MaxValue: "gamma"},
	})
	p2 := makePartitionWithStats("month=2024-02", []*metadata.ColumnStats{
		{ColumnId: 1, NullCount: 5, NanCount: 1, ValueCount: 800, MinValue: "5", MaxValue: "900"},
		{ColumnId: 2, NullCount: 3, NanCount: 0, ValueCount: 800, MinValue: "beta", MaxValue: "zeta"},
	})

	snap := commitSnapshot(t, ctx, client, "sales", 0, p1, p2)
	if !snap.GetSuccess() {
		t.Fatalf("CommitSnapshot failed: %s", snap.GetErrorMsg())
	}

	resp, err := client.GetColumnStats(ctx, &metadata.GetColumnStatsRequest{
		TableName:  "sales",
		SnapshotId: snap.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetColumnStats failed: %v", err)
	}
	if resp.GetSnapshotId() != snap.GetSnapshotId() {
		t.Errorf("snapshot_id mismatch: want %d got %d", snap.GetSnapshotId(), resp.GetSnapshotId())
	}

	byID := map[int32]*metadata.ColumnBounds{}
	for _, cb := range resp.GetColumnBounds() {
		byID[cb.GetColumnId()] = cb
	}

	c1 := byID[1]
	if c1 == nil {
		t.Fatal("missing bounds for column_id=1")
	}
	if c1.GetNullCount() != 7 {
		t.Errorf("column 1 null_count: want 7, got %d", c1.GetNullCount())
	}
	if c1.GetNanCount() != 1 {
		t.Errorf("column 1 nan_count: want 1, got %d", c1.GetNanCount())
	}
	if c1.GetValueCount() != 1800 {
		t.Errorf("column 1 value_count: want 1800, got %d", c1.GetValueCount())
	}
	if c1.GetMinValue() != "10" {
		t.Errorf("column 1 min_value: want '10', got %q", c1.GetMinValue())
	}
	if c1.GetMaxValue() != "900" {
		t.Errorf("column 1 max_value: want '900', got %q", c1.GetMaxValue())
	}

	c2 := byID[2]
	if c2 == nil {
		t.Fatal("missing bounds for column_id=2")
	}
	if c2.GetMinValue() != "alpha" {
		t.Errorf("column 2 min_value: want 'alpha', got %q", c2.GetMinValue())
	}
	if c2.GetMaxValue() != "zeta" {
		t.Errorf("column 2 max_value: want 'zeta', got %q", c2.GetMaxValue())
	}
}

func TestGetColumnStatsUsesCurrentSnapshotWhenZero(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupTestServer(t)
	defer cleanup()

	createTable(t, ctx, client, "logs")

	p := makePartitionWithStats("month=2024-03", []*metadata.ColumnStats{
		{ColumnId: 1, NullCount: 0, NanCount: 0, ValueCount: 200, MinValue: "a", MaxValue: "z"},
	})
	snap := commitSnapshot(t, ctx, client, "logs", 0, p)
	if !snap.GetSuccess() {
		t.Fatalf("CommitSnapshot failed: %s", snap.GetErrorMsg())
	}

	// snapshot_id = 0 → should resolve to current snapshot
	resp, err := client.GetColumnStats(ctx, &metadata.GetColumnStatsRequest{TableName: "logs"})
	if err != nil {
		t.Fatalf("GetColumnStats failed: %v", err)
	}
	if len(resp.GetColumnBounds()) != 1 {
		t.Fatalf("expected 1 column bounds entry, got %d", len(resp.GetColumnBounds()))
	}
}

func TestCommitSnapshotRejectsZeroRowCount(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupTestServer(t)
	defer cleanup()

	createTable(t, ctx, client, "strict")

	badPartition := &metadata.PartitionInfo{
		PartitionKey: "month=2024-01",
		DataFilePath: "s3://bucket/data.parquet",
		RowCount:     0,
		SizeBytes:    1000,
		FileFormat:   "parquet",
	}

	resp, err := client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:     "strict",
		Operation:     "append",
		NewPartitions: []*metadata.PartitionInfo{badPartition},
	})
	if err != nil {
		t.Fatalf("CommitSnapshot RPC failed: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("expected CommitSnapshot to fail for row_count=0, but it succeeded")
	}
	if resp.GetErrorMsg() == "" {
		t.Fatal("expected non-empty error_msg for zero row_count")
	}
}
