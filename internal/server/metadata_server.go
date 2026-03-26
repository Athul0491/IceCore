package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	metadata "github.com/Athul0491/IceCore/gen/metadata"
	"github.com/Athul0491/IceCore/internal/catalog"
	"github.com/Athul0491/IceCore/internal/config"
	"github.com/Athul0491/IceCore/internal/db"
	"github.com/Athul0491/IceCore/internal/lock"
	"github.com/Athul0491/IceCore/internal/transaction"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MetadataServer struct {
	metadata.UnimplementedMetadataServiceServer

	pgClient *db.PGClient
	locks    *lock.Manager
	mvcc     *transaction.MVCCManager

	catalog    *catalog.CatalogManager
	partitions *catalog.PartitionRegistry
	schemas    *catalog.SchemaStore
}

func defaultString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func New(cfg config.Config) (*MetadataServer, error) {
	pgClient, err := db.NewPGClient(context.Background(), cfg.PGConnString, cfg.PoolSize)
	if err != nil {
		return nil, fmt.Errorf("init pg client: %w", err)
	}

	locks := lock.NewManager()
	mvcc := transaction.NewMVCCManager(cfg.TxnTimeout)

	catalogMgr := catalog.NewCatalogManager(pgClient, locks, mvcc)
	partitionRegistry := catalog.NewPartitionRegistry(pgClient, locks, mvcc, cfg.CacheCapacity)
	schemaStore := catalog.NewSchemaStore(pgClient, locks)

	return &MetadataServer{
		pgClient:   pgClient,
		locks:      locks,
		mvcc:       mvcc,
		catalog:    catalogMgr,
		partitions: partitionRegistry,
		schemas:    schemaStore,
	}, nil
}

func (s *MetadataServer) Close() {
	if s.pgClient != nil {
		s.pgClient.Close()
	}
}

func propertiesToJSON(props map[string]string) string {
	if len(props) == 0 {
		return "{}"
	}
	b, err := json.Marshal(props)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func (s *MetadataServer) CreateTable(ctx context.Context, req *metadata.CreateTableRequest) (*metadata.OperationResponse, error) {
	result := s.catalog.CreateTable(
		ctx,
		req.GetTableName(),
		req.GetSchemaJson(),
		req.GetPartitionSpec(),
		propertiesToJSON(req.GetProperties()),
	)

	return &metadata.OperationResponse{
		Success:  result.Success,
		ErrorMsg: result.ErrorMsg,
	}, nil
}

func (s *MetadataServer) GetTableMetadata(ctx context.Context, req *metadata.TableRequest) (*metadata.TableMetadataResponse, error) {
	tableName := req.GetTableName()
	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	table, err := s.catalog.GetTable(ctx, tableName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if table == nil {
		return nil, status.Error(codes.NotFound, "table not found: "+tableName)
	}

	readSnapshot := req.GetSnapshotId()
	if readSnapshot == 0 {
		if table.CurrentSnapshotID < 0 {
			readSnapshot = 0
		} else {
			readSnapshot = uint64(table.CurrentSnapshotID)
		}
	}

	parts, err := s.partitions.GetPartitions(ctx, tableName, readSnapshot)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if parts == nil {
		parts = []db.PartitionRow{}
	}

	properties := map[string]string{}
	if table.PropertiesJSON != "" {
		if err := json.Unmarshal([]byte(table.PropertiesJSON), &properties); err != nil {
			return nil, status.Error(codes.Internal, "failed to parse table properties JSON")
		}
	}

	resp := &metadata.TableMetadataResponse{
		TableName:         table.TableName,
		SchemaJson:        table.SchemaJSON,
		CurrentSnapshotId: readSnapshot,
		SchemaVersion:     table.SchemaVersion,
		Partitions:        make([]*metadata.PartitionInfo, 0, len(parts)),
		Properties:        properties,
	}

	var totalRows int64
	var totalBytes int64

	for _, p := range parts {
		columnStats := map[string]string{}
		if p.ColumnStatsJSON != "" && p.ColumnStatsJSON != "null" {
			_ = json.Unmarshal([]byte(p.ColumnStatsJSON), &columnStats)
		}

		resp.Partitions = append(resp.Partitions, &metadata.PartitionInfo{
			PartitionKey: p.PartitionKey,
			DataFilePath: p.DataFilePath,
			RowCount:     p.RowCount,
			SizeBytes:    p.SizeBytes,
			SnapshotId:   uint64(p.SnapshotID),
			FileFormat:   p.FileFormat,
			ColumnStats:  columnStats,
		})

		totalRows += p.RowCount
		totalBytes += p.SizeBytes
	}

	resp.TotalRowCount = totalRows
	resp.TotalSizeBytes = totalBytes

	return resp, nil
}

func (s *MetadataServer) AlterTable(ctx context.Context, req *metadata.AlterTableRequest) (*metadata.OperationResponse, error) {
	tableName := req.GetTableName()
	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	switch alt := req.Alteration.(type) {
	case *metadata.AlterTableRequest_NewSchemaJson:
		current, err := s.schemas.GetCurrentSchema(ctx, tableName)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if current != nil {
			if msg := s.schemas.ValidateSchemaChange(current.SchemaJSON, alt.NewSchemaJson); msg != "" {
				return &metadata.OperationResponse{
					Success:  false,
					ErrorMsg: msg,
				}, nil
			}
		}

		result := s.catalog.AlterTableSchema(
			ctx,
			tableName,
			alt.NewSchemaJson,
			"schema evolution via AlterTable",
		)

		s.partitions.InvalidateTableCache(tableName)

		return &metadata.OperationResponse{
			Success:  result.Success,
			ErrorMsg: result.ErrorMsg,
		}, nil

	case *metadata.AlterTableRequest_Rename:
		newName := ""
		if alt.Rename != nil {
			newName = alt.Rename.GetNewName()
		}
		if newName == "" {
			return nil, status.Error(codes.InvalidArgument, "new table name is required")
		}

		result := s.catalog.RenameTable(ctx, tableName, newName)

		s.partitions.InvalidateTableCache(tableName)
		s.partitions.InvalidateTableCache(newName)

		return &metadata.OperationResponse{
			Success:  result.Success,
			ErrorMsg: result.ErrorMsg,
		}, nil

	case *metadata.AlterTableRequest_NewPartitionSpec:
		// still intentionally not implemented in your current design
		return &metadata.OperationResponse{
			Success:  false,
			ErrorMsg: "partition spec update not yet implemented",
		}, nil

	default:
		return &metadata.OperationResponse{
			Success:  false,
			ErrorMsg: "no alteration specified",
		}, nil
	}
}

func (s *MetadataServer) DropTable(ctx context.Context, req *metadata.DropTableRequest) (*metadata.OperationResponse, error) {
	tableName := req.GetTableName()
	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	result := s.catalog.DropTable(ctx, tableName, req.GetPurge())
	if result.Success {
		s.partitions.InvalidateTableCache(tableName)
	}

	return &metadata.OperationResponse{
		Success:  result.Success,
		ErrorMsg: result.ErrorMsg,
	}, nil
}

func (s *MetadataServer) ListTables(ctx context.Context, req *metadata.ListTablesRequest) (*metadata.ListTablesResponse, error) {
	rows, err := s.catalog.ListTables(
		ctx,
		req.GetNamespace(),
		req.GetPageSize(),
		req.GetPageToken(),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &metadata.ListTablesResponse{
		Tables: make([]*metadata.TableSummary, 0, len(rows)),
	}

	for _, t := range rows {
		resp.Tables = append(resp.Tables, &metadata.TableSummary{
			TableName:         t.TableName,
			CurrentSnapshotId: uint64(t.CurrentSnapshotID),
			TotalPartitions:   0, // can be enriched later with a COUNT query if you want
		})
	}

	// offset-style pagination: next token = current offset + returned rows
	if req.GetPageSize() > 0 && len(rows) == int(req.GetPageSize()) {
		currentOffset := int64(0)
		if req.GetPageToken() != "" {
			if parsed, parseErr := strconv.ParseInt(req.GetPageToken(), 10, 64); parseErr == nil {
				currentOffset = parsed
			}
		}
		resp.NextPageToken = strconv.FormatInt(currentOffset+int64(len(rows)), 10)
	}

	return resp, nil
}

func (s *MetadataServer) GetPartitions(ctx context.Context, req *metadata.PartitionRequest) (*metadata.PartitionListResponse, error) {
	var (
		rows []db.PartitionRow
		err  error
	)

	lastID := int64(0)
	if req.GetPageToken() != "" {
		if parsed, parseErr := strconv.ParseInt(req.GetPageToken(), 10, 64); parseErr == nil {
			lastID = parsed
		}
	}

	if req.GetPageSize() > 0 {
		rows, err = s.partitions.GetPartitionsPaged(
			ctx,
			req.GetTableName(),
			req.GetSnapshotId(),
			req.GetPageSize(),
			lastID,
		)
	} else {
		rows, err = s.partitions.GetPartitions(
			ctx,
			req.GetTableName(),
			req.GetSnapshotId(),
		)
	}

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if rows == nil {
		return nil, status.Error(codes.NotFound, "table not found: "+req.GetTableName())
	}

	resp := &metadata.PartitionListResponse{
		Partitions: make([]*metadata.PartitionInfo, 0, len(rows)),
		TotalCount: int64(len(rows)),
	}

	for _, p := range rows {
		resp.Partitions = append(resp.Partitions, &metadata.PartitionInfo{
			PartitionKey: p.PartitionKey,
			DataFilePath: p.DataFilePath,
			RowCount:     p.RowCount,
			SizeBytes:    p.SizeBytes,
			SnapshotId:   uint64(p.SnapshotID),
			FileFormat:   p.FileFormat,
		})
	}

	if req.GetPageSize() > 0 && len(rows) > 0 {
		resp.NextPageToken = strconv.FormatInt(rows[len(rows)-1].PartitionID, 10)
	}

	return resp, nil
}

func (s *MetadataServer) GetPartitionStats(ctx context.Context, req *metadata.PartitionStatsRequest) (*metadata.PartitionStatsResponse, error) {
	tableName := req.GetTableName()
	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	stats, err := s.partitions.GetStats(ctx, tableName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &metadata.PartitionStatsResponse{
		TotalPartitions:       stats.TotalPartitions,
		TotalRows:             stats.TotalRows,
		TotalBytes:            stats.TotalBytes,
		AvgPartitionSizeBytes: stats.AvgSizeBytes,
	}, nil
}

func (s *MetadataServer) CommitSnapshot(ctx context.Context, req *metadata.SnapshotRequest) (*metadata.SnapshotResponse, error) {
	newParts := make([]db.PartitionRow, 0, len(req.GetNewPartitions()))
	for _, p := range req.GetNewPartitions() {
		newParts = append(newParts, db.PartitionRow{
			PartitionKey:    p.GetPartitionKey(),
			DataFilePath:    p.GetDataFilePath(),
			FileFormat:      defaultString(p.GetFileFormat(), "parquet"),
			RowCount:        p.GetRowCount(),
			SizeBytes:       p.GetSizeBytes(),
			ColumnStatsJSON: "{}",
		})
	}

	result := s.partitions.CommitSnapshot(
		ctx,
		req.GetTableName(),
		req.GetParentSnapshotId(),
		defaultString(req.GetOperation(), "append"),
		newParts,
		req.GetDeletedPartitionKeys(),
	)

	return &metadata.SnapshotResponse{
		Success:    result.Success,
		SnapshotId: result.SnapshotID,
		ErrorMsg:   result.ErrorMsg,
	}, nil
}

func (s *MetadataServer) GetSnapshot(ctx context.Context, req *metadata.GetSnapshotRequest) (*metadata.SnapshotDetail, error) {
	snap, err := s.pgClient.GetSnapshot(ctx, req.GetTableName(), req.GetSnapshotId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if snap == nil {
		return nil, status.Error(codes.NotFound, "snapshot not found")
	}

	return &metadata.SnapshotDetail{
		SnapshotId:        uint64(snap.SnapshotID),
		ParentSnapshotId:  uint64(snap.ParentSnapshotID),
		Operation:         snap.Operation,
		AddedPartitions:   int64(snap.AddedFilesCount),
		DeletedPartitions: int64(snap.DeletedFilesCount),
		CommittedAt:       snap.CommittedAt,
	}, nil
}

func (s *MetadataServer) ListSnapshots(ctx context.Context, req *metadata.ListSnapshotsRequest) (*metadata.ListSnapshotsResponse, error) {
	snapshots, err := s.pgClient.ListSnapshots(ctx, req.GetTableName(), req.GetLimit())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &metadata.ListSnapshotsResponse{
		Snapshots: make([]*metadata.SnapshotDetail, 0, len(snapshots)),
	}

	for _, snap := range snapshots {
		resp.Snapshots = append(resp.Snapshots, &metadata.SnapshotDetail{
			SnapshotId:        uint64(snap.SnapshotID),
			ParentSnapshotId:  uint64(snap.ParentSnapshotID),
			Operation:         snap.Operation,
			AddedPartitions:   int64(snap.AddedFilesCount),
			DeletedPartitions: int64(snap.DeletedFilesCount),
			CommittedAt:       snap.CommittedAt,
		})
	}

	return resp, nil
}

func (s *MetadataServer) BeginTransaction(ctx context.Context, req *metadata.TransactionRequest) (*metadata.TransactionResponse, error) {
	clientID := req.GetClientId()
	tableName := req.GetTableName()

	if clientID == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id is required")
	}
	if tableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	table, err := s.catalog.GetTable(ctx, tableName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if table == nil {
		return nil, status.Error(codes.NotFound, "table not found: "+tableName)
	}

	readSnapshot := uint64(0)
	if table.CurrentSnapshotID > 0 {
		readSnapshot = uint64(table.CurrentSnapshotID)
	}

	isolation := "snapshot"
	if req.GetIsolation() == metadata.IsolationLevel_READ_COMMITTED {
		isolation = "read_committed"
	}

	txnID, err := s.pgClient.InsertTransaction(
		ctx,
		clientID,
		table.TableID,
		readSnapshot,
		isolation,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.mvcc.RegisterTransaction(txnID, readSnapshot)

	return &metadata.TransactionResponse{
		TxnId:          txnID,
		ReadSnapshotId: readSnapshot,
	}, nil
}

func (s *MetadataServer) CommitTransaction(ctx context.Context, req *metadata.CommitRequest) (*metadata.OperationResponse, error) {
	ok := s.mvcc.CommitTransaction(req.GetTxnId())
	if !ok {
		return &metadata.OperationResponse{
			Success:  false,
			ErrorMsg: "transaction not found or expired",
		}, nil
	}

	if err := s.pgClient.UpdateTransactionStatus(ctx, req.GetTxnId(), "committed"); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &metadata.OperationResponse{
		Success: true,
	}, nil
}

func (s *MetadataServer) AbortTransaction(ctx context.Context, req *metadata.AbortRequest) (*metadata.OperationResponse, error) {
	s.mvcc.AbortTransaction(req.GetTxnId())

	if err := s.pgClient.UpdateTransactionStatus(ctx, req.GetTxnId(), "aborted"); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &metadata.OperationResponse{
		Success: true,
	}, nil
}
