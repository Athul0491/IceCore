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
		pgClient:    pgClient,
		locks:       locks,
		mvcc:        mvcc,
		catalog:     catalogMgr,
		partitions:  partitionRegistry,
		schemas:     schemaStore,
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
	table, err := s.catalog.GetTable(ctx, req.GetTableName())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if table == nil {
		return nil, status.Error(codes.NotFound, "table not found: "+req.GetTableName())
	}

	// get current snapshot id
	currentSnapshot := table.CurrentSnapshotID

	// fetch partitions
	parts, err := s.partitions.GetPartitions(ctx, req.GetTableName(), currentSnapshot)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &metadata.TableMetadataResponse{
		TableName:      table.TableName,
		SchemaJson:     table.SchemaJSON,
		SchemaVersion:  table.SchemaVersion,
		SnapshotId:     currentSnapshot,
		Partitions:     make([]*metadata.PartitionInfo, 0),
	}

	var totalRows int64
	var totalBytes int64

	for _, p := range parts {
		resp.Partitions = append(resp.Partitions, &metadata.PartitionInfo{
			PartitionKey: p.PartitionKey,
			DataFilePath: p.DataFilePath,
			RowCount:     p.RowCount,
			SizeBytes:    p.SizeBytes,
			SnapshotId:   uint64(p.SnapshotID),
			FileFormat:   p.FileFormat,
		})

		totalRows += p.RowCount
		totalBytes += p.SizeBytes
	}

	resp.TotalRows = totalRows
	resp.TotalBytes = totalBytes

	return resp, nil
}

func (s *MetadataServer) AlterTable(ctx context.Context, req *metadata.AlterTableRequest) (*metadata.OperationResponse, error) {
	return nil, status.Error(codes.Unimplemented, "AlterTable not implemented yet")
}

func (s *MetadataServer) DropTable(ctx context.Context, req *metadata.DropTableRequest) (*metadata.OperationResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DropTable not implemented yet")
}

func (s *MetadataServer) ListTables(ctx context.Context, req *metadata.ListTablesRequest) (*metadata.ListTablesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListTables not implemented yet")
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
	stats, err := s.partitions.GetStats(ctx, req.GetTableName())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &metadata.PartitionStatsResponse{
		TotalPartitions:      stats.TotalPartitions,
		TotalRows:            stats.TotalRows,
		TotalBytes:           stats.TotalBytes,
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
		Success:   result.Success,
		SnapshotId: result.SnapshotID,
		ErrorMsg:  result.ErrorMsg,
	}, nil
}

func (s *MetadataServer) GetSnapshot(ctx context.Context, req *metadata.GetSnapshotRequest) (*metadata.SnapshotDetail, error) {
	return nil, status.Error(codes.Unimplemented, "GetSnapshot not implemented yet")
}

func (s *MetadataServer) ListSnapshots(ctx context.Context, req *metadata.ListSnapshotsRequest) (*metadata.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListSnapshots not implemented yet")
}

func (s *MetadataServer) BeginTransaction(ctx context.Context, req *metadata.TransactionRequest) (*metadata.TransactionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "BeginTransaction not implemented yet")
}

func (s *MetadataServer) CommitTransaction(ctx context.Context, req *metadata.CommitRequest) (*metadata.OperationResponse, error) {
	return nil, status.Error(codes.Unimplemented, "CommitTransaction not implemented yet")
}

func (s *MetadataServer) AbortTransaction(ctx context.Context, req *metadata.AbortRequest) (*metadata.OperationResponse, error) {
	return nil, status.Error(codes.Unimplemented, "AbortTransaction not implemented yet")
}