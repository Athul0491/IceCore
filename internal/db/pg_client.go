package db

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGClient struct {
	Pool *pgxpool.Pool
}

func NewPGClient(ctx context.Context, connString string, maxConns int) (*PGClient, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = int32(maxConns)

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return &PGClient{Pool: pool}, nil
}

func (c *PGClient) Close() {
	if c != nil && c.Pool != nil {
		c.Pool.Close()
	}
}

func (c *PGClient) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return c.Pool.BeginTx(ctx, pgx.TxOptions{})
}

func (c *PGClient) CreateTableTx(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	schemaJSON string,
	partitionSpecJSON string,
	propertiesJSON string,
) (int64, bool, error) {
	var tableID int64
	err := tx.QueryRow(
		ctx,
		`INSERT INTO tables (table_name, schema_json, partition_spec, properties)
		 VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb)
		 ON CONFLICT (table_name) DO NOTHING
		 RETURNING table_id`,
		tableName, schemaJSON, partitionSpecJSON, propertiesJSON,
	).Scan(&tableID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return tableID, true, nil
}

func (c *PGClient) GetTable(ctx context.Context, tableName string) (*TableRow, error) {
	row := c.Pool.QueryRow(
		ctx,
		`SELECT table_id, table_name, schema_json::text, schema_version,
		        partition_spec::text, partition_spec_version,
		        current_snapshot_id, properties::text
		   FROM tables
		  WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	)

	var t TableRow
	err := row.Scan(
		&t.TableID,
		&t.TableName,
		&t.SchemaJSON,
		&t.SchemaVersion,
		&t.PartitionSpec,
		&t.PartitionSpecVersion,
		&t.CurrentSnapshotID,
		&t.PropertiesJSON,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &t, nil
}

func (c *PGClient) GetTableID(ctx context.Context, tableName string) (int64, error) {
	row := c.Pool.QueryRow(
		ctx,
		`SELECT table_id
		   FROM tables
		  WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	)

	var tableID int64
	err := row.Scan(&tableID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return -1, nil
		}
		return -1, err
	}

	return tableID, nil
}

func (c *PGClient) UpdateTableSchema(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	newSchemaJSON string,
	newVersion int32,
	changeSummary string,
) error {
	_, err := tx.Exec(
		ctx,
		`UPDATE tables
		    SET schema_json = $1::jsonb,
		        schema_version = $2,
		        updated_at = now()
		  WHERE table_name = $3 AND is_deleted = false`,
		newSchemaJSON, newVersion, tableName,
	)
	if err != nil {
		return err
	}

	var tableID int64
	err = tx.QueryRow(
		ctx,
		`SELECT table_id
		   FROM tables
		  WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	).Scan(&tableID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO schema_history (table_id, schema_version, schema_json, change_summary)
		 VALUES ($1, $2, $3::jsonb, $4)`,
		tableID, newVersion, newSchemaJSON, changeSummary,
	)
	return err
}

func (c *PGClient) RenameTable(ctx context.Context, oldName, newName string) (bool, error) {
	cmd, err := c.Pool.Exec(
		ctx,
		`UPDATE tables
		    SET table_name = $1, updated_at = now()
		  WHERE table_name = $2 AND is_deleted = false`,
		newName, oldName,
	)
	if err != nil {
		return false, err
	}

	return cmd.RowsAffected() > 0, nil
}

func (c *PGClient) DropTable(ctx context.Context, tx pgx.Tx, tableName string, purge bool) (bool, error) {
	if purge {
		var tableID int64
		err := tx.QueryRow(
			ctx,
			`SELECT table_id
			   FROM tables
			  WHERE table_name = $1 AND is_deleted = false`,
			tableName,
		).Scan(&tableID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}

		if _, err := tx.Exec(ctx, `DELETE FROM manifest_files WHERE table_id = $1`, tableID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM manifest_lists WHERE table_id = $1`, tableID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM partitions WHERE table_id = $1`, tableID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM snapshots WHERE table_id = $1`, tableID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_history WHERE table_id = $1`, tableID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM partition_specs WHERE table_id = $1`, tableID); err != nil {
			return false, err
		}

		cmd, err := tx.Exec(ctx, `DELETE FROM tables WHERE table_name = $1`, tableName)
		if err != nil {
			return false, err
		}
		return cmd.RowsAffected() > 0, nil
	}

	cmd, err := tx.Exec(
		ctx,
		`UPDATE tables
		    SET is_deleted = true, updated_at = now()
		  WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	)
	if err != nil {
		return false, err
	}

	return cmd.RowsAffected() > 0, nil
}

func (c *PGClient) ListTables(
	ctx context.Context,
	namespace string,
	pageSize int32,
	pageToken string,
) ([]TableRow, error) {
	_ = namespace // keeping same behavior as C++ for now

	limit := pageSize
	if limit <= 0 {
		limit = 100
	}

	offset := int64(0)
	if pageToken != "" {
		var parsed int64
		parsed, err := strconv.ParseInt(pageToken, 10, 64)
		if err == nil {
			offset = parsed
		}
	}

	rows, err := c.Pool.Query(
		ctx,
		`SELECT table_id, table_name, schema_json::text, schema_version,
		        partition_spec::text, partition_spec_version,
		        current_snapshot_id, properties::text
		   FROM tables
		  WHERE is_deleted = false
		  ORDER BY table_id
		  LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []TableRow
	for rows.Next() {
		var t TableRow
		if err := rows.Scan(
			&t.TableID,
			&t.TableName,
			&t.SchemaJSON,
			&t.SchemaVersion,
			&t.PartitionSpec,
			&t.PartitionSpecVersion,
			&t.CurrentSnapshotID,
			&t.PropertiesJSON,
		); err != nil {
			return nil, err
		}
		result = append(result, t)
	}

	return result, rows.Err()
}

func (c *PGClient) GetCurrentSnapshot(ctx context.Context, tableName string) (uint64, error) {
	row := c.Pool.QueryRow(
		ctx,
		`SELECT current_snapshot_id
		   FROM tables
		  WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	)

	var snapshotID int64
	err := row.Scan(&snapshotID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	if snapshotID < 0 {
		return 0, nil
	}
	return uint64(snapshotID), nil
}

func (c *PGClient) InsertTransaction(
	ctx context.Context,
	clientID string,
	tableID int64,
	readSnapshotID uint64,
	isolation string,
) (uint64, error) {
	row := c.Pool.QueryRow(
		ctx,
		`INSERT INTO transactions (client_id, table_id, read_snapshot_id, isolation_level)
		 VALUES ($1, $2, $3, $4)
		 RETURNING txn_id`,
		clientID, tableID, int64(readSnapshotID), isolation,
	)

	var txnID int64
	if err := row.Scan(&txnID); err != nil {
		return 0, err
	}

	if txnID < 0 {
		return 0, nil
	}
	return uint64(txnID), nil
}

func (c *PGClient) UpdateTransactionStatus(ctx context.Context, txnID uint64, status string) error {
	query := `UPDATE transactions SET status = $1`
	if status == "committed" {
		query += `, committed_at = now()`
	}
	query += ` WHERE txn_id = $2`

	_, err := c.Pool.Exec(ctx, query, status, int64(txnID))
	return err
}

func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

// tiny helper so we don't pull extra parsing utilities for a page token
func pgtypeInt8Scan(s string, out *int64) (int64, error) {
	var v int64
	var sign int64 = 1
	var i int

	if len(s) == 0 {
		*out = 0
		return 0, nil
	}
	if s[0] == '-' {
		sign = -1
		i = 1
	}
	for ; i < len(s); i++ {
		ch := s[i]
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid int64 string")
		}
		v = v*10 + int64(ch-'0')
	}
	v *= sign
	*out = v
	return v, nil
}

func (c *PGClient) QueryPartitions(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
) ([]PartitionRow, error) {
	tableID, err := c.GetTableID(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if tableID < 0 {
		return nil, nil
	}

	rows, err := c.Pool.Query(
		ctx,
		`SELECT partition_id, table_id, snapshot_id, partition_key,
		        data_file_path, file_format, row_count, size_bytes, column_stats::text
		   FROM partitions
		  WHERE table_id = $1
		    AND snapshot_id <= $2
		    AND (is_deleted = false OR deleted_snapshot_id > $2)
		  ORDER BY partition_id`,
		tableID, int64(snapshotID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]PartitionRow, 0)
	for rows.Next() {
		var r PartitionRow
		var statsJSON string
		if err := rows.Scan(
			&r.PartitionID,
			&r.TableID,
			&r.SnapshotID,
			&r.PartitionKey,
			&r.DataFilePath,
			&r.FileFormat,
			&r.RowCount,
			&r.SizeBytes,
			&statsJSON,
		); err != nil {
			return nil, err
		}
		r.ColumnStats = unmarshalColumnStats(statsJSON)
		result = append(result, r)
	}

	return result, rows.Err()
}

func (c *PGClient) QueryPartitionsPaged(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
	pageSize int32,
	lastPartitionID int64,
) ([]PartitionRow, error) {
	tableID, err := c.GetTableID(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if tableID < 0 {
		return nil, nil
	}

	if pageSize <= 0 {
		pageSize = 1000
	}

	rows, err := c.Pool.Query(
		ctx,
		`SELECT partition_id, table_id, snapshot_id, partition_key,
		        data_file_path, file_format, row_count, size_bytes, column_stats::text
		   FROM partitions
		  WHERE table_id = $1
		    AND snapshot_id <= $2
		    AND (is_deleted = false OR deleted_snapshot_id > $2)
		    AND partition_id > $3
		  ORDER BY partition_id
		  LIMIT $4`,
		tableID, int64(snapshotID), lastPartitionID, pageSize,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]PartitionRow, 0)
	for rows.Next() {
		var r PartitionRow
		var statsJSON string
		if err := rows.Scan(
			&r.PartitionID,
			&r.TableID,
			&r.SnapshotID,
			&r.PartitionKey,
			&r.DataFilePath,
			&r.FileFormat,
			&r.RowCount,
			&r.SizeBytes,
			&statsJSON,
		); err != nil {
			return nil, err
		}
		r.ColumnStats = unmarshalColumnStats(statsJSON)
		result = append(result, r)
	}

	return result, rows.Err()
}

func (c *PGClient) InsertSnapshotTx(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	parentSnapshotID uint64,
	operation string,
	addedCount int32,
	deletedCount int32,
) (snapshotID uint64, tableID int64, err error) {
	err = tx.QueryRow(
		ctx,
		`SELECT table_id
		   FROM tables
		  WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	).Scan(&tableID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, nil
		}
		return 0, 0, err
	}

	var rawID int64
	err = tx.QueryRow(
		ctx,
		`INSERT INTO snapshots (
		     table_id, parent_snapshot_id, operation,
		     added_files_count, deleted_files_count
		 )
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING snapshot_id`,
		tableID, int64(parentSnapshotID), operation, addedCount, deletedCount,
	).Scan(&rawID)
	if err != nil {
		return 0, 0, err
	}

	if rawID < 0 {
		return 0, tableID, nil
	}
	return uint64(rawID), tableID, nil
}

func (c *PGClient) InsertManifestFileTx(
	ctx context.Context,
	tx pgx.Tx,
	row ManifestFileRow,
) (int64, error) {
	var id int64
	err := tx.QueryRow(
		ctx,
		`INSERT INTO manifest_files (
		     table_id, snapshot_id, manifest_path, partition_spec_id,
		     added_files_count, deleted_files_count,
		     added_rows_count, deleted_rows_count,
		     partition_summaries
		 )
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
		 RETURNING manifest_file_id`,
		row.TableID,
		row.SnapshotID,
		row.ManifestPath,
		row.PartitionSpecID,
		row.AddedFilesCount,
		row.DeletedFilesCount,
		row.AddedRowsCount,
		row.DeletedRowsCount,
		row.PartitionSummaries,
	).Scan(&id)
	return id, err
}

func (c *PGClient) InsertManifestListTx(
	ctx context.Context,
	tx pgx.Tx,
	snapshotID int64,
	tableID int64,
	manifestCount int32,
) (int64, error) {
	var id int64
	err := tx.QueryRow(
		ctx,
		`INSERT INTO manifest_lists (snapshot_id, table_id, manifest_count)
		 VALUES ($1, $2, $3)
		 RETURNING manifest_list_id`,
		snapshotID, tableID, manifestCount,
	).Scan(&id)
	return id, err
}

func (c *PGClient) GetManifestList(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
) (*ManifestListRow, []ManifestFileRow, error) {
	tableID, err := c.GetTableID(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}
	if tableID < 0 {
		return nil, nil, nil
	}

	var list ManifestListRow
	err = c.Pool.QueryRow(
		ctx,
		`SELECT manifest_list_id, snapshot_id, table_id, manifest_count
		   FROM manifest_lists
		  WHERE snapshot_id = $1 AND table_id = $2`,
		int64(snapshotID), tableID,
	).Scan(&list.ManifestListID, &list.SnapshotID, &list.TableID, &list.ManifestCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	rows, err := c.Pool.Query(
		ctx,
		`SELECT manifest_file_id, table_id, snapshot_id, manifest_path,
		        partition_spec_id, added_files_count, deleted_files_count,
		        added_rows_count, deleted_rows_count, partition_summaries::text
		   FROM manifest_files
		  WHERE snapshot_id = $1
		  ORDER BY manifest_file_id`,
		int64(snapshotID),
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var files []ManifestFileRow
	for rows.Next() {
		var f ManifestFileRow
		if err := rows.Scan(
			&f.ManifestFileID,
			&f.TableID,
			&f.SnapshotID,
			&f.ManifestPath,
			&f.PartitionSpecID,
			&f.AddedFilesCount,
			&f.DeletedFilesCount,
			&f.AddedRowsCount,
			&f.DeletedRowsCount,
			&f.PartitionSummaries,
		); err != nil {
			return nil, nil, err
		}
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return &list, files, nil
}

func (c *PGClient) GetManifestFile(
	ctx context.Context,
	tableName string,
	manifestFileID int64,
) (*ManifestFileRow, error) {
	var f ManifestFileRow
	err := c.Pool.QueryRow(
		ctx,
		`SELECT mf.manifest_file_id, mf.table_id, mf.snapshot_id, mf.manifest_path,
		        mf.partition_spec_id, mf.added_files_count, mf.deleted_files_count,
		        mf.added_rows_count, mf.deleted_rows_count, mf.partition_summaries::text
		   FROM manifest_files mf
		   JOIN tables t ON t.table_id = mf.table_id
		  WHERE mf.manifest_file_id = $1
		    AND t.table_name = $2
		    AND t.is_deleted = false`,
		manifestFileID, tableName,
	).Scan(
		&f.ManifestFileID,
		&f.TableID,
		&f.SnapshotID,
		&f.ManifestPath,
		&f.PartitionSpecID,
		&f.AddedFilesCount,
		&f.DeletedFilesCount,
		&f.AddedRowsCount,
		&f.DeletedRowsCount,
		&f.PartitionSummaries,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &f, nil
}

func (c *PGClient) InsertPartitionTx(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	snapshotID uint64,
	part PartitionRow,
) error {
	var tableID int64
	err := tx.QueryRow(
		ctx,
		`SELECT table_id
		   FROM tables
		  WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	).Scan(&tableID)
	if err != nil {
		return err
	}

	statsJSON := marshalColumnStats(part.ColumnStats)

	_, err = tx.Exec(
		ctx,
		`INSERT INTO partitions (
		     table_id, snapshot_id, partition_key, data_file_path,
		     file_format, row_count, size_bytes, column_stats
		 )
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)`,
		tableID,
		int64(snapshotID),
		part.PartitionKey,
		part.DataFilePath,
		part.FileFormat,
		part.RowCount,
		part.SizeBytes,
		statsJSON,
	)
	return err
}

// GetColumnStatsForSnapshot returns per-column bounds aggregated across all
// data files visible at the given snapshot.
func (c *PGClient) GetColumnStatsForSnapshot(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
) ([]ColumnStats, error) {
	rows, err := c.QueryPartitions(ctx, tableName, snapshotID)
	if err != nil {
		return nil, err
	}

	type agg struct {
		nullCount  int64
		nanCount   int64
		valueCount int64
		minValue   string
		maxValue   string
		hasMin     bool
	}
	byCol := map[int32]*agg{}

	for _, p := range rows {
		for _, cs := range p.ColumnStats {
			a, ok := byCol[cs.ColumnID]
			if !ok {
				a = &agg{}
				byCol[cs.ColumnID] = a
			}
			a.nullCount += cs.NullCount
			a.nanCount += cs.NaNCount
			a.valueCount += cs.ValueCount
			if cs.MinValue != "" {
				if !a.hasMin || cs.MinValue < a.minValue {
					a.minValue = cs.MinValue
					a.hasMin = true
				}
			}
			if cs.MaxValue != "" && cs.MaxValue > a.maxValue {
				a.maxValue = cs.MaxValue
			}
		}
	}

	result := make([]ColumnStats, 0, len(byCol))
	for colID, a := range byCol {
		result = append(result, ColumnStats{
			ColumnID:   colID,
			NullCount:  a.nullCount,
			NaNCount:   a.nanCount,
			ValueCount: a.valueCount,
			MinValue:   a.minValue,
			MaxValue:   a.maxValue,
		})
	}
	return result, nil
}

func marshalColumnStats(stats []ColumnStats) string {
	if len(stats) == 0 {
		return "[]"
	}
	b, err := json.Marshal(stats)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func unmarshalColumnStats(raw string) []ColumnStats {
	if raw == "" || raw == "null" || raw == "{}" {
		return nil
	}
	var stats []ColumnStats
	if err := json.Unmarshal([]byte(raw), &stats); err != nil {
		return nil
	}
	return stats
}

func (c *PGClient) MarkPartitionDeletedTx(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	partitionKey string,
	deletedSnapshotID uint64,
) error {
	var tableID int64
	err := tx.QueryRow(
		ctx,
		`SELECT table_id
		   FROM tables
		  WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	).Scan(&tableID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		ctx,
		`UPDATE partitions
		    SET is_deleted = true,
		        deleted_snapshot_id = $1
		  WHERE table_id = $2
		    AND partition_key = $3
		    AND is_deleted = false`,
		int64(deletedSnapshotID), tableID, partitionKey,
	)
	return err
}

func (c *PGClient) UpdateTableSnapshotTx(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	snapshotID uint64,
) error {
	_, err := tx.Exec(
		ctx,
		`UPDATE tables
		    SET current_snapshot_id = $1,
		        updated_at = now()
		  WHERE table_name = $2
		    AND is_deleted = false`,
		int64(snapshotID), tableName,
	)
	return err
}

func (c *PGClient) GetSnapshot(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
) (*SnapshotRow, error) {
	tableID, err := c.GetTableID(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if tableID < 0 {
		return nil, nil
	}

	row := c.Pool.QueryRow(
		ctx,
		`SELECT snapshot_id,
		        table_id,
		        parent_snapshot_id,
		        operation,
		        added_files_count,
		        deleted_files_count,
		        committed_at::text
		   FROM snapshots
		  WHERE table_id = $1
		    AND snapshot_id = $2`,
		tableID, int64(snapshotID),
	)

	var s SnapshotRow
	err = row.Scan(
		&s.SnapshotID,
		&s.TableID,
		&s.ParentSnapshotID,
		&s.Operation,
		&s.AddedFilesCount,
		&s.DeletedFilesCount,
		&s.CommittedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &s, nil
}

func (c *PGClient) ListSnapshots(
	ctx context.Context,
	tableName string,
	limit int32,
) ([]SnapshotRow, error) {
	tableID, err := c.GetTableID(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if tableID < 0 {
		return []SnapshotRow{}, nil
	}

	if limit <= 0 {
		limit = 50
	}

	rows, err := c.Pool.Query(
		ctx,
		`SELECT snapshot_id,
		        table_id,
		        parent_snapshot_id,
		        operation,
		        added_files_count,
		        deleted_files_count,
		        committed_at::text
		   FROM snapshots
		  WHERE table_id = $1
		  ORDER BY committed_at DESC
		  LIMIT $2`,
		tableID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]SnapshotRow, 0)
	for rows.Next() {
		var s SnapshotRow
		if err := rows.Scan(
			&s.SnapshotID,
			&s.TableID,
			&s.ParentSnapshotID,
			&s.Operation,
			&s.AddedFilesCount,
			&s.DeletedFilesCount,
			&s.CommittedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, s)
	}

	return result, rows.Err()
}

func (c *PGClient) InsertPartitionSpecTx(
	ctx context.Context,
	tx pgx.Tx,
	tableID int64,
	specVersion int32,
	specJSON string,
	changeSummary string,
) error {
	_, err := tx.Exec(
		ctx,
		`INSERT INTO partition_specs (table_id, spec_version, spec_json, change_summary)
		 VALUES ($1, $2, $3::jsonb, $4)`,
		tableID, specVersion, specJSON, changeSummary,
	)
	return err
}

func (c *PGClient) UpdateTablePartitionSpecTx(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	newSpecJSON string,
	newVersion int32,
) error {
	_, err := tx.Exec(
		ctx,
		`UPDATE tables
		    SET partition_spec = $1::jsonb,
		        partition_spec_version = $2,
		        updated_at = now()
		  WHERE table_name = $3 AND is_deleted = false`,
		newSpecJSON, newVersion, tableName,
	)
	return err
}

func (c *PGClient) GetPartitionSpecByVersion(
	ctx context.Context,
	tableID int64,
	specVersion int32,
) (*PartitionSpecRow, error) {
	var r PartitionSpecRow
	err := c.Pool.QueryRow(
		ctx,
		`SELECT partition_spec_id, table_id, spec_version,
		        spec_json::text, changed_at::text, COALESCE(change_summary, '')
		   FROM partition_specs
		  WHERE table_id = $1 AND spec_version = $2`,
		tableID, specVersion,
	).Scan(&r.PartitionSpecID, &r.TableID, &r.SpecVersion, &r.SpecJSON, &r.ChangedAt, &r.ChangeSummary)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

func (c *PGClient) ListPartitionSpecHistory(
	ctx context.Context,
	tableID int64,
) ([]PartitionSpecRow, error) {
	rows, err := c.Pool.Query(
		ctx,
		`SELECT partition_spec_id, table_id, spec_version,
		        spec_json::text, changed_at::text, COALESCE(change_summary, '')
		   FROM partition_specs
		  WHERE table_id = $1
		  ORDER BY spec_version DESC`,
		tableID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []PartitionSpecRow
	for rows.Next() {
		var r PartitionSpecRow
		if err := rows.Scan(
			&r.PartitionSpecID, &r.TableID, &r.SpecVersion,
			&r.SpecJSON, &r.ChangedAt, &r.ChangeSummary,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (c *PGClient) GetTableSpecVersion(ctx context.Context, tableName string) (int32, error) {
	var v int32
	err := c.Pool.QueryRow(
		ctx,
		`SELECT partition_spec_version FROM tables WHERE table_name = $1 AND is_deleted = false`,
		tableName,
	).Scan(&v)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

func (c *PGClient) GetVisiblePartitionCount(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
) (int64, error) {
	tableID, err := c.GetTableID(ctx, tableName)
	if err != nil {
		return 0, err
	}
	if tableID < 0 {
		return 0, nil
	}

	row := c.Pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		   FROM partitions
		  WHERE table_id = $1
		    AND snapshot_id <= $2
		    AND (is_deleted = false OR deleted_snapshot_id > $2)`,
		tableID, int64(snapshotID),
	)

	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}
