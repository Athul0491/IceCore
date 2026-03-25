package db

import (
	"context"
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

func (c *PGClient) CreateTable(
	ctx context.Context,
	tableName string,
	schemaJSON string,
	partitionSpec string,
	propertiesJSON string,
) (bool, error) {
	cmd, err := c.Pool.Exec(
		ctx,
		`INSERT INTO tables (table_name, schema_json, partition_spec, properties)
		 VALUES ($1, $2::jsonb, $3, $4::jsonb)
		 ON CONFLICT (table_name) DO NOTHING`,
		tableName, schemaJSON, partitionSpec, propertiesJSON,
	)
	if err != nil {
		return false, err
	}

	return cmd.RowsAffected() > 0, nil
}

func (c *PGClient) GetTable(ctx context.Context, tableName string) (*TableRow, error) {
	row := c.Pool.QueryRow(
		ctx,
		`SELECT table_id, table_name, schema_json::text, schema_version,
		        partition_spec, current_snapshot_id, properties::text
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

		if _, err := tx.Exec(ctx, `DELETE FROM partitions WHERE table_id = $1`, tableID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM snapshots WHERE table_id = $1`, tableID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_history WHERE table_id = $1`, tableID); err != nil {
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
		        partition_spec, current_snapshot_id, properties::text
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
	readSnapshotID uint64,
	isolation string,
) (uint64, error) {
	row := c.Pool.QueryRow(
		ctx,
		`INSERT INTO transactions (client_id, read_snapshot_id, isolation_level)
		 VALUES ($1, $2, $3)
		 RETURNING txn_id`,
		clientID, int64(readSnapshotID), isolation,
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
		if err := rows.Scan(
			&r.PartitionID,
			&r.TableID,
			&r.SnapshotID,
			&r.PartitionKey,
			&r.DataFilePath,
			&r.FileFormat,
			&r.RowCount,
			&r.SizeBytes,
			&r.ColumnStatsJSON,
		); err != nil {
			return nil, err
		}
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
		if err := rows.Scan(
			&r.PartitionID,
			&r.TableID,
			&r.SnapshotID,
			&r.PartitionKey,
			&r.DataFilePath,
			&r.FileFormat,
			&r.RowCount,
			&r.SizeBytes,
			&r.ColumnStatsJSON,
		); err != nil {
			return nil, err
		}
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
) (uint64, error) {
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
			return 0, nil
		}
		return 0, err
	}

	var snapshotID int64
	err = tx.QueryRow(
		ctx,
		`INSERT INTO snapshots (
		     table_id, parent_snapshot_id, operation,
		     added_files_count, deleted_files_count
		 )
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING snapshot_id`,
		tableID, int64(parentSnapshotID), operation, addedCount, deletedCount,
	).Scan(&snapshotID)
	if err != nil {
		return 0, err
	}

	if snapshotID < 0 {
		return 0, nil
	}
	return uint64(snapshotID), nil
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

	columnStats := part.ColumnStatsJSON
	if columnStats == "" {
		columnStats = "{}"
	}

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
		columnStats,
	)
	return err
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