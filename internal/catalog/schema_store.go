package catalog

import (
	"context"

	"github.com/Athul0491/IceCore/internal/db"
	"github.com/Athul0491/IceCore/internal/lock"
)

type SchemaVersion struct {
	Version       int32
	SchemaJSON    string
	ChangedAt     string
	ChangeSummary string
}

type SchemaStore struct {
	pg    *db.PGClient
	locks *lock.Manager
}

func NewSchemaStore(pg *db.PGClient, locks *lock.Manager) *SchemaStore {
	return &SchemaStore{
		pg:    pg,
		locks: locks,
	}
}

// Get current schema (from tables table)
func (s *SchemaStore) GetCurrentSchema(
	ctx context.Context,
	tableName string,
) (*SchemaVersion, error) {
	unlock := s.locks.LockShared(tableName)
	defer unlock()

	table, err := s.pg.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, nil
	}

	return &SchemaVersion{
		Version:       table.SchemaVersion,
		SchemaJSON:    table.SchemaJSON,
		ChangedAt:     "",
		ChangeSummary: "current",
	}, nil
}

// Get schema at a specific version
func (s *SchemaStore) GetSchemaAtVersion(
	ctx context.Context,
	tableName string,
	version int32,
) (*SchemaVersion, error) {
	unlock := s.locks.LockShared(tableName)
	defer unlock()

	tableID, err := s.pg.GetTableID(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if tableID < 0 {
		return nil, nil
	}

	row := s.pg.Pool.QueryRow(
		ctx,
		`SELECT schema_version,
		        schema_json::text,
		        changed_at::text,
		        change_summary
		   FROM schema_history
		  WHERE table_id = $1 AND schema_version = $2`,
		tableID, version,
	)

	var sv SchemaVersion
	err = row.Scan(
		&sv.Version,
		&sv.SchemaJSON,
		&sv.ChangedAt,
		&sv.ChangeSummary,
	)
	if err != nil {
		return nil, nil // same behavior as C++
	}

	return &sv, nil
}

// List full schema history (latest first)
func (s *SchemaStore) ListSchemaHistory(
	ctx context.Context,
	tableName string,
) ([]SchemaVersion, error) {
	unlock := s.locks.LockShared(tableName)
	defer unlock()

	tableID, err := s.pg.GetTableID(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if tableID < 0 {
		return []SchemaVersion{}, nil
	}

	rows, err := s.pg.Pool.Query(
		ctx,
		`SELECT schema_version,
		        schema_json::text,
		        changed_at::text,
		        change_summary
		   FROM schema_history
		  WHERE table_id = $1
		  ORDER BY schema_version DESC`,
		tableID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []SchemaVersion

	for rows.Next() {
		var sv SchemaVersion
		if err := rows.Scan(
			&sv.Version,
			&sv.SchemaJSON,
			&sv.ChangedAt,
			&sv.ChangeSummary,
		); err != nil {
			return nil, err
		}
		history = append(history, sv)
	}

	return history, rows.Err()
}

// Simple validation (same as your C++ logic)
func (s *SchemaStore) ValidateSchemaChange(
	currentJSON string,
	proposedJSON string,
) string {
	if proposedJSON == "" || proposedJSON == "{}" {
		return "Proposed schema cannot be empty"
	}

	if proposedJSON[0] != '{' || proposedJSON[len(proposedJSON)-1] != '}' {
		return "Proposed schema must be valid JSON object"
	}

	return ""
}
