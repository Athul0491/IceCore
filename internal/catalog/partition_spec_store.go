package catalog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Athul0491/IceCore/internal/db"
	"github.com/Athul0491/IceCore/internal/lock"
)

type PartitionSpecVersion struct {
	Version       int32
	SpecJSON      string
	ChangedAt     string
	ChangeSummary string
}

type PartitionSpecStore struct {
	pg    *db.PGClient
	locks *lock.Manager
}

func NewPartitionSpecStore(pg *db.PGClient, locks *lock.Manager) *PartitionSpecStore {
	return &PartitionSpecStore{pg: pg, locks: locks}
}

func (s *PartitionSpecStore) GetCurrentSpec(
	ctx context.Context,
	tableName string,
) (*PartitionSpecVersion, error) {
	unlock := s.locks.LockShared(tableName)
	defer unlock()

	table, err := s.pg.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, nil
	}

	return &PartitionSpecVersion{
		Version:       table.PartitionSpecVersion,
		SpecJSON:      table.PartitionSpec,
		ChangedAt:     "",
		ChangeSummary: "current",
	}, nil
}

func (s *PartitionSpecStore) GetSpecAtVersion(
	ctx context.Context,
	tableName string,
	version int32,
) (*PartitionSpecVersion, error) {
	unlock := s.locks.LockShared(tableName)
	defer unlock()

	tableID, err := s.pg.GetTableID(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if tableID < 0 {
		return nil, nil
	}

	row, err := s.pg.GetPartitionSpecByVersion(ctx, tableID, version)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	return &PartitionSpecVersion{
		Version:       row.SpecVersion,
		SpecJSON:      row.SpecJSON,
		ChangedAt:     row.ChangedAt,
		ChangeSummary: row.ChangeSummary,
	}, nil
}

func (s *PartitionSpecStore) ListSpecHistory(
	ctx context.Context,
	tableName string,
) ([]PartitionSpecVersion, error) {
	unlock := s.locks.LockShared(tableName)
	defer unlock()

	tableID, err := s.pg.GetTableID(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if tableID < 0 {
		return []PartitionSpecVersion{}, nil
	}

	rows, err := s.pg.ListPartitionSpecHistory(ctx, tableID)
	if err != nil {
		return nil, err
	}

	result := make([]PartitionSpecVersion, 0, len(rows))
	for _, r := range rows {
		result = append(result, PartitionSpecVersion{
			Version:       r.SpecVersion,
			SpecJSON:      r.SpecJSON,
			ChangedAt:     r.ChangedAt,
			ChangeSummary: r.ChangeSummary,
		})
	}
	return result, nil
}

// ValidateSpecChange checks the proposed spec JSON against the current spec JSON.
// Returns an empty string if valid, or an error message if not.
// currentJSON may be empty (no existing spec).
func (s *PartitionSpecStore) ValidateSpecChange(currentJSON, proposedJSON string) string {
	var proposed PartitionSpec
	if err := json.Unmarshal([]byte(proposedJSON), &proposed); err != nil {
		return "proposed partition spec is not valid JSON: " + err.Error()
	}
	if len(proposed.Fields) == 0 {
		return "proposed partition spec must have at least one field"
	}

	// Validate each field in the proposed spec.
	seenIDs := map[int]bool{}
	for i, f := range proposed.Fields {
		if f.SourceColumn == "" {
			return fmt.Sprintf("field at index %d has empty source_column", i)
		}
		if !validTransforms[f.Transform] {
			return fmt.Sprintf("field %q has unknown transform %q", f.SourceColumn, f.Transform)
		}
		if paramRequired(f.Transform) && (f.TransformParam == nil || *f.TransformParam <= 0) {
			return fmt.Sprintf("field %q transform %q requires a positive transform_param", f.SourceColumn, f.Transform)
		}
		if !paramRequired(f.Transform) && f.TransformParam != nil {
			return fmt.Sprintf("field %q transform %q must not have transform_param", f.SourceColumn, f.Transform)
		}
		if seenIDs[f.FieldID] {
			return fmt.Sprintf("duplicate field_id %d in proposed spec", f.FieldID)
		}
		seenIDs[f.FieldID] = true
	}

	// If there's no current spec, any valid proposed spec is accepted.
	if currentJSON == "" {
		return ""
	}

	var current PartitionSpec
	if err := json.Unmarshal([]byte(currentJSON), &current); err != nil {
		// Current spec is corrupt — allow any valid proposal.
		return ""
	}

	// Find the max field_id in the current spec.
	maxCurrentID := 0
	currentByID := map[int]PartitionField{}
	for _, f := range current.Fields {
		currentByID[f.FieldID] = f
		if f.FieldID > maxCurrentID {
			maxCurrentID = f.FieldID
		}
	}

	// Every existing field must be preserved unchanged.
	for _, cf := range current.Fields {
		pf, ok := seenIDs[cf.FieldID]
		_ = pf
		found := false
		for _, proposed := range proposed.Fields {
			if proposed.FieldID == cf.FieldID {
				found = true
				if proposed.SourceColumn != cf.SourceColumn {
					return fmt.Sprintf("field_id %d: cannot change source_column from %q to %q",
						cf.FieldID, cf.SourceColumn, proposed.SourceColumn)
				}
				if proposed.Transform != cf.Transform {
					return fmt.Sprintf("field_id %d: cannot change transform from %q to %q",
						cf.FieldID, cf.Transform, proposed.Transform)
				}
				break
			}
		}
		if !found {
			return fmt.Sprintf("field_id %d (%q) was removed; partition fields cannot be removed",
				cf.FieldID, cf.SourceColumn)
		}
		_ = ok
	}

	// New fields must have field_ids greater than all existing ones.
	for _, pf := range proposed.Fields {
		if _, exists := currentByID[pf.FieldID]; !exists {
			if pf.FieldID <= maxCurrentID {
				return fmt.Sprintf("new field_id %d must be greater than all existing field_ids (max=%d)",
					pf.FieldID, maxCurrentID)
			}
		}
	}

	return ""
}
