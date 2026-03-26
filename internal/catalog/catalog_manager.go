package catalog

import (
	"context"

	"github.com/Athul0491/IceCore/internal/db"
	"github.com/Athul0491/IceCore/internal/lock"
	"github.com/Athul0491/IceCore/internal/transaction"
)

type CreateTableResult struct {
	Success  bool
	ErrorMsg string
}

type AlterResult struct {
	Success  bool
	ErrorMsg string
}

type DropResult struct {
	Success  bool
	ErrorMsg string
}

type CatalogManager struct {
	pg    *db.PGClient
	locks *lock.Manager
	mvcc  *transaction.MVCCManager
}

func NewCatalogManager(pg *db.PGClient, locks *lock.Manager, mvcc *transaction.MVCCManager) *CatalogManager {
	return &CatalogManager{
		pg:    pg,
		locks: locks,
		mvcc:  mvcc,
	}
}

func (m *CatalogManager) CreateTable(
	ctx context.Context,
	tableName string,
	schemaJSON string,
	partitionSpec string,
	propertiesJSON string,
) CreateTableResult {
	unlock := m.locks.LockExclusive(tableName)
	defer unlock()

	ok, err := m.pg.CreateTable(ctx, tableName, schemaJSON, partitionSpec, propertiesJSON)
	if err != nil {
		return CreateTableResult{
			Success:  false,
			ErrorMsg: "failed to create table: " + err.Error(),
		}
	}
	if !ok {
		return CreateTableResult{
			Success:  false,
			ErrorMsg: "failed to create table (may already exist): " + tableName,
		}
	}

	return CreateTableResult{
		Success: true,
	}
}

func (m *CatalogManager) GetTable(ctx context.Context, tableName string) (*db.TableRow, error) {
	unlock := m.locks.LockShared(tableName)
	defer unlock()

	return m.pg.GetTable(ctx, tableName)
}

func (m *CatalogManager) AlterTableSchema(
	ctx context.Context,
	tableName string,
	newSchemaJSON string,
	changeSummary string,
) AlterResult {
	unlock := m.locks.LockExclusive(tableName)
	defer unlock()

	existing, err := m.pg.GetTable(ctx, tableName)
	if err != nil {
		return AlterResult{Success: false, ErrorMsg: err.Error()}
	}
	if existing == nil {
		return AlterResult{Success: false, ErrorMsg: "table not found: " + tableName}
	}

	tx, err := m.pg.BeginTx(ctx)
	if err != nil {
		return AlterResult{Success: false, ErrorMsg: err.Error()}
	}
	defer tx.Rollback(ctx)

	newVersion := existing.SchemaVersion + 1
	if err := m.pg.UpdateTableSchema(ctx, tx, tableName, newSchemaJSON, newVersion, changeSummary); err != nil {
		return AlterResult{Success: false, ErrorMsg: "failed to update schema for: " + tableName}
	}

	if err := tx.Commit(ctx); err != nil {
		return AlterResult{Success: false, ErrorMsg: err.Error()}
	}

	return AlterResult{Success: true}
}

func (m *CatalogManager) RenameTable(ctx context.Context, oldName, newName string) AlterResult {
	unlockOld := m.locks.LockExclusive(oldName)
	defer unlockOld()

	unlockNew := m.locks.LockExclusive(newName)
	defer unlockNew()

	ok, err := m.pg.RenameTable(ctx, oldName, newName)
	if err != nil {
		return AlterResult{Success: false, ErrorMsg: err.Error()}
	}
	if !ok {
		return AlterResult{Success: false, ErrorMsg: "failed to rename " + oldName + " to " + newName}
	}

	return AlterResult{Success: true}
}

func (m *CatalogManager) DropTable(ctx context.Context, tableName string, purge bool) DropResult {
	unlock := m.locks.LockExclusive(tableName)
	defer unlock()

	tx, err := m.pg.BeginTx(ctx)
	if err != nil {
		return DropResult{Success: false, ErrorMsg: err.Error()}
	}
	defer tx.Rollback(ctx)

	ok, err := m.pg.DropTable(ctx, tx, tableName, purge)
	if err != nil {
		return DropResult{Success: false, ErrorMsg: err.Error()}
	}
	if !ok {
		return DropResult{Success: false, ErrorMsg: "failed to drop table: " + tableName}
	}

	if err := tx.Commit(ctx); err != nil {
		return DropResult{Success: false, ErrorMsg: err.Error()}
	}

	return DropResult{Success: true}
}

func (m *CatalogManager) ListTables(
	ctx context.Context,
	namespace string,
	pageSize int32,
	pageToken string,
) ([]db.TableRow, error) {
	return m.pg.ListTables(ctx, namespace, pageSize, pageToken)
}
