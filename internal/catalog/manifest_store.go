package catalog

import (
	"context"

	"github.com/Athul0491/IceCore/internal/db"
)

type ManifestStore struct {
	pg *db.PGClient
}

func NewManifestStore(pg *db.PGClient) *ManifestStore {
	return &ManifestStore{pg: pg}
}

func (m *ManifestStore) GetManifestList(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
) (*db.ManifestListRow, []db.ManifestFileRow, error) {
	return m.pg.GetManifestList(ctx, tableName, snapshotID)
}

func (m *ManifestStore) GetManifestFile(
	ctx context.Context,
	tableName string,
	manifestFileID int64,
) (*db.ManifestFileRow, error) {
	return m.pg.GetManifestFile(ctx, tableName, manifestFileID)
}
