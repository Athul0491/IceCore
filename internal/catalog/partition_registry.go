package catalog

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Athul0491/IceCore/internal/cache"
	"github.com/Athul0491/IceCore/internal/db"
	"github.com/Athul0491/IceCore/internal/lock"
	"github.com/Athul0491/IceCore/internal/transaction"
)

type PartitionStats struct {
	TotalPartitions int64
	TotalRows       int64
	TotalBytes      int64
	AvgSizeBytes    int64
}

type CommitResult struct {
	Success    bool
	SnapshotID uint64
	ErrorMsg   string
}

type PartitionRegistry struct {
	pg    *db.PGClient
	locks *lock.Manager
	mvcc  *transaction.MVCCManager
	cache *cache.LRU[string, []db.PartitionRow]
}

func NewPartitionRegistry(
	pg *db.PGClient,
	locks *lock.Manager,
	mvcc *transaction.MVCCManager,
	cacheCapacity int,
) *PartitionRegistry {
	return &PartitionRegistry{
		pg:    pg,
		locks: locks,
		mvcc:  mvcc,
		cache: cache.NewLRU[string, []db.PartitionRow](cacheCapacity),
	}
}

func (r *PartitionRegistry) GetPartitions(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
) ([]db.PartitionRow, error) {
	unlock := r.locks.LockShared(tableName)
	defer unlock()

	readSnap, err := r.resolveSnapshot(ctx, tableName, snapshotID)
	if err != nil {
		return nil, err
	}

	key := r.makeCacheKey(tableName, readSnap)
	if cached, ok := r.cache.Get(key); ok {
		return cached, nil
	}

	rows, err := r.pg.QueryPartitions(ctx, tableName, readSnap)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}

	r.cache.Put(key, rows)
	return rows, nil
}

func (r *PartitionRegistry) GetPartitionsPaged(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
	pageSize int32,
	lastPartitionID int64,
) ([]db.PartitionRow, error) {
	unlock := r.locks.LockShared(tableName)
	defer unlock()

	readSnap, err := r.resolveSnapshot(ctx, tableName, snapshotID)
	if err != nil {
		return nil, err
	}

	rows, err := r.pg.QueryPartitionsPaged(ctx, tableName, readSnap, pageSize, lastPartitionID)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}

	return rows, nil
}

func (r *PartitionRegistry) GetStats(
	ctx context.Context,
	tableName string,
) (PartitionStats, error) {
	unlock := r.locks.LockShared(tableName)
	defer unlock()

	readSnap, err := r.resolveSnapshot(ctx, tableName, 0)
	if err != nil {
		return PartitionStats{}, err
	}

	rows, err := r.pg.QueryPartitions(ctx, tableName, readSnap)
	if err != nil {
		return PartitionStats{}, err
	}
	if rows == nil {
		return PartitionStats{}, nil
	}

	stats := PartitionStats{
		TotalPartitions: int64(len(rows)),
	}
	for _, p := range rows {
		stats.TotalRows += p.RowCount
		stats.TotalBytes += p.SizeBytes
	}
	if stats.TotalPartitions > 0 {
		stats.AvgSizeBytes = stats.TotalBytes / stats.TotalPartitions
	}

	return stats, nil
}

func (r *PartitionRegistry) CommitSnapshot(
	ctx context.Context,
	tableName string,
	parentSnapshotID uint64,
	operation string,
	newPartitions []db.PartitionRow,
	deletedPartitionKeys []string,
) CommitResult {
	unlock := r.locks.LockExclusive(tableName)
	defer unlock()

	currentSnap, err := r.pg.GetCurrentSnapshot(ctx, tableName)
	if err != nil {
		return CommitResult{
			Success:  false,
			ErrorMsg: err.Error(),
		}
	}

	if !r.mvcc.ValidateParentSnapshot(tableName, parentSnapshotID, currentSnap) {
		return CommitResult{
			Success: false,
			ErrorMsg: fmt.Sprintf(
				"conflict: snapshot %d is no longer current (current=%d)",
				parentSnapshotID,
				currentSnap,
			),
		}
	}

	tx, err := r.pg.BeginTx(ctx)
	if err != nil {
		return CommitResult{
			Success:  false,
			ErrorMsg: err.Error(),
		}
	}
	defer tx.Rollback(ctx)

	newSnap, err := r.pg.InsertSnapshotTx(
		ctx,
		tx,
		tableName,
		parentSnapshotID,
		operation,
		int32(len(newPartitions)),
		int32(len(deletedPartitionKeys)),
	)
	if err != nil || newSnap == 0 {
		msg := "failed to insert snapshot record"
		if err != nil {
			msg = err.Error()
		}
		return CommitResult{
			Success:  false,
			ErrorMsg: msg,
		}
	}

	for _, part := range newPartitions {
		if err := r.pg.InsertPartitionTx(ctx, tx, tableName, newSnap, part); err != nil {
			return CommitResult{
				Success:  false,
				ErrorMsg: "failed to insert partition: " + part.PartitionKey,
			}
		}
	}

	for _, key := range deletedPartitionKeys {
		if err := r.pg.MarkPartitionDeletedTx(ctx, tx, tableName, key, newSnap); err != nil {
			return CommitResult{
				Success:  false,
				ErrorMsg: "failed to mark partition deleted: " + key,
			}
		}
	}

	if err := r.pg.UpdateTableSnapshotTx(ctx, tx, tableName, newSnap); err != nil {
		return CommitResult{
			Success:  false,
			ErrorMsg: "failed to update table snapshot pointer",
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return CommitResult{
			Success:  false,
			ErrorMsg: err.Error(),
		}
	}

	r.InvalidateTableCache(tableName)

	return CommitResult{
		Success:    true,
		SnapshotID: newSnap,
	}
}

func (r *PartitionRegistry) InvalidateTableCache(tableName string) int {
	prefix := tableName + ":"
	return r.cache.InvalidateIf(func(key string) bool {
		return len(key) >= len(prefix) && key[:len(prefix)] == prefix
	})
}

func (r *PartitionRegistry) CacheHitRate() float64 {
	return r.cache.HitRate()
}

func (r *PartitionRegistry) CacheSize() int {
	return r.cache.Size()
}

func (r *PartitionRegistry) CacheHits() uint64 {
	return r.cache.Hits()
}

func (r *PartitionRegistry) CacheMisses() uint64 {
	return r.cache.Misses()
}

func (r *PartitionRegistry) resolveSnapshot(
	ctx context.Context,
	tableName string,
	snapshotID uint64,
) (uint64, error) {
	if snapshotID != 0 {
		return snapshotID, nil
	}

	// For correctness, use DB table pointer as source of truth.
	// This avoids the global in-memory snapshot mismatch from the C++ version.
	current, err := r.pg.GetCurrentSnapshot(ctx, tableName)
	if err != nil {
		return 0, err
	}
	return current, nil
}

func (r *PartitionRegistry) makeCacheKey(table string, snapshot uint64) string {
	return table + ":" + strconv.FormatUint(snapshot, 10)
}
