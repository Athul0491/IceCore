package catalog

import (
	"github.com/Athul0491/IceCore/internal/db"
	"github.com/Athul0491/IceCore/internal/lock"
	"github.com/Athul0491/IceCore/internal/transaction"
)

type PartitionRegistry struct {
	pg    *db.PGClient
	locks *lock.Manager
	mvcc  *transaction.MVCCManager
}

func NewPartitionRegistry(pg *db.PGClient, locks *lock.Manager, mvcc *transaction.MVCCManager, cacheCapacity int) *PartitionRegistry {
	return &PartitionRegistry{
		pg:    pg,
		locks: locks,
		mvcc:  mvcc,
	}
}