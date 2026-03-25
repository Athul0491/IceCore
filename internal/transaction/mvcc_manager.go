package transaction

import (
	"sync"
	"sync/atomic"
	"time"
)

type TransactionState struct {
	TxnID          uint64
	ReadSnapshotID uint64
	StartedAt      time.Time
}

type MVCCManager struct {
	nextTxnID atomic.Uint64

	mu                 sync.RWMutex
	activeTransactions map[uint64]TransactionState
	pinnedSnapshots    map[uint64]int

	txnTimeout time.Duration
}

func NewMVCCManager(timeout time.Duration) *MVCCManager {
	m := &MVCCManager{
		activeTransactions: make(map[uint64]TransactionState),
		pinnedSnapshots:    make(map[uint64]int),
		txnTimeout:         timeout,
	}
	m.nextTxnID.Store(1)
	return m
}

func (m *MVCCManager) BeginTransaction(readSnapshot uint64) (txnID uint64, snapshot uint64) {
	txnID = m.nextTxnID.Add(1) - 1
	snapshot = readSnapshot

	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeTransactions[txnID] = TransactionState{
		TxnID:          txnID,
		ReadSnapshotID: snapshot,
		StartedAt:      time.Now(),
	}
	m.pinnedSnapshots[snapshot]++

	return txnID, snapshot
}

func (m *MVCCManager) CommitTransaction(txnID uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.activeTransactions[txnID]
	if !ok {
		return false
	}

	if time.Since(tx.StartedAt) > m.txnTimeout {
		m.unpinSnapshotLocked(tx.ReadSnapshotID)
		delete(m.activeTransactions, txnID)
		return false
	}

	m.unpinSnapshotLocked(tx.ReadSnapshotID)
	delete(m.activeTransactions, txnID)
	return true
}

func (m *MVCCManager) AbortTransaction(txnID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.activeTransactions[txnID]
	if !ok {
		return
	}

	m.unpinSnapshotLocked(tx.ReadSnapshotID)
	delete(m.activeTransactions, txnID)
}

func (m *MVCCManager) ValidateParentSnapshot(tableName string, parentSnapshotID, currentSnapshotID uint64) bool {
	_ = tableName
	return parentSnapshotID == currentSnapshotID
}

func (m *MVCCManager) CleanupExpiredTransactions() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for txnID, tx := range m.activeTransactions {
		if now.Sub(tx.StartedAt) > m.txnTimeout {
			m.unpinSnapshotLocked(tx.ReadSnapshotID)
			delete(m.activeTransactions, txnID)
			cleaned++
		}
	}

	return cleaned
}

func (m *MVCCManager) ActiveTransactionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.activeTransactions)
}

func (m *MVCCManager) IsSnapshotPinned(snapshotID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pinnedSnapshots[snapshotID] > 0
}

func (m *MVCCManager) unpinSnapshotLocked(snapshotID uint64) {
	count := m.pinnedSnapshots[snapshotID]
	if count <= 1 {
		delete(m.pinnedSnapshots, snapshotID)
		return
	}
	m.pinnedSnapshots[snapshotID] = count - 1
}