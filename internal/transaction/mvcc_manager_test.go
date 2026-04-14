package transaction

import (
	"testing"
	"time"
)

func TestRegisterCommitAndAbortTransactions(t *testing.T) {
	manager := NewMVCCManager(time.Minute)

	manager.RegisterTransaction(1, 10)
	if manager.ActiveTransactionCount() != 1 {
		t.Fatalf("expected 1 active transaction, got %d", manager.ActiveTransactionCount())
	}
	if !manager.IsSnapshotPinned(10) {
		t.Fatalf("expected snapshot 10 to be pinned")
	}

	if !manager.CommitTransaction(1) {
		t.Fatalf("expected known transaction commit to succeed")
	}
	if manager.ActiveTransactionCount() != 0 {
		t.Fatalf("expected no active transactions, got %d", manager.ActiveTransactionCount())
	}
	if manager.IsSnapshotPinned(10) {
		t.Fatalf("expected snapshot 10 to be unpinned")
	}

	if manager.CommitTransaction(1) {
		t.Fatalf("expected unknown transaction commit to fail")
	}

	manager.RegisterTransaction(2, 20)
	manager.AbortTransaction(2)
	manager.AbortTransaction(2)
	if manager.ActiveTransactionCount() != 0 {
		t.Fatalf("expected abort to remove active transaction")
	}
	if manager.IsSnapshotPinned(20) {
		t.Fatalf("expected abort to unpin snapshot")
	}
}

func TestMultipleTransactionsPinningSameSnapshot(t *testing.T) {
	manager := NewMVCCManager(time.Minute)

	manager.RegisterTransaction(1, 10)
	manager.RegisterTransaction(2, 10)

	if !manager.IsSnapshotPinned(10) {
		t.Fatalf("expected snapshot 10 to be pinned")
	}

	manager.AbortTransaction(1)
	if !manager.IsSnapshotPinned(10) {
		t.Fatalf("expected snapshot 10 to remain pinned while another transaction uses it")
	}

	if !manager.CommitTransaction(2) {
		t.Fatalf("expected second transaction commit to succeed")
	}
	if manager.IsSnapshotPinned(10) {
		t.Fatalf("expected snapshot 10 to be unpinned after all transactions finish")
	}
}

func TestExpiredCommitFailsAndUnpinsSnapshot(t *testing.T) {
	manager := NewMVCCManager(5 * time.Millisecond)
	manager.RegisterTransaction(1, 10)

	time.Sleep(10 * time.Millisecond)

	if manager.CommitTransaction(1) {
		t.Fatalf("expected expired transaction commit to fail")
	}
	if manager.ActiveTransactionCount() != 0 {
		t.Fatalf("expected expired transaction to be removed")
	}
	if manager.IsSnapshotPinned(10) {
		t.Fatalf("expected expired transaction to unpin snapshot")
	}
}

func TestCleanupExpiredTransactions(t *testing.T) {
	manager := NewMVCCManager(5 * time.Millisecond)
	manager.RegisterTransaction(1, 10)
	manager.RegisterTransaction(2, 20)

	time.Sleep(10 * time.Millisecond)

	cleaned := manager.CleanupExpiredTransactions()
	if cleaned != 2 {
		t.Fatalf("expected 2 cleaned transactions, got %d", cleaned)
	}
	if manager.ActiveTransactionCount() != 0 {
		t.Fatalf("expected no active transactions after cleanup")
	}
	if manager.IsSnapshotPinned(10) || manager.IsSnapshotPinned(20) {
		t.Fatalf("expected cleaned transactions to unpin snapshots")
	}
}

func TestValidateParentSnapshot(t *testing.T) {
	manager := NewMVCCManager(time.Minute)

	if !manager.ValidateParentSnapshot("events", 2, 2) {
		t.Fatalf("expected matching parent and current snapshots to validate")
	}
	if manager.ValidateParentSnapshot("events", 1, 2) {
		t.Fatalf("expected stale parent snapshot to fail validation")
	}
}
