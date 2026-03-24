package transaction

import "time"

type MVCCManager struct {
	txnTimeout time.Duration
}

func NewMVCCManager(timeout time.Duration) *MVCCManager {
	return &MVCCManager{txnTimeout: timeout}
}