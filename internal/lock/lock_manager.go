package lock

import "sync"

type Manager struct {
	mu    sync.Mutex
	locks map[string]*sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		locks: make(map[string]*sync.RWMutex),
	}
}

func (m *Manager) getOrCreate(table string) *sync.RWMutex {
	m.mu.Lock()
	defer m.mu.Unlock()

	if l, ok := m.locks[table]; ok {
		return l
	}

	l := &sync.RWMutex{}
	m.locks[table] = l
	return l
}

func (m *Manager) LockShared(table string) func() {
	l := m.getOrCreate(table)
	l.RLock()
	return l.RUnlock
}

func (m *Manager) LockExclusive(table string) func() {
	l := m.getOrCreate(table)
	l.Lock()
	return l.Unlock
}
