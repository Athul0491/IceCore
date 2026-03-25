package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type entry[K comparable, V any] struct {
	key   K
	value V
}

type LRU[K comparable, V any] struct {
	capacity int

	mu    sync.Mutex
	items map[K]*list.Element
	list  *list.List

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	if capacity <= 0 {
		capacity = 1
	}
	return &LRU[K, V]{
		capacity: capacity,
		items:    make(map[K]*list.Element),
		list:     list.New(),
	}
}

func (l *LRU[K, V]) Get(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.items[key]; ok {
		l.list.MoveToFront(elem)
		l.hits.Add(1)
		return elem.Value.(entry[K, V]).value, true
	}

	l.misses.Add(1)
	var zero V
	return zero, false
}

func (l *LRU[K, V]) Put(key K, value V) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.items[key]; ok {
		elem.Value = entry[K, V]{key: key, value: value}
		l.list.MoveToFront(elem)
		return
	}

	if l.list.Len() >= l.capacity {
		last := l.list.Back()
		if last != nil {
			ent := last.Value.(entry[K, V])
			delete(l.items, ent.key)
			l.list.Remove(last)
			l.evictions.Add(1)
		}
	}

	elem := l.list.PushFront(entry[K, V]{key: key, value: value})
	l.items[key] = elem
}

func (l *LRU[K, V]) Invalidate(key K) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.items[key]; ok {
		delete(l.items, key)
		l.list.Remove(elem)
	}
}

func (l *LRU[K, V]) InvalidateIf(pred func(K) bool) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	removed := 0
	for key, elem := range l.items {
		if pred(key) {
			delete(l.items, key)
			l.list.Remove(elem)
			removed++
		}
	}
	return removed
}

func (l *LRU[K, V]) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.items = make(map[K]*list.Element)
	l.list.Init()
}

func (l *LRU[K, V]) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

func (l *LRU[K, V]) Capacity() int {
	return l.capacity
}

func (l *LRU[K, V]) Hits() uint64 {
	return l.hits.Load()
}

func (l *LRU[K, V]) Misses() uint64 {
	return l.misses.Load()
}

func (l *LRU[K, V]) Evictions() uint64 {
	return l.evictions.Load()
}

func (l *LRU[K, V]) HitRate() float64 {
	h := l.hits.Load()
	m := l.misses.Load()
	total := h + m
	if total == 0 {
		return 0
	}
	return float64(h) / float64(total)
}