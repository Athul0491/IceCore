package cache

import "testing"

func TestNewLRUNormalizesNonPositiveCapacity(t *testing.T) {
	for _, capacity := range []int{0, -10} {
		lru := NewLRU[string, int](capacity)

		if lru.Capacity() != 1 {
			t.Fatalf("expected capacity 1 for input %d, got %d", capacity, lru.Capacity())
		}
	}
}

func TestLRUPutGetAndStats(t *testing.T) {
	lru := NewLRU[string, int](2)

	lru.Put("a", 1)

	got, ok := lru.Get("a")
	if !ok {
		t.Fatalf("expected key a to be present")
	}
	if got != 1 {
		t.Fatalf("expected value 1, got %d", got)
	}

	if _, ok := lru.Get("missing"); ok {
		t.Fatalf("expected missing key to be absent")
	}

	if lru.Hits() != 1 {
		t.Fatalf("expected 1 hit, got %d", lru.Hits())
	}
	if lru.Misses() != 1 {
		t.Fatalf("expected 1 miss, got %d", lru.Misses())
	}
	if lru.HitRate() != 0.5 {
		t.Fatalf("expected hit rate 0.5, got %f", lru.HitRate())
	}
}

func TestLRUUpdateRefreshesRecency(t *testing.T) {
	lru := NewLRU[string, int](2)

	lru.Put("a", 1)
	lru.Put("b", 2)
	lru.Put("a", 10)
	lru.Put("c", 3)

	if _, ok := lru.Get("b"); ok {
		t.Fatalf("expected b to be evicted")
	}
	got, ok := lru.Get("a")
	if !ok {
		t.Fatalf("expected a to remain after update")
	}
	if got != 10 {
		t.Fatalf("expected updated value 10, got %d", got)
	}
	if _, ok := lru.Get("c"); !ok {
		t.Fatalf("expected c to be present")
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	lru := NewLRU[string, int](2)

	lru.Put("a", 1)
	lru.Put("b", 2)
	if _, ok := lru.Get("a"); !ok {
		t.Fatalf("expected a to be present")
	}
	lru.Put("c", 3)

	if _, ok := lru.Get("b"); ok {
		t.Fatalf("expected b to be evicted")
	}
	if _, ok := lru.Get("a"); !ok {
		t.Fatalf("expected a to remain")
	}
	if _, ok := lru.Get("c"); !ok {
		t.Fatalf("expected c to be present")
	}
	if lru.Evictions() != 1 {
		t.Fatalf("expected 1 eviction, got %d", lru.Evictions())
	}
	if lru.Size() != 2 {
		t.Fatalf("expected size 2, got %d", lru.Size())
	}
}

func TestLRUInvalidateClearAndZeroHitRate(t *testing.T) {
	lru := NewLRU[string, int](4)

	if lru.HitRate() != 0 {
		t.Fatalf("expected empty hit rate to be 0, got %f", lru.HitRate())
	}

	lru.Put("users:1", 1)
	lru.Put("users:2", 2)
	lru.Put("orders:1", 3)

	lru.Invalidate("users:1")
	if _, ok := lru.Get("users:1"); ok {
		t.Fatalf("expected users:1 to be invalidated")
	}

	removed := lru.InvalidateIf(func(key string) bool {
		return len(key) >= len("users:") && key[:len("users:")] == "users:"
	})
	if removed != 1 {
		t.Fatalf("expected 1 item removed by predicate, got %d", removed)
	}
	if _, ok := lru.Get("users:2"); ok {
		t.Fatalf("expected users:2 to be invalidated")
	}
	if _, ok := lru.Get("orders:1"); !ok {
		t.Fatalf("expected orders:1 to remain")
	}

	lru.Clear()
	if lru.Size() != 0 {
		t.Fatalf("expected size 0 after clear, got %d", lru.Size())
	}
}
