package common

import "sync"

// StringInterner interns strings to reduce memory usage when the same value
// appears multiple times. It is sharded to reduce lock contention.
//
// The implementation is safe for concurrent use.

const numShards = 32

type stringShard struct {
	mu    sync.RWMutex
	store map[string]string
}

type StringInterner struct {
	shards [numShards]*stringShard
}

// NewStringInterner creates a new StringInterner instance.
func NewStringInterner() *StringInterner {
	interner := &StringInterner{}
	for i := 0; i < numShards; i++ {
		interner.shards[i] = &stringShard{
			store: make(map[string]string),
		}
	}
	return interner
}

func (i *StringInterner) getShard(value string) *stringShard {
	var hash uint32 = 2166136261
	for j := 0; j < len(value); j++ {
		hash ^= uint32(value[j])
		hash *= 16777619
	}
	return i.shards[hash%numShards]
}

// Intern returns a canonical representation for the provided string.
func (i *StringInterner) Intern(value string) string {
	if value == "" {
		return ""
	}

	shard := i.getShard(value)

	shard.mu.RLock()
	canonical, ok := shard.store[value]
	shard.mu.RUnlock()
	if ok {
		return canonical
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if canonical, ok = shard.store[value]; ok {
		return canonical
	}

	shard.store[value] = value
	return value
}
