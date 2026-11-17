package sturdyc

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// entry represents a single cache entry.
type entry[T any] struct {
	key                  string
	value                T
	expiresAt            time.Time
	backgroundRefreshAt  time.Time
	synchronousRefreshAt time.Time
	numOfRefreshRetries  int
	// size is the estimated number of bytes that the entry occupies. It's
	// only computed when the cache has been configured to report metrics.
	// It's an int32 so that it fits in the padding next to the bool, which
	// means that it doesn't increase the size of the entry.
	size            int32
	isMissingRecord bool
}

// shard is a thread-safe data structure that holds a subset of the cache entries.
type shard[T any] struct {
	sync.RWMutex
	*Config
	capacity           int
	ttl                time.Duration
	entries            map[string]*entry[T]
	evictionPercentage int
	// sizer is nil unless the cache has been configured to report metrics.
	sizer *entrySizer[T]
	// sizeInBytes is a running tally of the estimated size of all entries.
	sizeInBytes int64
	// inFlight holds the keys of this shard that are currently being fetched.
	// It's guarded by the shard's lock so that writes and deletions can
	// invalidate a fetch atomically with respect to the fetch committing its
	// result.
	inFlight map[string]*flight[T]
}

// newShard creates a new shard and returns a pointer to it.
func newShard[T any](capacity int, ttl time.Duration, evictionPercentage int, cfg *Config) *shard[T] {
	s := &shard[T]{
		Config:             cfg,
		capacity:           capacity,
		ttl:                ttl,
		entries:            make(map[string]*entry[T]),
		evictionPercentage: evictionPercentage,
		inFlight:           make(map[string]*flight[T]),
	}
	// Estimating the size of the entries isn't free, so we'll
	// only do it if there is someone to report the size to.
	if cfg.metricsRecorder != nil {
		s.sizer = newEntrySizer[T]()
	}
	return s
}

// size returns the number of entries in the shard.
func (s *shard[T]) size() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.entries)
}

// sizeBytes returns the estimated number of bytes that the shard's entries occupy.
func (s *shard[T]) sizeBytes() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.sizeInBytes
}

// removeEntry deletes an entry from the shard. Should be called with a lock.
func (s *shard[T]) removeEntry(e *entry[T]) {
	delete(s.entries, e.key)
	s.sizeInBytes -= int64(e.size)
}

// evictExpired evicts all the expired entries in the shard.
func (s *shard[T]) evictExpired() {
	s.Lock()
	defer s.Unlock()

	var entriesEvicted int
	for _, e := range s.entries {
		if s.clock.Now().After(e.expiresAt) {
			s.removeEntry(e)
			entriesEvicted++
		}
	}
	s.reportEntriesEvicted(entriesEvicted)
}

// forceEvict evicts a certain percentage of the entries in the shard
// based on the expiration time. Should be called with a lock.
func (s *shard[T]) forceEvict() {
	s.reportForcedEviction()

	// Check if we should evict all entries.
	if s.evictionPercentage == 100 {
		evictedCount := len(s.entries)
		s.entries = make(map[string]*entry[T])
		s.sizeInBytes = 0
		s.reportEntriesEvicted(evictedCount)
		return
	}

	expirationTimes := make([]time.Time, 0, len(s.entries))
	for _, e := range s.entries {
		expirationTimes = append(expirationTimes, e.expiresAt)
	}

	// We could have a lumpy distribution of expiration times. As an example, we
	// might have 100 entries in the cache but only 2 unique expiration times. In
	// order to not over-evict when trying to remove 10%, we'll have to keep
	// track of the number of entries that we've evicted.
	percentage := float64(s.evictionPercentage) / 100
	cutoff := FindCutoff(expirationTimes, percentage)
	entriesToEvict := int(float64(len(expirationTimes)) * percentage)
	entriesEvicted := 0
	for _, e := range s.entries {
		// Here we're essentially saying: if e.expiresAt <= cutoff.
		if !e.expiresAt.After(cutoff) {
			s.removeEntry(e)
			entriesEvicted++

			if entriesEvicted == entriesToEvict {
				break
			}
		}
	}
	s.reportEntriesEvicted(entriesEvicted)
}

// get attempts to retrieve a value from the shard.
//
// Parameters:
//
//	key: The key for which the value is to be retrieved.
//
// Returns:
//
//	val: The value associated with the key, if it exists.
//	exists: A boolean indicating if the value exists in the shard.
//	markedAsMissing: A boolean indicating if the key has been marked as a missing record.
//	refresh: A boolean indicating if the value should be refreshed in the background.
func (s *shard[T]) get(key string) (val T, exists, markedAsMissing, backgroundRefresh, synchronousRefresh bool) {
	s.RLock()
	item, ok := s.entries[key]
	if !ok {
		s.RUnlock()
		return val, false, false, false, false
	}

	if s.clock.Now().After(item.expiresAt) {
		s.RUnlock()
		return val, false, false, false, false
	}

	// Check if the record should be synchronously refreshed.
	if s.earlyRefreshes && s.clock.Now().After(item.synchronousRefreshAt) {
		s.RUnlock()
		return item.value, true, item.isMissingRecord, false, true
	}

	shouldRefresh := s.earlyRefreshes && s.clock.Now().After(item.backgroundRefreshAt)
	if shouldRefresh {
		// Release the read lock, and switch to a write lock.
		s.RUnlock()
		s.Lock()

		// However, during the time it takes to switch locks, another goroutine
		// might have acquired it and moved the refreshAt. Therefore, we'll have to
		// check if this operation should still be performed.
		if !s.clock.Now().After(item.backgroundRefreshAt) {
			s.Unlock()
			return item.value, true, item.isMissingRecord, false, false
		}

		// Update the "refreshAt" so no other goroutines attempts to refresh the same entry.
		item.backgroundRefreshAt = s.clock.Now().Add(s.refreshRetryDelay(item.numOfRefreshRetries))
		item.numOfRefreshRetries++

		s.Unlock()
		return item.value, true, item.isMissingRecord, shouldRefresh, false
	}

	s.RUnlock()
	return item.value, true, item.isMissingRecord, false, false
}

// entrySize estimates the size of an entry. It's done before we acquire the
// lock, as it could involve walking a large value using reflection.
func (s *shard[T]) entrySize(key string, value T) int32 {
	if s.sizer == nil {
		return 0
	}
	return s.sizer.size(key, value)
}

// set writes a key-value pair to the shard and returns a boolean indicating
// whether an eviction was performed. It's used for writes that originate from
// the user, which is why it invalidates any fetch that is in flight for the
// key: that fetch could have read the data before the user's write, and must
// not be allowed to overwrite it.
func (s *shard[T]) set(key string, value T, isMissingRecord bool) bool {
	size := s.entrySize(key, value)
	s.Lock()
	defer s.Unlock()
	s.invalidateInFlight(key)
	return s.write(key, value, isMissingRecord, size)
}

// write stores the entry and returns a boolean indicating whether an eviction
// was performed. Should be called with a lock.
func (s *shard[T]) write(key string, value T, isMissingRecord bool, size int32) bool {
	// Overwriting an existing entry doesn't increase the number of entries,
	// so it must never trigger an eviction.
	existing, exists := s.entries[key]
	evict := !exists && len(s.entries) >= s.capacity

	// If the cache is configured to not evict any entries,
	// and we're at full capacity, we'll return early.
	if s.evictionPercentage < 1 && evict {
		return false
	}

	if evict {
		s.forceEvict()
	}

	now := s.clock.Now()
	newEntry := &entry[T]{
		key:             key,
		value:           value,
		expiresAt:       now.Add(s.ttl),
		size:            size,
		isMissingRecord: isMissingRecord,
	}

	if s.earlyRefreshes {
		// If there is a difference between the min- and maxRefreshTime we'll use that to
		// set a random padding so that the refreshes get spread out evenly over time.
		var padding time.Duration
		if s.minAsyncRefreshTime != s.maxAsyncRefreshTime {
			padding = time.Duration(rand.Int64N(int64(s.maxAsyncRefreshTime - s.minAsyncRefreshTime)))
		}
		newEntry.backgroundRefreshAt = now.Add(s.minAsyncRefreshTime + padding)
		newEntry.synchronousRefreshAt = now.Add(s.syncRefreshTime)
		newEntry.numOfRefreshRetries = 0
	}

	// If we're overwriting an existing entry, we have to make
	// sure that its size is subtracted from the running tally.
	if exists {
		s.sizeInBytes -= int64(existing.size)
	}
	s.entries[key] = newEntry
	s.sizeInBytes += int64(size)
	return evict
}

// delete removes a key from the shard. Any fetch that is in flight for the
// key is invalidated so that it can't write data that was read before the
// deletion back into the cache.
func (s *shard[T]) delete(key string) {
	s.Lock()
	defer s.Unlock()
	s.invalidateInFlight(key)
	if e, ok := s.entries[key]; ok {
		s.removeEntry(e)
	}
}

// keys returns all non-expired keys in the shard.
func (s *shard[T]) keys() []string {
	s.RLock()
	defer s.RUnlock()
	keys := make([]string, 0, len(s.entries))
	for k, v := range s.entries {
		if s.clock.Now().After(v.expiresAt) {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

// refreshRetryDelay returns how long we'll wait before attempting another
// background refresh. The delay doubles with every retry, and it's capped at
// the TTL, as there is no point in waiting for longer than the entry lives.
func (s *shard[T]) refreshRetryDelay(retries int) time.Duration {
	delay := s.retryBaseDelay
	for i := 0; i < retries && delay < s.ttl; i++ {
		if delay > math.MaxInt64/2 {
			return s.ttl
		}
		delay *= 2
	}
	return min(delay, s.ttl)
}

// numInFlight returns the number of keys in the shard that are being fetched.
func (s *shard[T]) numInFlight() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.inFlight)
}

// joinOrLead returns the fetch that is in flight for the key. If there isn't
// one, a new flight is registered as part of the given call, and the caller
// becomes responsible for fetching the key. If the call is nil, a new one is
// created.
//
// If reuseFresh is true and the key has an entry that doesn't need to be
// fetched, a completed flight holding that entry is returned instead. This
// happens when another fetch commits its result after the caller looked the
// key up, but before it got here.
func (s *shard[T]) joinOrLead(key string, c *call, reuseFresh bool) (f *flight[T], leader bool) {
	s.Lock()
	defer s.Unlock()
	if existing, ok := s.inFlight[key]; ok {
		return existing, false
	}
	if e, ok := s.entries[key]; ok && reuseFresh && s.isFresh(e) {
		cached := &flight[T]{call: completedCall, val: e.value, found: !e.isMissingRecord}
		if e.isMissingRecord {
			cached.err = ErrMissingRecord
		}
		return cached, false
	}
	if c == nil {
		c = newCall()
	}
	f = &flight[T]{call: c}
	s.inFlight[key] = f
	return f, true
}

// isFresh reports whether an entry can be returned without being fetched,
// i.e. it hasn't expired, and it isn't due for a synchronous refresh. Should
// be called with a lock.
func (s *shard[T]) isFresh(e *entry[T]) bool {
	now := s.clock.Now()
	if now.After(e.expiresAt) {
		return false
	}
	return !s.earlyRefreshes || !now.After(e.synchronousRefreshAt)
}

// invalidateInFlight marks the fetch for the key as invalidated, and removes it
// so that subsequent requests start a new fetch instead of joining one that
// may have read stale data. Should be called with a lock.
func (s *shard[T]) invalidateInFlight(key string) {
	if f, ok := s.inFlight[key]; ok {
		f.invalidated = true
		delete(s.inFlight, key)
	}
}

// commit applies the result of a fetch to the shard and removes the flight.
// Nothing is written if the key was written or deleted after the fetch
// started, as the result could be older than what is in the cache now.
func (s *shard[T]) commit(key string, f *flight[T], op cacheOp, value T) {
	var size int32
	switch op {
	case opWrite:
		size = s.entrySize(key, value)
	case opWriteMissing:
		var zero T
		size = s.entrySize(key, zero)
	case opNone, opDelete:
	}

	s.Lock()
	defer s.Unlock()
	if s.inFlight[key] == f {
		delete(s.inFlight, key)
	}
	if f.invalidated {
		return
	}

	switch op {
	case opWrite:
		s.write(key, value, false, size)
	case opWriteMissing:
		var zero T
		s.write(key, zero, true, size)
	case opDelete:
		if e, ok := s.entries[key]; ok {
			s.removeEntry(e)
		}
	case opNone:
	}
}
