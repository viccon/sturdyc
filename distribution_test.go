package sturdyc_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

type mockStorage struct {
	sync.Mutex
	getCount    int
	setCount    int
	deleteCount int
	records     map[string][]byte
	cancelFunc  *context.CancelFunc
}

func (m *mockStorage) Get(_ context.Context, key string) ([]byte, bool) {
	m.Lock()
	defer func() {
		if m.cancelFunc != nil {
			(*m.cancelFunc)()
		}
		m.Unlock()
	}()
	m.getCount++

	bytes, ok := m.records[key]
	return bytes, ok
}

func (m *mockStorage) Set(_ context.Context, key string, bytes []byte) {
	m.Lock()
	defer m.Unlock()
	m.setCount++

	if m.records == nil {
		m.records = make(map[string][]byte)
	}
	m.records[key] = bytes
}

func (m *mockStorage) Delete(_ context.Context, key string) {
	m.Lock()
	defer m.Unlock()
	m.deleteCount++
	delete(m.records, key)
}

func (m *mockStorage) GetBatch(_ context.Context, _ []string) map[string][]byte {
	m.Lock()
	defer func() {
		if m.cancelFunc != nil {
			(*m.cancelFunc)()
		}
		m.Unlock()
	}()
	m.getCount++
	return m.records
}

func (m *mockStorage) SetBatch(_ context.Context, records map[string][]byte) {
	m.Lock()
	defer m.Unlock()
	m.setCount++

	if m.records == nil {
		m.records = records
		return
	}

	for key, value := range records {
		m.records[key] = value
	}
}

func (m *mockStorage) DeleteBatch(_ context.Context, keys []string) {
	m.Lock()
	defer m.Unlock()
	for _, key := range keys {
		m.deleteCount++
		delete(m.records, key)
	}
}

func (m *mockStorage) assertRecord(t *testing.T, key string) {
	t.Helper()

	m.Lock()
	defer m.Unlock()

	if _, ok := m.records[key]; !ok {
		t.Errorf("expected key %s to be in records", key)
	}
}

func (m *mockStorage) assertRecords(t *testing.T, ids []string, keyFn sturdyc.KeyFn) {
	t.Helper()

	m.Lock()
	defer m.Unlock()

	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, keyFn(id))
	}

	for _, key := range keys {
		if _, ok := m.records[key]; !ok {
			t.Errorf("expected key %s to be in records", key)
		}
	}
}

func (m *mockStorage) assertGetCount(t *testing.T, count int) {
	t.Helper()
	m.Lock()
	defer m.Unlock()
	if m.getCount != count {
		t.Errorf("expected get count %d, got %d", count, m.getCount)
	}
}

func (m *mockStorage) assertSetCount(t *testing.T, count int) {
	t.Helper()
	m.Lock()
	defer m.Unlock()
	if m.setCount != count {
		t.Errorf("expected set count %d, got %d", count, m.setCount)
	}
}

func (m *mockStorage) assertDeleteCount(t *testing.T, count int) {
	t.Helper()
	m.Lock()
	defer m.Unlock()
	if m.deleteCount != count {
		t.Errorf("expected delete count %d, got %d", count, m.deleteCount)
	}
}

func TestDistributedStorage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithDistributedStorage(distributedStorage),
	)
	fetchObserver := NewFetchObserver(1)

	key := "key1"
	fetchObserver.Response(key)
	_, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecord(t, key)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the records from the in-memory cache to simulate that they were evicted.
	c.Delete(key)
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Now we can request the same key again. The underlying data source should not be called.
	res, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "valuekey1" {
		t.Errorf("expected valuekey1, got %s", res)
	}

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 1)
	distributedStorage.assertGetCount(t, 2)
	distributedStorage.assertSetCount(t, 1)
	distributedStorage.assertDeleteCount(t, 0)
}

func TestDistributedStaleStorage(t *testing.T) {
	t.Parallel()

	clock := sturdyc.NewTestClock(time.Now())
	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, time.Minute),
	)
	fetchObserver := NewFetchObserver(1)

	key := "key1"
	fetchObserver.Response(key)
	_, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecord(t, key)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll move the clock to make the record expire in the
	// in-memory cache, and become stale in the distributed storage.
	clock.Add(time.Minute * 2)

	// Now we can request the same key again, but we'll make the fetchFn error.
	// This is going to result in an ErrOnlyCachedRecords error.
	fetchObserver.Err(errors.New("error"))
	res, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected ErrOnlyCachedRecords, got %v", err)
	}
	if !errors.Is(err, fetchObserver.err) {
		t.Fatal("expected the original error to have been joined with ErrOnlyCachedRecords")
	}
	if res != "valuekey1" {
		t.Errorf("expected valuekey1, got %s", res)
	}

	// We'll want to assert that the fetch observer was called again.
	time.Sleep(100 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 2)
	distributedStorage.assertGetCount(t, 2)
	distributedStorage.assertSetCount(t, 1)
	distributedStorage.assertDeleteCount(t, 0)

	// Getting the key now should not result in another error since we took the
	// stale value from the distributed storage, and wrote it to the in-memory
	// cache.
	res, err = sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "valuekey1" {
		t.Errorf("expected valuekey1, got %s", res)
	}
}

func TestDistributedStaleStorageDeletes(t *testing.T) {
	t.Parallel()

	clock := sturdyc.NewTestClock(time.Now())
	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, time.Minute),
	)
	fetchObserver := NewFetchObserver(1)

	key := "key1"
	fetchObserver.Response(key)
	_, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecord(t, key)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll move the clock to make the record expire in the
	// in-memory cache, and become stale in the distributed storage.
	clock.Add(time.Minute * 2)

	// Now we can request the same key again, but we'll make the fetchFn return a
	// ErrNotFound. This should signal to the cache that the record has been
	// deleted at the underlying data source.
	fetchObserver.Err(sturdyc.ErrNotFound)
	res, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if !errors.Is(err, sturdyc.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if res != "" {
		t.Errorf("expected empty string (zero value), got %s", res)
	}

	// We'll want to assert that the fetch observer was called again.
	time.Sleep(100 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 2)
	distributedStorage.assertGetCount(t, 2)
	distributedStorage.assertSetCount(t, 1)
	distributedStorage.assertDeleteCount(t, 1)
}

func TestDistributedStaleStorageConvertsToMissingRecord(t *testing.T) {
	t.Parallel()

	clock := sturdyc.NewTestClock(time.Now())
	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, time.Minute),
		sturdyc.WithMissingRecordStorage(),
	)
	fetchObserver := NewFetchObserver(1)

	key := "key1"
	fetchObserver.Response(key)
	_, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecord(t, key)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll move the clock to make the record expire in the
	// in-memory cache, and become stale in the distributed storage.
	clock.Add(time.Minute * 2)

	// Now we can request the same key again, but we'll make the fetchFn return a
	// ErrNotFound. This should signal to the cache that the record has been
	// deleted at the underlying data source.
	fetchObserver.Err(sturdyc.ErrNotFound)
	res, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if !errors.Is(err, sturdyc.ErrMissingRecord) {
		t.Fatalf("expected ErrMissingRecord, got %v", err)
	}
	if res != "" {
		t.Errorf("expected empty string (zero value), got %s", res)
	}

	// We'll want to assert that the fetch observer was called again.
	<-fetchObserver.FetchCompleted
	time.Sleep(100 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 2)
	distributedStorage.assertGetCount(t, 2)
	distributedStorage.assertSetCount(t, 2)
	distributedStorage.assertDeleteCount(t, 0)

	// Lastly, we'll want to ensure that the record can be brought back into
	// existence if the fetchFn returns it from a refresh.
	fetchObserver.Clear()
	fetchObserver.Response(key)
	c.Delete(key)
	clock.Add(time.Minute * 2)

	res, err = sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "valuekey1" {
		t.Errorf("expected valuekey1, got %s", res)
	}

	time.Sleep(100 * time.Millisecond)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 3)
	distributedStorage.assertGetCount(t, 3)
	distributedStorage.assertSetCount(t, 3)
	distributedStorage.assertDeleteCount(t, 0)

	// And now we'll get it from the distributed storage without
	// a fetch to ensure that the conversion propagated.
	c.Delete(key)
	res, err = sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "valuekey1" {
		t.Errorf("expected valuekey1, got %s", res)
	}

	time.Sleep(100 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 3)
	distributedStorage.assertGetCount(t, 4)
	distributedStorage.assertSetCount(t, 3)
	distributedStorage.assertDeleteCount(t, 0)
}

func TestDistributedStorageBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithDistributedStorage(distributedStorage),
	)
	fetchObserver := NewFetchObserver(1)

	keyFn := c.BatchKeyFn("item")
	firstBatchOfIDs := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(firstBatchOfIDs)
	_, err := sturdyc.GetOrFetchBatch(ctx, c, firstBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, firstBatchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, firstBatchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the records from the in-memory cache to simulate that they were evicted.
	for _, id := range firstBatchOfIDs {
		c.Delete(keyFn(id))
	}
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Now we can request a second batch of IDs. The fetchObservers
	// FetchBatch function should not get called for IDs 1-3.
	fetchObserver.BatchResponse([]string{"4", "5", "6"})
	secondBatchOfIDs := []string{"1", "2", "3", "4", "5", "6"}
	res, err := sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	for _, id := range secondBatchOfIDs {
		if _, ok := res[id]; !ok {
			t.Errorf("expected id %s to be in the response", id)
		}
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, []string{"4", "5", "6"})
	fetchObserver.AssertFetchCount(t, 2)

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, secondBatchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 2)
	distributedStorage.assertSetCount(t, 2)
	distributedStorage.assertDeleteCount(t, 0)
}

func TestDistributedStaleStorageBatch(t *testing.T) {
	t.Parallel()

	clock := sturdyc.NewTestClock(time.Now())
	staleDuration := time.Minute
	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, staleDuration),
	)
	fetchObserver := NewFetchObserver(1)

	keyFn := c.BatchKeyFn("item")
	firstBatchOfIDs := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(firstBatchOfIDs)
	_, err := sturdyc.GetOrFetchBatch(ctx, c, firstBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, firstBatchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, firstBatchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the records from the in-memory cache to simulate that they were evicted.
	for _, id := range firstBatchOfIDs {
		c.Delete(keyFn(id))
	}
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Make the records stale by moving the clock, and then make the next fetch call return an error.
	clock.Add(staleDuration + 1)
	fetchObserver.Err(errors.New("error"))

	res, err := sturdyc.GetOrFetchBatch(ctx, c, firstBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected ErrOnlyCachedRecords, got %v", err)
	}
	for id, value := range res {
		if value != "value"+id {
			t.Errorf("expected value%s, got %s", id, value)
		}
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, firstBatchOfIDs)
	fetchObserver.AssertFetchCount(t, 2)

	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertGetCount(t, 2)
	distributedStorage.assertSetCount(t, 1)
	distributedStorage.assertDeleteCount(t, 0)
}

func TestDistributedStorageBatchDeletes(t *testing.T) {
	t.Parallel()

	staleDuration := time.Minute
	clock := sturdyc.NewTestClock(time.Now())
	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, staleDuration),
	)
	fetchObserver := NewFetchObserver(1)

	keyFn := c.BatchKeyFn("item")
	batchOfIDs := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(batchOfIDs)
	_, err := sturdyc.GetOrFetchBatch(ctx, c, batchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, batchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, batchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the records from the in-memory cache to simulate that they were evicted.
	for _, id := range batchOfIDs {
		c.Delete(keyFn(id))
	}
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Now we'll want to go past the stale time, and setup the fetch observer so
	// that it only returns the first two IDs. This will simulate that the last
	// ID has been deleted at the underlying data source.
	clock.Add(staleDuration + 1)
	fetchObserver.BatchResponse([]string{"1", "2"})
	res, err := sturdyc.GetOrFetchBatch(ctx, c, batchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(res) != 2 {
		t.Fatalf("expected 2 records, got %d", len(res))
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, batchOfIDs)
	fetchObserver.AssertFetchCount(t, 2)

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, []string{"1", "2"}, keyFn)
	distributedStorage.assertGetCount(t, 2)
	distributedStorage.assertSetCount(t, 2)
	distributedStorage.assertDeleteCount(t, 1)
}

func TestDistributedStorageBatchConvertsToMissingRecord(t *testing.T) {
	t.Parallel()

	staleDuration := time.Minute
	clock := sturdyc.NewTestClock(time.Now())
	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, staleDuration),
	)
	fetchObserver := NewFetchObserver(1)

	keyFn := c.BatchKeyFn("item")
	batchOfIDs := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(batchOfIDs)
	_, err := sturdyc.GetOrFetchBatch(ctx, c, batchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, batchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, batchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the records from the in-memory cache to simulate that they were evicted.
	for _, id := range batchOfIDs {
		c.Delete(keyFn(id))
	}
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Now we'll want to go past the stale time, and setup the fetch observer so
	// that it only returns the first two IDs. This will simulate that the last
	// ID has been deleted at the underlying data source.
	clock.Add(staleDuration + 1)
	fetchObserver.BatchResponse([]string{"1", "2"})
	res, err := sturdyc.GetOrFetchBatch(ctx, c, batchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(res) != 2 {
		t.Fatalf("expected 2 records, got %d", len(res))
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, batchOfIDs)
	fetchObserver.AssertFetchCount(t, 2)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, []string{"1", "2"}, keyFn)
	distributedStorage.assertGetCount(t, 2)
	distributedStorage.assertSetCount(t, 2)
	distributedStorage.assertDeleteCount(t, 0)

	// Next, we'll want to assert that the records can be restored from missing to existing.
	for _, id := range batchOfIDs {
		c.Delete(keyFn(id))
	}

	clock.Add(staleDuration + 1)
	fetchObserver.BatchResponse(batchOfIDs)

	res, err = sturdyc.GetOrFetchBatch(ctx, c, batchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, batchOfIDs)
	fetchObserver.AssertFetchCount(t, 3)

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, batchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 3)
	distributedStorage.assertSetCount(t, 3)
	distributedStorage.assertDeleteCount(t, 0)

	// Delete the ids to make sure that we get them from the distributed cache.
	for _, id := range batchOfIDs {
		c.Delete(keyFn(id))
	}
	res, err = sturdyc.GetOrFetchBatch(ctx, c, batchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}

	time.Sleep(50 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 3)
}

func TestDistributedStorageDoesNotCachePartialResponseAsMissingRecords(t *testing.T) {
	t.Parallel()

	refreshAfter := time.Minute
	clock := sturdyc.NewTestClock(time.Now())
	ctx := context.Background()
	ttl := time.Second * 30
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, refreshAfter),
	)
	fetchObserver := NewFetchObserver(1)

	keyFn := c.BatchKeyFn("item")
	batchOfIDs := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(batchOfIDs)
	_, err := sturdyc.GetOrFetchBatch(ctx, c, batchOfIDs, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, batchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, batchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the records from the in-memory cache to simulate that they were evicted.
	for _, id := range batchOfIDs {
		c.Delete(keyFn(id))
	}
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Now we'll add a new id to the batch that we're going to fetch. Next, we'll
	// set up the fetch observer so that it errors. We should still be able to
	// retrieve the records that we have in the distributed cache, and assert
	// that the remaining ID  should not be stored as a missing record.
	secondBatchOfIDs := []string{"1", "2", "3", "4"}
	fetchObserver.Err(errors.New("boom"))
	res, err := sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected ErrOnlyCachedRecords, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, []string{"4"})
	fetchObserver.AssertFetchCount(t, 2)
	fetchObserver.Clear()

	// The 3 records we had in the distributed cache should have been synced to
	// the in-memory cache. If we had 4  records here, it would mean that we
	// cached the last record as a missing record when the fetch observer errored
	// out. That is not the behaviour we want.
	if c.Size() != 3 {
		t.Fatalf("expected 3 records, got %d", c.Size())
	}
}

func TestPartialResponseForRefreshesDoesNotResultInMissingRecords(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 50
	evictionPercentage := 10
	ttl := time.Hour
	minRefreshDelay := time.Minute * 5
	maxRefreshDelay := time.Minute * 10
	synchronousRefreshDelay := time.Minute * 30
	refreshRetryInterval := time.Millisecond * 10
	batchSize := 10
	batchBufferTimeout := time.Minute
	refreshAfter := minRefreshDelay
	distributedStorage := &mockStorage{}
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithRefreshCoalescing(batchSize, batchBufferTimeout),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, refreshAfter),
		sturdyc.WithClock(clock),
	)

	keyFn := c.BatchKeyFn("item")
	ids := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		ids = append(ids, strconv.Itoa(i))
	}

	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)
	res, err := sturdyc.GetOrFetchBatch(ctx, c, ids, keyFn, fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 100 {
		t.Fatalf("expected 100 records, got %d", len(res))
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.Clear()

	// We need to add a sleep because the keys are written asynchronously to the
	// distributed storage. We expect that the distributed storage was queried
	// for the ids before we went to the underlying data source, and then written
	// to when it resulted in a cache miss and the data was in fact fetched.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, ids, keyFn)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll move the clock past the maxRefreshDelay. This should guarantee
	// that the next records we request gets scheduled for a refresh. We're also
	// going to add two more ids to the batch and make the fetchObserver error.
	// What should happen is the following: The cache queries the distributed
	// storage and sees that ID 1-100 are due for a refresh, and that ID 101 and
	// 102 are missing. Hence, it queries the underlying data source for all of
	// them. When the underlying data source returns an error, we should get the
	// records we have in the distributed cache back. The cache should still only
	// contain 100 records because we can't tell if ID 101 and 102 are missing or
	// not.
	clock.Add(maxRefreshDelay + time.Second)
	fetchObserver.Err(errors.New("boom"))
	secondBatchOfIDs := make([]string, 0, 102)
	for i := 1; i <= 102; i++ {
		secondBatchOfIDs = append(secondBatchOfIDs, strconv.Itoa(i))
	}
	res, err = sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected ErrOnlyCachedRecords, got %v", err)
	}
	if len(res) != 100 {
		t.Fatalf("expected 100 records, got %d", len(res))
	}

	// The fetch observer should be called 11 times. 10 times for the batches of
	// ids that we tried to refresh, and once for id 101 and 102 which we didn't
	// have in the cache.
	for i := 0; i < 11; i++ {
		<-fetchObserver.FetchCompleted
	}
	fetchObserver.AssertFetchCount(t, 12)

	// Assert that the distributed storage was queried when we
	// tried to refresh the records we had in the memory cache.
	distributedStorage.assertGetCount(t, 12)
	distributedStorage.assertSetCount(t, 1)

	// The in-memory cache should only have 100 records because we can't tell if
	// ID 101 and 102 are missing or not because the fetch observer errored out
	// when we tried to fetch them.
	if c.Size() != 100 {
		t.Fatalf("expected cache size to be 100, got %d", c.Size())
	}
}

func TestReturnsDataSourceErrorsWhenThereAreNoDistributedRecords(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithDistributedStorage(distributedStorage),
	)

	keyFn := c.BatchKeyFn("item")
	ids := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		ids = append(ids, strconv.Itoa(i))
	}

	fetchObserver := NewFetchObserver(1)
	fetchObserver.err = context.Canceled
	res, err := sturdyc.GetOrFetchBatch(ctx, c, ids, keyFn, fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted

	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.AssertRequestedRecords(t, ids)
	distributedStorage.assertGetCount(t, 1)

	if len(res) != 0 {
		t.Fatalf("expected 0 records, got %d", len(res))
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Error("expected no ErrOnlyCachedRecords since the response was empty")
	}
}

func TestReturnsPartialResultsAndJoinedErrorsOnDataSourceFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithDistributedStorage(distributedStorage),
	)
	fetchObserver := NewFetchObserver(1)

	keyFn := c.BatchKeyFn("item")
	firstBatchOfIDs := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(firstBatchOfIDs)
	_, err := sturdyc.GetOrFetchBatch(ctx, c, firstBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	fetchObserver.AssertRequestedRecords(t, firstBatchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, firstBatchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the records from the in-memory cache to ensure that we
	// get them from the distributed storage on the next GetOrFetchBatchCall.
	for _, id := range firstBatchOfIDs {
		c.Delete(keyFn(id))
	}
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Now we can request a second batch of IDs. This time though, we'll make the
	// underlying data source error out. This means that we're only going to get
	// ids 1-3 from the distributed storage.
	secondBatchOfIDs := []string{"1", "2", "3", "4", "5", "6"}
	fetchObserver.err = context.Canceled
	res, err := sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted

	// Assert that we got the 3 records which were stored in the distributed storage after the first request.
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}

	// Assert that we're returning an ErrOnlyCachedRecords error to
	// inform that the call to the underlying data source failed.
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected err to be ErrOnlyCachedRecords, got %v", err)
	}

	// Assert that the error from the request to fetch the additional IDs is returned as well.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected err to be context.Canceled, got %v", err)
	}

	// Let's also make sure that we don't lose any data source errors when
	// there is multiple joined errors returned from the fetch function.
	fetchObserver.Clear()
	fetchObserver.err = errors.Join(context.Canceled, context.DeadlineExceeded)

	// Clear the in-memory cache.
	for _, id := range firstBatchOfIDs {
		c.Delete(keyFn(id))
	}
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	res, err = sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted

	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}

	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected err to be ErrOnlyCachedRecords, got %v", err)
	}

	// Lastly, let's assert that we got both the context.Canceled and context.DeadlineExceeded errors.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected err to be context.Canceled, got %v", err)
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected err to be context.DeadlineExceeded, got %v", err)
	}
}

func TestGetOrFetchDoesNotProceedToCallTheFetchFunctionIfTheContextIsCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	clock := sturdyc.NewTestClock(time.Now())
	refreshAfter := time.Second * 10
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, refreshAfter),
		sturdyc.WithClock(clock),
	)
	fetchObserver := NewFetchObserver(1)

	key := "1"
	fetchObserver.Response(key)
	val, err := sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)
	if val != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The key is written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecord(t, key)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the key from the in-memory cache.
	c.Delete(key)
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Now we'll request the key again. This time though, we'll move the clock
	// forward to indicate that the request has to be refreshed, but before that
	// is done we'll cancel the context.
	clock.Add(refreshAfter + time.Second)
	distributedStorage.cancelFunc = &cancel
	val, err = sturdyc.GetOrFetch(ctx, c, key, fetchObserver.Fetch)

	// Assert that we got the key from the distributed storage.
	distributedStorage.assertGetCount(t, 2)

	// Assert that we didn't call the fetch function again.
	if fetchObserver.fetchCount != 1 {
		t.Fatalf("expected fetch count to be 1, got %d", fetchObserver.fetchCount)
	}

	if val != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected err to be context.Canceled, got %v", err)
	}

	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected err to be context.Canceled, got %v", err)
	}

	// Assert that we wrote the value from the distributed cache back to the in-memory cache.
	if c.Size() != 1 {
		t.Fatalf("expected cache size to be 1, got %d", c.Size())
	}
}

func TestGetOrFetchBatchDoesNotProceedToCallTheFetchFunctionIfTheContextIsCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	ttl := time.Minute
	distributedStorage := &mockStorage{}
	c := sturdyc.New[string](1000, 10, ttl, 30,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithDistributedStorage(distributedStorage),
	)
	fetchObserver := NewFetchObserver(1)

	keyFn := c.BatchKeyFn("item")
	firstBatchOfIDs := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(firstBatchOfIDs)
	_, err := sturdyc.GetOrFetchBatch(ctx, c, firstBatchOfIDs, keyFn, fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	fetchObserver.AssertRequestedRecords(t, firstBatchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// The keys are written asynchronously to the distributed storage.
	time.Sleep(100 * time.Millisecond)
	distributedStorage.assertRecords(t, firstBatchOfIDs, keyFn)
	distributedStorage.assertGetCount(t, 1)
	distributedStorage.assertSetCount(t, 1)

	// Next, we'll delete the records from the in-memory cache to ensure that we
	// get them from the distributed storage on the next GetOrFetchBatchCall.
	for _, id := range firstBatchOfIDs {
		c.Delete(keyFn(id))
	}
	if c.Size() != 0 {
		t.Fatalf("expected cache size to be 0, got %d", c.Size())
	}

	// Now we can request a second batch of IDs. This time though, we'll cancel
	// the context after the distributed cache has been called.
	secondBatchOfIDs := []string{"1", "2", "3", "4", "5", "6"}
	distributedStorage.cancelFunc = &cancel
	res, err := sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, keyFn, fetchObserver.FetchBatch)

	// Assert that we got the 3 records which were stored in the distributed storage after the first request.
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}

	// Assert that we didn't call the fetch function again.
	if fetchObserver.fetchCount != 1 {
		t.Fatalf("expected fetch count to be 1, got %d", fetchObserver.fetchCount)
	}

	// Assert that the error is both an ErrOnlyCachedRecords and a context.Canceled error.
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected err to be ErrOnlyCachedRecords, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected err to be context.Canceled, got %v", err)
	}
}
