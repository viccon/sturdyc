package sturdyc_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

func TestGetOrFetch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 5
	numShards := 2
	ttl := time.Minute
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1)
	fetchObserver.Response(id)

	// The first time we call Get, it should call the fetchFn to retrieve the value.
	firstValue, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if firstValue != "value1" {
		t.Errorf("expected value1, got %v", firstValue)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)

	// The second time we call Get, we expect to have it served from the sturdyc.
	secondValue, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if secondValue != "value1" {
		t.Errorf("expected value1, got %v", secondValue)
	}
	time.Sleep(time.Millisecond * 10)
	fetchObserver.AssertFetchCount(t, 1)
}

func TestGetOrFetchStampedeProtection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10
	numShards := 2
	ttl := time.Second * 2
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Millisecond * 500
	synchronousRefreshDelay := time.Second
	refreshRetryInterval := time.Millisecond * 10

	// The cache is going to have a 2 second TTL, and the first refresh should happen within a second.
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1)
	fetchObserver.Response(id)

	// We will start the test by trying to get key1, which wont exist in the sturdyc. Hence,
	// the fetch function is going to get called and we'll set the initial value to val1.
	sturdyc.GetOrFetch[string](ctx, c, id, fetchObserver.Fetch)

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)

	// Now, we're going to go past the refresh delay and try to refresh it from 1000 goroutines at once.
	numGoroutines := 1000
	clock.Add(maxRefreshDelay + 1)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
}

func TestGetOrFetchRefreshRetries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 5
	numShards := 1
	ttl := time.Minute
	evictionPercentage := 10
	minRefreshDelay := time.Second
	maxRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(6)
	fetchObserver.Response(id)

	_, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// Now, we'll move the clock passed the refresh delay which should make the
	// next call to GetOrFetchBatch result in a call to refresh the record.
	clock.Add(maxRefreshDelay + 1)
	fetchObserver.Err(errors.New("error"))
	_, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Next, we'll assert that the retries grows exponentially. Even though we're
	// making 100 requests with 1 second between them, we only expect 6 calls to
	// go through.
	for i := 0; i < 100; i++ {
		clock.Add(retryInterval)
		sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	}
	for i := 0; i < 6; i++ {
		<-fetchObserver.FetchCompleted
	}
	fetchObserver.AssertMaxFetchCount(t, 8)
}

func TestGetOrFetchMissingRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 5
	numShards := 1
	ttl := time.Minute
	evictionPercentage := 20
	minRefreshDelay := time.Second
	maxRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
	)

	fetchObserver := NewFetchObserver(1)
	fetchObserver.Err(sturdyc.ErrNotFound)
	_, err := sturdyc.GetOrFetch(ctx, c, "1", fetchObserver.Fetch)
	if !errors.Is(err, sturdyc.ErrMissingRecord) {
		t.Fatalf("expected ErrMissingRecord, got %v", err)
	}
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// Make the request again. It should trigger the refresh of the missing record to happen in the background.
	clock.Add(maxRefreshDelay * 1)
	fetchObserver.Response("1")
	_, err = sturdyc.GetOrFetch(ctx, c, "1", fetchObserver.Fetch)
	if !errors.Is(err, sturdyc.ErrMissingRecord) {
		t.Fatalf("expected ErrMissingRecordCooldown, got %v", err)
	}
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)

	// The next time we call the cache, the record should be there.
	val, err := sturdyc.GetOrFetch(ctx, c, "1", fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "value1" {
		t.Errorf("expected value to be value1, got %v", val)
	}
	fetchObserver.AssertFetchCount(t, 2)
}

func TestGetOrFetchBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := sturdyc.New[string](5, 1, time.Minute, 30,
		sturdyc.WithNoContinuousEvictions(),
	)
	fetchObserver := NewFetchObserver(1)

	firstBatchOfIDs := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(firstBatchOfIDs)
	_, err := sturdyc.GetOrFetchBatch(ctx, c, firstBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, firstBatchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// At this point, id 1, 2, and 3 should be in the sturdyc. Therefore, if we make a second
	// request where we'll request item 1, 2, 3, and 4, we'll only expect that item 4 is fetched.
	secondBatchOfIDs := []string{"1", "2", "3", "4"}
	fetchObserver.BatchResponse([]string{"4"})
	_, err = sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, []string{"4"})
	fetchObserver.AssertFetchCount(t, 2)
	fetchObserver.Clear()

	// The last scenario we want to test is for partial responses. This time, we'll request ids 2, 4, and 6. The item with
	// id 6 isn't in our cache yet, so the fetch function should get invoked. However, we'll make the fetch function error.
	// This should give us a ErrOnlyCachedRecords error, along with the records we could retrieve from the sturdyc.
	thirdBatchOfIDs := []string{"2", "4", "6"}
	fetchObserver.Err(errors.New("error"))
	records, err := sturdyc.GetOrFetchBatch(ctx, c, thirdBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, []string{"6"})
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Errorf("expected ErrPartialBatchResponse, got %v", err)
	}
	if len(records) != 2 {
		t.Errorf("expected to get the two records we had cached, got %v", len(records))
	}
}

func TestBatchGetOrFetchNilMapMissingRecords(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 5
	numShards := 1
	ttl := time.Minute
	evictionPercentage := 50
	minRefreshDelay := time.Second
	maxRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Second
	clock := sturdyc.NewTestClock(time.Now())
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	fetchObserver := NewFetchObserver(1)
	ids := []string{"1", "2", "3", "4"}
	records, err := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no records, got %v", records)
	}
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)

	// The request didn't return any records, and we have configured the cache to
	// store these ids as cache misses. Hence, performing the request again,
	// should not result in another call before the refresh delay has passed.
	clock.Add(minRefreshDelay - 1)
	records, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no records, got %v", records)
	}
	time.Sleep(time.Millisecond * 10)
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)
}

func TestGetOrFetchBatchRetries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 5
	numShards := 1
	ttl := time.Hour * 24
	evictionPercentage := 10
	minRefreshDelay := time.Hour
	maxRefreshDelay := time.Hour * 2
	synchronousRefreshDelay := time.Hour * 4
	retryInterval := time.Second
	clock := sturdyc.NewTestClock(time.Now())
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)
	fetchObserver := NewFetchObserver(6)

	ids := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(ids)

	// Assert that all records were requested, and that we retrieved each one of them.
	_, err := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// Now, we'll move the clock passed the refresh delay which should make the
	// next call to GetOrFetchBatch result in a call to refresh the record.
	clock.Add(maxRefreshDelay + 1)
	fetchObserver.Err(errors.New("error"))
	_, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 2)

	// Next, we'll assert that the retries grows exponentially. Even though we're
	// making 100 requests with 1 second between them, we only expect 6 calls to
	// go through.
	for i := 0; i < 100; i++ {
		clock.Add(retryInterval)
		sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	}
	for i := 0; i < 6; i++ {
		<-fetchObserver.FetchCompleted
	}
	fetchObserver.AssertFetchCount(t, 8)
}

func TestBatchGetOrFetchOnlyCachedRecordsErr(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 5
	numShards := 1
	ttl := time.Minute
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
	)
	fetchObserver := NewFetchObserver(1)

	// We'll start by fetching a couple of ids without any errors to fill the sturdyc.
	ids := []string{"1", "2", "3", "4"}
	fetchObserver.BatchResponse(ids)
	_, firstBatchErr := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if firstBatchErr != nil {
		t.Errorf("expected no error, got %v", firstBatchErr)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// Now, we'll append "5" to our slice of ids. After that we'll try to fetch the
	// records again. This time with a BatchFn that returns an error. That should give
	// us a ErrOnlyCachedRecords error along with the records we had in the sturdyc.
	// This allows the caller to decide if they want to proceed or not.
	ids = append(ids, "5")
	fetchObserver.Err(errors.New("error"))
	records, secondBatchErr := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)

	if !errors.Is(secondBatchErr, sturdyc.ErrOnlyCachedRecords) {
		t.Errorf("expected ErrPartialBatchResponse, got %v", secondBatchErr)
	}
	// We should have a record for every id except the last one.
	if len(records) != len(ids)-1 {
		t.Errorf("expected to get %v records, got %v", len(ids)-1, len(records))
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, []string{"5"})
	fetchObserver.AssertFetchCount(t, 2)
}

func TestGetOrFetchBatchStampedeProtection(t *testing.T) {
	t.Parallel()

	// We're going to fetch the same list of ids in 1000 goroutines.
	numGoroutines := 1000
	ctx := context.Background()
	capacity := 10
	shards := 2
	ttl := time.Second * 2
	evictionPercentage := 5
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Millisecond * 1000
	synchronousRefreshDelay := time.Millisecond * 1500
	refreshRetryInterval := time.Millisecond * 10
	c := sturdyc.New[string](capacity, shards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
		sturdyc.WithMetrics(newTestMetricsRecorder(shards)),
	)

	ids := []string{"1", "2", "3"}
	fetchObserver := NewFetchObserver(1000)
	fetchObserver.BatchResponse(ids)

	_, err := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)

	// Set the clock to be just before the min cache refresh threshold.
	// This should not be enough to make the cache call our fetchFn.
	clock.Add(minRefreshDelay - 1)
	_, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// We don't expect fetchObserver.Fetch to have been called. Therfore, we'll
	// sleep for a brief duration and asser that the fetch count is still 1.
	time.Sleep(time.Millisecond * 10)
	fetchObserver.AssertFetchCount(t, 1)

	// Now, let's go past the threshold. This should make the next GetOrFetchBatch
	// call schedule a refresh in the background, and with that we're going to
	// test that the stampede protection works as intended. Invoking it from 1000
	// goroutines at the same time should not make us schedule multiple refreshes.
	clock.Add((maxRefreshDelay - minRefreshDelay) + 1)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			_, goroutineErr := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
			if goroutineErr != nil {
				panic(goroutineErr)
			}
		}()
	}
	wg.Wait()

	// Even though we called GetOrFetch 1000 times, it should only result in a
	// maximum of 3 outgoing requests. Most likely, it should just be one
	// additional request but without having a top level lock in GetOrFetchBatch we
	// can't guarantee that the first goroutine moves the refreshAt of all 3 ids.
	// The first goroutine might get a lock for the first index, and then get paused.
	// During that time a second goroutine could have refreshed id 2 and 3.
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertMaxFetchCount(t, 4)
}

func TestGetOrFetchDeletesRecordsThatHaveBeenRemovedAtTheSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Second
	synchronousRefreshDelay := time.Minute
	refreshRetryInterval := time.Millisecond * 10

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1)
	fetchObserver.Response(id)

	c.GetOrFetch(ctx, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	time.Sleep(time.Millisecond * 10)

	if c.Size() != 1 {
		t.Errorf("expected cache to have 1 records, got %v", c.Size())
	}

	// Now we're going to go past the refresh delay, and return an error
	// that indicates that the record has been deleted at the source.
	fetchObserver.Clear()
	fetchObserver.Err(sturdyc.ErrNotFound)
	clock.Add(maxRefreshDelay + 1)
	c.GetOrFetch(ctx, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	time.Sleep(time.Millisecond * 10)

	if c.Size() != 0 {
		t.Errorf("expected cache to have 0 records, got %v", c.Size())
	}
}

func TestGetOrFetchConvertsDeletedRecordsToMissingRecords(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Second
	synchronousRefreshDelay := time.Minute
	refreshRetryInterval := time.Millisecond * 10

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1)
	fetchObserver.Response(id)

	c.GetOrFetch(ctx, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	time.Sleep(time.Millisecond * 10)

	if c.Size() != 1 {
		t.Errorf("expected cache to have 1 records, got %v", c.Size())
	}
	if _, ok := c.Get(id); !ok {
		t.Errorf("expected item to exist in the cache")
	}

	// Now we're going to go past the refresh delay, and return an error
	// that indicates that the record should now be stored as missing.
	fetchObserver.Clear()
	fetchObserver.Err(sturdyc.ErrNotFound)
	clock.Add(maxRefreshDelay + 1)
	c.GetOrFetch(ctx, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	time.Sleep(time.Millisecond * 10)

	if c.Size() != 1 {
		t.Errorf("expected cache to have 1 record, got %v", c.Size())
	}
	if _, ok := c.Get(id); ok {
		t.Errorf("expected item to not be returned by the Get call because it's a missing record")
	}
}

func TestGetOrFetchBatchDeletesRecordsThatHaveBeenRemovedAtTheSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Second
	synchronousRefreshDelay := time.Minute
	refreshRetryInterval := time.Millisecond * 10

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithClock(clock),
	)

	ids := []string{"1", "2", "3", "4", "5"}
	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)

	c.GetOrFetchBatch(ctx, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	time.Sleep(time.Millisecond * 10)

	if c.Size() != 5 {
		t.Errorf("expected cache to have 5 records, got %v", c.Size())
	}

	// Now we're going to go past the refresh delay, and
	// only return the first 3 records from the source.
	fetchObserver.BatchResponse([]string{"1", "2", "3"})
	clock.Add(maxRefreshDelay + 1)
	c.GetOrFetchBatch(ctx, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
	time.Sleep(time.Millisecond * 10)

	// The other two records should have been deleted.
	if c.Size() != 3 {
		t.Errorf("expected cache to have 3 records, got %v", c.Size())
	}
}

func TestGetFetchBatchConvertsDeletedRecordsToMissingRecords(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Second
	synchronousRefreshDelay := time.Minute
	refreshRetryInterval := time.Millisecond * 10

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	ids := []string{"1", "2", "3"}
	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)

	c.GetOrFetchBatch(ctx, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	time.Sleep(time.Millisecond * 10)

	if c.Size() != 3 {
		t.Errorf("expected cache to have 3 records, got %v", c.Size())
	}

	if _, ok := c.Get(c.BatchKeyFn("item")("3")); !ok {
		t.Errorf("expected key3 to exist in the cache")
	}

	// Now we're going to go past the refresh delay, and
	// only return the first 2 records from the source.
	fetchObserver.BatchResponse([]string{"1", "2"})
	clock.Add(maxRefreshDelay + 1)
	c.GetOrFetchBatch(ctx, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
	time.Sleep(time.Millisecond * 10)

	// The record should now exist as a missing record. Hence, the size should still be
	// 3. However, we should not be able to pull this item out of the cache using Get.
	if c.Size() != 3 {
		t.Errorf("expected cache to have 3 records, got %v", c.Size())
	}
	if _, ok := c.Get(c.BatchKeyFn("item")("3")); ok {
		t.Errorf("expected key3 to not be returned by Get")
	}
}

func TestGetFetchSynchronousRefreshes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	minBackgroundRefreshDelay := time.Second
	maxBackgroundRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minBackgroundRefreshDelay, maxBackgroundRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1)
	fetchObserver.Response(id)

	res, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value1" {
		t.Errorf("expected value1, got %v", res)
	}

	// Now, let's make the fetchObserver return a new value, and only move the
	// clock enough to warrant a background refresh. The value we get should
	// still be the same as the previous one because the refresh happens in the
	// background.
	fetchObserver.Clear()
	fetchObserver.Response("2")
	clock.Add(maxBackgroundRefreshDelay + 1)
	res, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value1" {
		t.Errorf("expected value1, got %v", res)
	}

	// Now we can wait for the background refresh to complete, and then assert
	// that the next time we ask for this ID we'll get the new value.
	time.Sleep(time.Millisecond * 100)
	res, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value2" {
		t.Errorf("expected value2, got %v", res)
	}

	// Let's do this again, but this time we'll move the clock passed the synchronous refresh delay.
	// This should result in a synchronous refresh and we should get the new value right away.
	fetchObserver.Clear()
	fetchObserver.Response("3")
	clock.Add(synchronousRefreshDelay + 1)
	res, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value3" {
		t.Errorf("expected value3, got %v", res)
	}
	fetchObserver.AssertFetchCount(t, 3)
}

func TestGetFetchBatchSynchronousRefreshes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	minBackgroundRefreshDelay := time.Second
	maxBackgroundRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minBackgroundRefreshDelay, maxBackgroundRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	firstBatchOfIDs := []string{"1", "2", "3"}
	fetchObserver := NewFetchObserver(2)
	fetchObserver.BatchResponse(firstBatchOfIDs)

	_, err := sturdyc.GetOrFetchBatch(ctx, c, firstBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	fetchObserver.AssertRequestedRecords(t, firstBatchOfIDs)
	fetchObserver.AssertFetchCount(t, 1)

	// Now, let's move the clock 5 seconds and then request another batch of IDs.
	clock.Add(time.Second * 5)
	secondBatchOfIDs := []string{"4", "5", "6"}
	fetchObserver.BatchResponse(secondBatchOfIDs)
	_, err = sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	fetchObserver.AssertRequestedRecords(t, secondBatchOfIDs)
	fetchObserver.AssertFetchCount(t, 2)

	// At this point, we should have IDs 1-3 in the cache that are 5  seconds
	// old, and IDs 4-6 that are completely new. If we now move the clock another
	// 5 seconds, we should reach the point where IDs 1-3 are due for a
	// synchronous refresh, and IDs 4-6 are due for a background refresh.
	clock.Add((time.Second * 5) + 1)

	fullBatchOfIDs := []string{"1", "2", "3", "4", "5", "6"}
	fetchObserver.BatchResponse(firstBatchOfIDs)
	_, err = sturdyc.GetOrFetchBatch(ctx, c, fullBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	// We'll assert that two refreshes happened. One synchronous refresh and one background refresh.
	<-fetchObserver.FetchCompleted
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	fetchObserver.AssertFetchCount(t, 4)
}

func TestGetFetchSynchronousRefreshConvertsToMissingRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	minBackgroundRefreshDelay := time.Second
	maxBackgroundRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minBackgroundRefreshDelay, maxBackgroundRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1)
	fetchObserver.Response(id)

	res, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value1" {
		t.Errorf("expected value1, got %v", res)
	}

	// Here, we'll set up the next request to return a not found error. Given
	// that we have missing record storage enabled, we'll expect that the
	// synchronous refresh returns a sturdyc.MissingRecord error.
	fetchObserver.Clear()
	fetchObserver.Err(sturdyc.ErrNotFound)
	clock.Add(synchronousRefreshDelay + 1)

	_, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	if !errors.Is(err, sturdyc.ErrMissingRecord) {
		t.Fatalf("expected ErrMissingRecord, got %v", err)
	}
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
	if c.Size() != 1 {
		t.Errorf("expected cache size to be 1, got %d", c.Size())
	}

	// Let's also make sure that the record can reappear again.
	fetchObserver.Clear()
	fetchObserver.Response("2")
	clock.Add(synchronousRefreshDelay + 1)

	res, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value2" {
		t.Errorf("expected value2, got %v", res)
	}
	fetchObserver.AssertFetchCount(t, 3)
}

func TestGetFetchBatchSynchronousRefreshConvertsToMissingRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	minBackgroundRefreshDelay := time.Second
	maxBackgroundRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minBackgroundRefreshDelay, maxBackgroundRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	ids := []string{"1", "2", "3", "4", "5", "6"}
	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)

	res, err := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 6 {
		t.Fatalf("expected 6 records, got %d", len(res))
	}
	if c.Size() != 6 {
		t.Errorf("expected cache size to be 6, got %d", c.Size())
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)

	// Now, let's move the clock passed the synchronous refresh delay,
	// and make the refresh only return values for IDs 1-3.
	clock.Add(synchronousRefreshDelay + 1)
	fetchObserver.Clear()
	fetchObserver.BatchResponse([]string{"1", "2", "3"})

	res, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}
	if c.Size() != 6 {
		t.Errorf("expected cache size to be 6, got %d", c.Size())
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 2)

	// Next, let's assert that the records were successfully stored as missing.
	clock.Add(minBackgroundRefreshDelay - 1)
	res, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}
	if c.Size() != 6 {
		t.Errorf("expected cache size to be 6, got %d", c.Size())
	}

	// And finally, let's make sure that the records can reappear again.
	clock.Add(synchronousRefreshDelay)
	fetchObserver.Clear()
	fetchObserver.BatchResponse(ids)

	res, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 6 {
		t.Fatalf("expected 6 records, got %d", len(res))
	}
	if c.Size() != 6 {
		t.Errorf("expected cache size to be 6, got %d", c.Size())
	}
	fetchObserver.AssertRequestedRecords(t, ids)
}

func TestGetFetchSynchronousRefreshDeletion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	minBackgroundRefreshDelay := time.Second
	maxBackgroundRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minBackgroundRefreshDelay, maxBackgroundRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1)
	fetchObserver.Response(id)

	res, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value1" {
		t.Errorf("expected value1, got %v", res)
	}

	// Here, we'll set up the next request to return a not found error. Given
	// that we have missing record storage enabled, we'll expect that the
	// synchronous refresh returns a sturdyc.MissingRecord error.
	fetchObserver.Clear()
	fetchObserver.Err(sturdyc.ErrNotFound)
	clock.Add(synchronousRefreshDelay + 1)

	_, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	if !errors.Is(err, sturdyc.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
	if c.Size() != 0 {
		t.Errorf("expected cache size to be 0, got %d", c.Size())
	}

	// Let's also make sure that the record can reappear again.
	clock.Add(synchronousRefreshDelay)
	fetchObserver.Clear()
	fetchObserver.Response(id)
	res, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value1" {
		t.Errorf("expected value1, got %v", res)
	}
	if c.Size() != 1 {
		t.Errorf("expected cache size to be 1, got %d", c.Size())
	}
	fetchObserver.AssertFetchCount(t, 3)
}

func TestGetFetchBatchSynchronousRefreshDeletion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	minBackgroundRefreshDelay := time.Second
	maxBackgroundRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minBackgroundRefreshDelay, maxBackgroundRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithClock(clock),
	)

	ids := []string{"1", "2", "3", "4", "5", "6"}
	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)

	res, err := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 6 {
		t.Fatalf("expected 6 records, got %d", len(res))
	}
	if c.Size() != 6 {
		t.Errorf("expected cache size to be 6, got %d", c.Size())
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)

	// Now, let's move the clock passed the synchronous refresh delay,
	// and make the refresh only return values for IDs 1-3.
	clock.Add(synchronousRefreshDelay + 1)
	fetchObserver.Clear()
	fetchObserver.BatchResponse([]string{"1", "2", "3"})

	res, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}
	// IDs 4-6 should not have been deleted.
	if c.Size() != 3 {
		t.Errorf("expected cache size to be 3, got %d", c.Size())
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 2)

	// Next, let's assert that the records doesn't reappear the next time we ask for the same IDs.
	fetchObserver.Clear()
	fetchObserver.BatchResponse([]string{})
	clock.Add(minBackgroundRefreshDelay - 1)

	res, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}
	if c.Size() != 3 {
		t.Errorf("expected cache size to be 3, got %d", c.Size())
	}
	// IDs 4-6 should have been deleted, hence we should get another outgoing request.
	fetchObserver.AssertFetchCount(t, 3)
	fetchObserver.AssertRequestedRecords(t, []string{"4", "5", "6"})

	// Finally, let's make sure that the records can reappear again.
	clock.Add(synchronousRefreshDelay)
	fetchObserver.Clear()
	fetchObserver.BatchResponse(ids)

	res, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 6 {
		t.Fatalf("expected 6 records, got %d", len(res))
	}
	if c.Size() != 6 {
		t.Errorf("expected cache size to be 6, got %d", c.Size())
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 4)
}

func TestGetFetchSynchronousRefreshFailureGivesLatestValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	minBackgroundRefreshDelay := time.Second
	maxBackgroundRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minBackgroundRefreshDelay, maxBackgroundRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1)
	fetchObserver.Response(id)

	res, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value1" {
		t.Errorf("expected value1, got %v", res)
	}

	// Here, we'll set up the next request to return an error. Given that
	// we still have this key cached, we expect the cache to give us an
	// ErrOnlyCachedRecords error along with the cached value.
	fetchObserver.Clear()
	fetchObserver.Err(errors.New("error"))
	clock.Add(synchronousRefreshDelay + 1)

	res, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected ErrOnlyCachedRecords, got %v", err)
	}
	if res != "value1" {
		t.Errorf("expected value1, got %v", res)
	}
	fetchObserver.AssertFetchCount(t, 2)

	// Now, requesting the same ID again should result in another synchronous
	// refresh without us having to move the clock. Let's set this one up to return
	// an actual value.
	fetchObserver.Clear()
	fetchObserver.Response("2")

	res, err = sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value2" {
		t.Errorf("expected value2, got %v", res)
	}
	fetchObserver.AssertFetchCount(t, 3)
}

func TestGetFetchBatchSynchronousRefreshFailureGivesLatestValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	minBackgroundRefreshDelay := time.Second
	maxBackgroundRefreshDelay := time.Second * 2
	synchronousRefreshDelay := time.Second * 10
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minBackgroundRefreshDelay, maxBackgroundRefreshDelay, synchronousRefreshDelay, retryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	ids := []string{"1", "2", "3", "4", "5"}
	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)

	res, err := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 5 {
		t.Fatalf("expected 5 records, got %d", len(res))
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)

	// Now, let's move the clock passed the synchronous refresh
	// delay, and make the next call return an error.
	clock.Add(synchronousRefreshDelay + 1)
	fetchObserver.Clear()
	fetchObserver.Err(errors.New("error"))

	res, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 5 {
		t.Fatalf("expected 5 records, got %d", len(res))
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 2)

	// If a synchronous refresh fails, we won't do any exponential backoff.
	clock.Add(time.Millisecond)
	res, err = sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if !errors.Is(err, sturdyc.ErrOnlyCachedRecords) {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 5 {
		t.Fatalf("expected 5 records, got %d", len(res))
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 3)
}

func TestGetFetchSynchronousRefreshStampedeProtection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10
	numShards := 2
	ttl := time.Second * 2
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Millisecond * 500
	synchronousRefreshDelay := time.Second
	refreshRetryInterval := time.Millisecond * 10

	// The cache is going to have a 2 second TTL, and the first refresh should happen within a second.
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	id := "1"
	fetchObserver := NewFetchObserver(1000)
	fetchObserver.Response(id)

	// We will start the test by trying to get key1, which wont exist in the sturdyc. Hence,
	// the fetch function is going to get called and we'll set the initial value to val1.
	sturdyc.GetOrFetch[string](ctx, c, id, fetchObserver.Fetch)

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)

	// Now, we're going to go past the synchronous refresh delay and try to retrieve the key from 1000 goroutines at once.
	numGoroutines := 1000
	clock.Add(synchronousRefreshDelay + 1)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := sturdyc.GetOrFetch(ctx, c, id, fetchObserver.Fetch)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
}

func TestGetFetchBatchSynchronousRefreshStampedeProtection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10
	numShards := 2
	ttl := time.Second * 2
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Millisecond * 500
	synchronousRefreshDelay := time.Second
	refreshRetryInterval := time.Millisecond * 10

	// The cache is going to have a 2 second TTL, and the first refresh should happen within a second.
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithClock(clock),
	)

	ids := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)

	res, err := sturdyc.GetOrFetchBatch(ctx, c, ids, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 10 {
		t.Fatalf("expected 10 records, got %d", len(res))
	}
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.Clear()

	// Now, we're going to go past the synchronous refresh delay and try to
	// retrieve 3 random keys (without duplicates) from 1000 goroutines at once.
	// In an ideal world, this should lead to 4 outgoing requests, e.g:
	// 1, 2, 3
	// 4, 5, 6
	// 7, 8, 9
	// However, we're not using delaying these requests, hence we could get a
	// maximum of 10 outgoing requests if the batches were to get spread out
	// something like this:
	// 1, 2, 3
	// 1, 2, 4
	// 1, 2, 5
	// 1, 2, 6
	// 1, 2, 7
	// 1, 2, ...
	clock.Add(synchronousRefreshDelay + 1)
	numGoroutines := 1000
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// We need to create another fetch mock because the fetchObserver resolves
	// the response immediately. However, we want to delay the response in order to
	// check that the deduplication works as expected. If we don't delay the
	// function responding, we'll have other goroutines with synchronous refresh
	// ids that are going to send of another request right after.
	signal := make(chan struct{})
	var callCount atomic.Int32
	fetchMock := func(_ context.Context, ids []string) (map[string]string, error) {
		<-signal
		callCount.Add(1)
		responseMap := make(map[string]string, len(ids))
		for _, id := range ids {
			responseMap[id] = "value" + id
		}
		return responseMap, nil
	}

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			uniqueIDs := make(map[string]struct{})
			for len(uniqueIDs) < 3 {
				id := ids[rand.IntN(len(ids))]
				if _, ok := uniqueIDs[id]; ok {
					continue
				}
				uniqueIDs[id] = struct{}{}
			}
			idsToFetch := make([]string, 0, 3)
			for id := range uniqueIDs {
				idsToFetch = append(idsToFetch, id)
			}
			res, err := sturdyc.GetOrFetchBatch(ctx, c, idsToFetch, c.BatchKeyFn("item"), fetchMock)
			if err != nil {
				panic(err)
			}
			if len(res) != 3 {
				panic("expected 3 records, got " + strconv.Itoa(len(res)))
			}
			for _, id := range idsToFetch {
				if _, ok := res[id]; !ok {
					panic("expected id " + id + " to be in the response")
				}
			}
		}()
	}
	// Allow all of the goroutines to start and get some CPU time.
	time.Sleep(time.Millisecond * 500)
	// Now, we'll close the channel which should give all of the goroutines their response.
	close(signal)
	// Wait for the wait group so that we can run the assertions within the goroutines.
	wg.Wait()
	if callCount.Load() > 10 {
		t.Errorf("expected no more than 10 calls, got %d", callCount.Load())
	}
}

func TestGetFetchBatchMixOfSynchronousAndAsynchronousRefreshes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10
	numShards := 2
	ttl := time.Second * 2
	evictionPercentage := 10
	clock := sturdyc.NewTestClock(time.Now())
	minRefreshDelay := time.Millisecond * 500
	maxRefreshDelay := time.Millisecond * 500
	synchronousRefreshDelay := time.Second
	refreshRetryInterval := time.Millisecond * 10
	batchSize := 20
	batchBufferTimeout := time.Millisecond * 50

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithRefreshCoalescing(batchSize, batchBufferTimeout),
		sturdyc.WithClock(clock),
	)

	// We'll start by fetching one batch of IDs, and make some assertions.
	firstBatchOfIDs := []string{"1", "2", "3"}
	fetchObserver := NewFetchObserver(2)
	fetchObserver.BatchResponse(firstBatchOfIDs)
	res, err := sturdyc.GetOrFetchBatch(ctx, c, firstBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}
	fetchObserver.AssertRequestedRecords(t, firstBatchOfIDs)
	fetchObserver.Clear()

	// Next, we'll move the clock past the synchronous refresh delay, and
	// make a call for a second batch of IDs. The first batch of IDs should
	// now be a second old, and  due for a synchronous refresh the next time
	// any of the IDs are requested.
	clock.Add(synchronousRefreshDelay + 1)
	secondBatchOfIDs := []string{"4", "5", "6"}
	fetchObserver.BatchResponse(secondBatchOfIDs)
	res, err = sturdyc.GetOrFetchBatch(ctx, c, secondBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}
	fetchObserver.AssertRequestedRecords(t, secondBatchOfIDs)
	fetchObserver.Clear()

	// Now we'll move the clock passed the maxRefreshDelay, which should make
	// the second batch of IDs due for a refresh, but not a synchronous one.
	clock.Add(maxRefreshDelay + 1)

	// Here we create a third batch of IDs which contains one of the IDs from the
	// first batch, and another ID from the second batch, and an additional ID
	// that we haven't seen before.
	thirdBatchOfIDs := []string{"1", "4", "23"}
	fetchObserver.BatchResponse([]string{"1", "23"})
	res, err = sturdyc.GetOrFetchBatch(ctx, c, thirdBatchOfIDs, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 records, got %d", len(res))
	}
	// We only expect to have called the underlying data source with the
	// ID from the first batch, and the ID we haven't seen before.
	fetchObserver.AssertRequestedRecords(t, []string{"1", "23"})
	fetchObserver.AssertFetchCount(t, 3)
	fetchObserver.Clear()

	// Since we're using the WithRefreshCoalescing option, the cache will have created
	// an internal buffer where it's trying to gather 10 IDs before sending them off
	// to the underlying data source. However, we're only asking for one ID from the
	// second batch. Therefore, we'll have to move the clock in order to make the cache
	// exceed the buffering timeout.
	fetchObserver.BatchResponse([]string{"4"})
	// Give the buffering goroutine a chance to run before we move the clock.
	time.Sleep(time.Millisecond * 200)
	clock.Add(batchBufferTimeout + 1)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertRequestedRecords(t, []string{"4"})
	fetchObserver.AssertFetchCount(t, 4)
}

func TestGetOrFetchGenerics(t *testing.T) {
	t.Parallel()

	capacity := 10
	numShards := 2
	ttl := time.Second * 2
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)

	fetchFuncInt := func(_ context.Context) (int, error) {
		return 1, nil
	}

	_, err := sturdyc.GetOrFetch(context.Background(), c, "1", fetchFuncInt)
	if !errors.Is(err, sturdyc.ErrInvalidType) {
		t.Errorf("expected ErrInvalidType, got %v", err)
	}

	if _, ok := c.Get("1"); ok {
		t.Error("we should not have cached anything given that the value was of the wrong type")
	}

	if c.Size() != 0 {
		t.Errorf("expected cache size to be 0, got %d", c.Size())
	}

	fetchFuncString := func(_ context.Context) (string, error) {
		return "value", nil
	}

	val, err := sturdyc.GetOrFetch(context.Background(), c, "1", fetchFuncString)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "value" {
		t.Errorf("expected value to be value, got %v", val)
	}

	cachedValue, ok := c.Get("1")
	if !ok {
		t.Error("expected value to be in the cache")
	}
	if cachedValue != "value" {
		t.Errorf("expected value to be value, got %v", cachedValue)
	}
	if c.Size() != 1 {
		t.Errorf("expected cache size to be 1, got %d", c.Size())
	}
}

func TestGetOrFetchBatchGenerics(t *testing.T) {
	t.Parallel()

	capacity := 10
	numShards := 2
	ttl := time.Second * 2
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)

	fetchFuncInt := func(_ context.Context, ids []string) (map[string]int, error) {
		response := make(map[string]int, len(ids))
		for i, id := range ids {
			response[id] = i
		}
		return response, nil
	}

	ids := []string{"1", "2", "3"}
	_, err := sturdyc.GetOrFetchBatch(context.Background(), c, ids, c.BatchKeyFn("item"), fetchFuncInt)
	if !errors.Is(err, sturdyc.ErrInvalidType) {
		t.Errorf("expected ErrInvalidType, got %v", err)
	}
	if c.Size() != 0 {
		t.Errorf("expected cache size to be 0, got %d", c.Size())
	}

	fetchFuncString := func(_ context.Context, ids []string) (map[string]string, error) {
		response := make(map[string]string, len(ids))
		for _, id := range ids {
			response[id] = "value" + id
		}
		return response, nil
	}

	keyFunc := c.BatchKeyFn("item")
	values, err := sturdyc.GetOrFetchBatch(context.Background(), c, ids, keyFunc, fetchFuncString)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(values) != 3 {
		t.Errorf("expected 3 values, got %d", len(values))
	}
	if values["1"] != "value1" {
		t.Errorf("expected value to be value1, got %v", values["1"])
	}
	if values["2"] != "value2" {
		t.Errorf("expected value to be value2, got %v", values["2"])
	}
	if values["3"] != "value3" {
		t.Errorf("expected value to be value3, got %v", values["3"])
	}

	cacheKeys := make([]string, 0, len(ids))
	for _, id := range ids {
		cacheKeys = append(cacheKeys, keyFunc(id))
	}
	cachedValues := c.GetMany(cacheKeys)
	if len(cachedValues) != 3 {
		t.Errorf("expected 3 values, got %d", len(cachedValues))
	}
	if cachedValues[cacheKeys[0]] != "value1" {
		t.Errorf("expected value to be value1, got %v", cachedValues["1"])
	}
	if cachedValues[cacheKeys[1]] != "value2" {
		t.Errorf("expected value to be value2, got %v", cachedValues["2"])
	}
	if cachedValues[cacheKeys[2]] != "value3" {
		t.Errorf("expected value to be value3, got %v", cachedValues["3"])
	}
}

func TestGetOrFetchGenericsClashingTypes(t *testing.T) {
	t.Parallel()

	capacity := 10
	numShards := 2
	ttl := time.Second * 2
	evictionPercentage := 10
	c := sturdyc.New[any](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)

	resolve := make(chan struct{})
	fetchFuncInt := func(_ context.Context) (int, error) {
		<-resolve
		return 1, nil
	}
	fetchFuncString := func(_ context.Context) (string, error) {
		return "value", nil
	}

	go func() {
		time.Sleep(time.Millisecond * 500)
		resolve <- struct{}{}
	}()
	resOne, errOne := sturdyc.GetOrFetch(context.Background(), c, "1", fetchFuncInt)
	_, errTwo := sturdyc.GetOrFetch(context.Background(), c, "1", fetchFuncString)

	if errOne != nil {
		t.Errorf("expected no error, got %v", errOne)
	}
	if resOne != 1 {
		t.Errorf("expected value to be 1, got %v", resOne)
	}

	if !errors.Is(errTwo, sturdyc.ErrInvalidType) {
		t.Errorf("expected ErrInvalidType, got %v", errTwo)
	}
}

func TestGetOrFetchBatchGenericsClashingTypes(t *testing.T) {
	t.Parallel()

	capacity := 10
	numShards := 2
	ttl := time.Second * 2
	evictionPercentage := 10
	c := sturdyc.New[any](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)

	// We are going to test the behaviour for when the same IDs are passed to
	// fetch functions which return different types.
	cacheKeyFunc := c.BatchKeyFn("item")
	firstCallIDs := []string{"1", "2", "3"}
	secondCallIDs := []string{"1", "2", "3", "4"}

	// First, we'll create a fetch function which returns integers. This fetch
	// function is going to wait for a message on the resolve channel before
	// returning. This is so that we're able to create an in-flight batch for the
	// first set of IDs.
	resolve := make(chan struct{})
	fetchFuncInt := func(_ context.Context, ids []string) (map[string]int, error) {
		if len(ids) != 3 {
			t.Fatalf("expected 3 IDs, got %d", len(ids))
		}
		response := make(map[string]int, len(ids))
		for i, id := range ids {
			response[id] = i + 1
		}
		<-resolve
		return response, nil
	}

	// Next, we'll create a fetch function which returns strings. This fetch
	// function should only be called with ID "4" because IDs 1-3 are picked from
	// the first batch.
	fetchFuncString := func(_ context.Context, ids []string) (map[string]string, error) {
		if len(ids) != 1 {
			t.Fatalf("expected 1 ID, got %d", len(ids))
		}
		response := make(map[string]string, len(ids))
		for _, id := range ids {
			response[id] = "value" + id
		}
		return response, nil
	}

	go func() {
		time.Sleep(time.Millisecond * 500)
		resolve <- struct{}{}
	}()
	firstCallValues, firstCallErr := sturdyc.GetOrFetchBatch(context.Background(), c, firstCallIDs, cacheKeyFunc, fetchFuncInt)
	_, secondCallErr := sturdyc.GetOrFetchBatch(context.Background(), c, secondCallIDs, cacheKeyFunc, fetchFuncString)

	if firstCallErr != nil {
		t.Errorf("expected no error, got %v", firstCallErr)
	}
	if len(firstCallValues) != 3 {
		t.Errorf("expected 3 values, got %d", len(firstCallValues))
	}
	if firstCallValues["1"] != 1 {
		t.Errorf("expected value to be 1, got %v", firstCallValues["1"])
	}
	if firstCallValues["2"] != 2 {
		t.Errorf("expected value to be 2, got %v", firstCallValues["2"])
	}
	if firstCallValues["3"] != 3 {
		t.Errorf("expected value to be 3, got %v", firstCallValues["3"])
	}

	if !errors.Is(secondCallErr, sturdyc.ErrInvalidType) {
		t.Errorf("expected ErrInvalidType, got %v", secondCallErr)
	}
}
