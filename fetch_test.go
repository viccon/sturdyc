package sturdyc_test

import (
	"context"
	"errors"
	"sync"
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
	refreshRetryInterval := time.Millisecond * 10

	// The cache is going to have a 2 second TTL, and the first refresh should happen within a second.
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, refreshRetryInterval),
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
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, retryInterval),
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
	retryInterval := time.Millisecond * 10
	clock := sturdyc.NewTestClock(time.Now())
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, retryInterval),
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
	minRefreshDelay := time.Minute
	maxRefreshDelay := time.Minute * 2
	retryInterval := time.Second
	clock := sturdyc.NewTestClock(time.Now())
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, retryInterval),
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
	retryInterval := time.Second
	clock := sturdyc.NewTestClock(time.Now())
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, retryInterval),
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
	refreshRetryInterval := time.Millisecond * 10
	c := sturdyc.New[string](capacity, shards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, refreshRetryInterval),
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
	refreshRetryInterval := time.Millisecond * 10

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, refreshRetryInterval),
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
	refreshRetryInterval := time.Millisecond * 10

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, refreshRetryInterval),
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
	refreshRetryInterval := time.Millisecond * 10

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, refreshRetryInterval),
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
	refreshRetryInterval := time.Millisecond * 10

	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, refreshRetryInterval),
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
