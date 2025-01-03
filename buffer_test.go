package sturdyc_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

func TestBatchIsRefreshedWhenTheTimeoutExpires(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	ttl := time.Hour
	numShards := 100
	evictionPercentage := 10
	minRefreshDelay := time.Minute * 5
	maxRefreshDelay := time.Minute * 10
	synchronousRefreshDelay := time.Minute * 30
	refreshRetryInterval := time.Millisecond * 10
	batchSize := 10
	batchBufferTimeout := time.Minute
	clock := sturdyc.NewTestClock(time.Now())

	// The client will be configured as follows:
	// - Records will be assigned a TTL of one hour.
	// - If a record is re-requested within a random interval of 5 to
	//   10 minutes, the client will queue a refresh for that record.
	// - The queued refresh will be executed under two conditions:
	//    1. The number of scheduled refreshes exceeds the specified 'batchSize'.
	//    2. The 'batchBufferTimeout' threshold is exceeded.
	client := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithRefreshCoalescing(batchSize, batchBufferTimeout),
		sturdyc.WithClock(clock),
	)

	// Populate the cache with 100 records.
	ids := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		ids = append(ids, strconv.Itoa(i))
	}

	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)
	sturdyc.GetOrFetchBatch(ctx, client, ids, client.BatchKeyFn("item"), fetchObserver.FetchBatch)

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.Clear()

	// Next, we'll move the clock past the maxRefreshDelay. This should guarantee
	// that the next records we request gets scheduled for a refresh.
	clock.Add(maxRefreshDelay + time.Second)

	// We'll create a batch function that stores the ids it was called with, and
	// then invoke "GetOrFetchBatch". We are going to request 3 ids, which is less
	// than our wanted batch size. This should lead to a batch being scheduled.
	recordsToRequest := []string{"1", "2", "3"}
	fetchObserver.BatchResponse(recordsToRequest)
	sturdyc.GetOrFetchBatch(ctx, client, recordsToRequest, client.BatchKeyFn("item"), fetchObserver.FetchBatch)
	time.Sleep(10 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 1)

	// Now, we'll move the clock forward passed the "batchBufferTimeout". This should
	// trigger a refresh even though we have less records than our wanted buffer size.
	clock.Add(batchBufferTimeout + 1)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
	fetchObserver.AssertRequestedRecords(t, recordsToRequest)
}

func TestBatchIsRefreshedWhenTheBufferSizeIsReached(t *testing.T) {
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
	clock := sturdyc.NewTestClock(time.Now())

	// The client will be configured as follows:
	// - Records will be assigned a TTL of one hour.
	// - If a record is re-requested within a random interval of 5 to
	//   10 minutes, the client will queue a refresh for that record.
	// - The queued refresh will be executed under two conditions:
	//    1. The number of scheduled refreshes exceeds the specified 'batchSize'.
	//    2. The 'batchBufferTimeout' threshold is exceeded.
	client := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithRefreshCoalescing(batchSize, batchBufferTimeout),
		sturdyc.WithClock(clock),
	)

	ids := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		ids = append(ids, strconv.Itoa(i))
	}

	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)
	sturdyc.GetOrFetchBatch(ctx, client, ids, client.BatchKeyFn("item"), fetchObserver.FetchBatch)

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.Clear()

	// Next, we'll move the clock past the maxRefreshDelay. This should guarantee
	// that the next records we request gets scheduled for a refresh.
	clock.Add(maxRefreshDelay + time.Second)

	// We'll create a batch function that stores the ids it was called with, and
	// then invoke "GetOrFetchBatch". We are going to request 3 ids, which is less
	// than our ideal batch size. This should lead to a batch being scheduled.
	firstBatchOfRequestedRecords := []string{"1", "2", "3"}
	fetchObserver.BatchResponse([]string{"1", "2", "3"})
	sturdyc.GetOrFetchBatch(ctx, client, firstBatchOfRequestedRecords, client.BatchKeyFn("item"), fetchObserver.FetchBatch)

	// Now, we'll move the clock forward 5 seconds before requesting another 3 records.
	// Our wanted batch size is 10, hence this should NOT be enough to trigger a refresh.
	clock.Add(5 * time.Second)
	secondBatchOfRecords := []string{"4", "5", "6"}
	sturdyc.GetOrFetchBatch(ctx, client, secondBatchOfRecords, client.BatchKeyFn("item"), fetchObserver.FetchBatch)

	// Move the clock another 10 seconds. Again, this should not trigger a refresh. We'll
	// perform a sleep here too just to ensure that the buffer is not refreshed prematurely.
	clock.Add(10 * time.Second)
	time.Sleep(5 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 1)

	// In the the third batch I'm going to request 6 records. With that, we've
	// requested 12 record in total, which is greater than our buffer size of 10.
	thirdBatchOfRecords := []string{"7", "8", "9", "10", "11", "12"}
	sturdyc.GetOrFetchBatch(ctx, client, thirdBatchOfRecords, client.BatchKeyFn("item"), fetchObserver.FetchBatch)

	// An actual refresh should happen for the first 10 ids, while the 2 that
	// overflows should get scheduled for a refresh. Block until the request has
	// happened, and then assert that it received the expected batch.
	wantedRequestedRecords := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
	fetchObserver.AssertRequestedRecords(t, wantedRequestedRecords)
	fetchObserver.Clear()

	// Lastly, we'll move the clock passed the "batchBufferTimeout" to assert
	// that the overflowing records were requested when the timer expired. We
	// have to sleep here because we only know that the goroutine that is going
	// to listen on the timer has been launched, but it might not have reached
	// the line where it creates a ticker yet. Hence, we'll sleep just to make
	// sure it's attached before moving the clock.
	time.Sleep(30 * time.Millisecond)
	clock.Add(batchBufferTimeout + time.Second)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 3)
	fetchObserver.AssertRequestedRecords(t, []string{"11", "12"})
}

func TestBatchIsNotRefreshedByDuplicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 100
	ttl := time.Hour
	evictionPercentage := 10
	minRefreshDelay := time.Minute * 5
	maxRefreshDelay := time.Minute * 10
	synchronousRefreshDelay := time.Minute * 30
	refreshRetryInterval := time.Millisecond * 10
	batchSize := 10
	batchBufferTimeout := time.Minute
	clock := sturdyc.NewTestClock(time.Now())

	// The client will be configured as follows:
	// - Records will be assigned a TTL of one hour.
	// - If a record is re-requested within a random interval of 5 to
	//   10 minutes, the client will queue a refresh for that record.
	// - The queued refresh will be executed under two conditions:
	//    1. The number of scheduled refreshes exceeds the specified 'batchSize'.
	//    2. The 'batchBufferTimeout' threshold is exceeded.
	client := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithRefreshCoalescing(batchSize, batchBufferTimeout),
		sturdyc.WithClock(clock),
	)

	// Populate the cache with 100 records.
	ids := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		ids = append(ids, strconv.Itoa(i))
	}

	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(ids)
	sturdyc.GetOrFetchBatch(ctx, client, ids, client.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.AssertRequestedRecords(t, ids)
	fetchObserver.Clear()

	// Next, we're going to go past the maxRefreshDelay and request id 1, 2, and
	// 3 100 times. Because we're only requesting the same records over and over
	// again, we don't expect any outgoing requests.
	clock.Add(maxRefreshDelay + time.Second)
	numRequests := 100
	wg := sync.WaitGroup{}
	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		go func(i int) {
			id := []string{strconv.Itoa((i % 3) + 1)}
			sturdyc.GetOrFetchBatch(ctx, client, id, client.BatchKeyFn("item"), fetchObserver.FetchBatch)
			wg.Done()
		}(i)
	}
	wg.Wait()
	time.Sleep(5 * time.Millisecond)
	fetchObserver.AssertFetchCount(t, 1)

	// Now, we'll move the clock forward passed the "batchBufferTimeout".
	fetchObserver.BatchResponse([]string{"1", "2", "3"})
	clock.Add(batchBufferTimeout + time.Second)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
	fetchObserver.AssertRequestedRecords(t, []string{"1", "2", "3"})
}

func TestBatchesAreGroupedByPermutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 100
	ttl := time.Hour
	evictionPercentage := 15
	minRefreshDelay := time.Minute * 5
	maxRefreshDelay := time.Minute * 10
	synchronousRefreshDelay := time.Minute * 30
	refreshRetryInterval := time.Millisecond * 10
	batchSize := 5
	batchBufferTimeout := time.Minute
	clock := sturdyc.NewTestClock(time.Now())

	// The c will be configured as follows:
	// - Records will be assigned a TTL of one hour.
	// - If a record is re-requested within a random interval of 5 to
	//   10 minutes, the c will queue a refresh for that record.
	// - The queued refresh will be executed under two conditions:
	//    1. The number of scheduled refreshes exceeds the specified 'batchSize'.
	//    2. The 'batchBufferTimeout' threshold is exceeded.
	c := sturdyc.New[any](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithRefreshCoalescing(batchSize, batchBufferTimeout),
		sturdyc.WithClock(clock),
	)

	// We are going to seed the cache by adding ids 1-10 but with varying options.
	prefix := "item"
	type QueryParams struct {
		IncludeUpcoming bool
		ProductGroupIDs []string
		SortOrder       string
	}
	optsOne := QueryParams{IncludeUpcoming: true, ProductGroupIDs: []string{"1", "2", "3"}, SortOrder: "asc"}
	optsTwo := QueryParams{IncludeUpcoming: false, ProductGroupIDs: []string{"4", "5", "6"}, SortOrder: "desc"}

	seedIDs := make([]string, 0, 10)
	for i := 1; i <= 10; i++ {
		seedIDs = append(seedIDs, strconv.Itoa(i))
	}

	fetchObserver := NewFetchObserver(1)
	fetchObserver.BatchResponse(seedIDs)
	sturdyc.GetOrFetchBatch(ctx, c, seedIDs, c.PermutatedBatchKeyFn(prefix, optsOne), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	sturdyc.GetOrFetchBatch(ctx, c, seedIDs, c.PermutatedBatchKeyFn(prefix, optsTwo), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 2)
	fetchObserver.Clear()

	// Next, we'll move the clock past the maxRefreshDelay. This should guarantee
	// that the next records we request gets scheduled for a refresh.
	clock.Add(maxRefreshDelay + time.Second)

	// For the records that were stored using optsOne, we'll only request 3
	// records. We'll have to move the clock later on to make them expire.
	optsOneIDs := []string{"1", "2", "3"}
	// For the records that were stored using optsTwo we'll request 5 records in 2 batches,
	// which should fill the buffer size. We'll want to assert that the batches were grouped
	// correctly based on the options used, not the sequence in which they were requested.
	optsTwoBatch1 := []string{"4", "5"}
	optsTwoBatch2 := []string{"6", "7", "8"}

	// Request the first batch of records. This should wait for additional IDs.
	sturdyc.GetOrFetchBatch(ctx, c, optsOneIDs, c.PermutatedBatchKeyFn(prefix, optsOne), fetchObserver.FetchBatch)

	// Next, we're requesting ids 4-8 with the second options which should exceed the buffer size for that permutation.
	fetchObserver.BatchResponse([]string{"4", "5", "6", "7", "8"})
	sturdyc.GetOrFetchBatch(ctx, c, optsTwoBatch1, c.PermutatedBatchKeyFn(prefix, optsTwo), fetchObserver.FetchBatch)
	sturdyc.GetOrFetchBatch(ctx, c, optsTwoBatch2, c.PermutatedBatchKeyFn(prefix, optsTwo), fetchObserver.FetchBatch)

	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 3)
	fetchObserver.AssertRequestedRecords(t, []string{"4", "5", "6", "7", "8"})
	fetchObserver.Clear()

	// IDs 1-3 should still be waiting because they were requested using
	// different options. We'll move the clock forward to make them expire.
	fetchObserver.BatchResponse(optsOneIDs)
	clock.Add(batchBufferTimeout + time.Second)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 4)
	fetchObserver.AssertRequestedRecords(t, optsOneIDs)
}

func TestLargeBatchesAreChunkedCorrectly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	numShards := 100
	ttl := time.Hour
	evictionPercentage := 23
	minRefreshDelay := time.Minute * 5
	maxRefreshDelay := time.Minute * 10
	synchronousRefreshDelay := time.Minute * 30
	refreshRetryInterval := time.Millisecond * 10
	batchSize := 5
	batchBufferTimeout := time.Minute
	clock := sturdyc.NewTestClock(time.Now())

	// The client will be configured as follows:
	// - Records will be assigned a TTL of one hour.
	// - If a record is re-requested within a random interval of 5 to
	//   10 minutes, the client will queue a refresh for that record.
	// - The queued refresh will be executed under two conditions:
	//    1. The number of scheduled refreshes exceeds the specified 'batchSize'.
	//    2. The 'batchBufferTimeout' threshold is exceeded.
	client := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithRefreshCoalescing(batchSize, batchBufferTimeout),
		sturdyc.WithClock(clock),
	)

	// We are going to seed the cache by adding ids 1-100.
	cacheKeyPrefix := "item"
	seedIDs := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		seedIDs = append(seedIDs, strconv.Itoa(i))
	}

	fetchObserver := NewFetchObserver(5)
	fetchObserver.BatchResponse(seedIDs)
	sturdyc.GetOrFetchBatch(ctx, client, seedIDs, client.BatchKeyFn(cacheKeyPrefix), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	fetchObserver.AssertFetchCount(t, 1)
	fetchObserver.AssertRequestedRecords(t, seedIDs)
	fetchObserver.Clear()

	// Next, we'll move the clock past the maxRefreshDelay. This should guarantee
	// that the next records we request gets scheduled for a refresh.
	clock.Add(maxRefreshDelay + time.Second*5)

	// Now we are going to request 50 items at once. The batch size is set to
	// 5, so this should be chunked internally into 10 separate batches.
	largeBatch := make([]string, 0, 50)
	for i := 1; i <= 50; i++ {
		largeBatch = append(largeBatch, strconv.Itoa(i))
	}
	sturdyc.GetOrFetchBatch(ctx, client, largeBatch, client.BatchKeyFn(cacheKeyPrefix), fetchObserver.FetchBatch)
	for i := 0; i < 10; i++ {
		<-fetchObserver.FetchCompleted
	}
	fetchObserver.AssertFetchCount(t, 11)
}

func TestValuesAreUpdatedCorrectly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 1000
	ttl := time.Hour
	numShards := 100
	evictionPercentage := 10
	minRefreshDelay := time.Minute * 5
	maxRefreshDelay := time.Minute * 10
	synchronousRefreshDelay := time.Minute * 50
	refreshRetryInterval := time.Millisecond * 10
	batchSize := 10
	batchBufferTimeout := time.Minute
	clock := sturdyc.NewTestClock(time.Now())

	// The client will be configured as follows:
	// - Records will be assigned a TTL of one hour.
	// - If a record is re-requested within a random interval of 5 to
	//   10 minutes, the client will queue a refresh for that record.
	// - The queued refresh will be executed under two conditions:
	//    1. The number of scheduled refreshes exceeds the specified 'batchSize'.
	//    2. The 'batchBufferTimeout' threshold is exceeded.
	client := sturdyc.New[any](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, refreshRetryInterval),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithRefreshCoalescing(batchSize, batchBufferTimeout),
		sturdyc.WithClock(clock),
	)

	type Foo struct {
		Value string
	}

	records := []string{"1", "2", "3"}
	res, _ := sturdyc.GetOrFetchBatch[Foo](ctx, client, records, client.BatchKeyFn("item"), func(_ context.Context, ids []string) (map[string]Foo, error) {
		values := make(map[string]Foo, len(ids))
		for _, id := range ids {
			values[id] = Foo{Value: "foo-" + id}
		}
		return values, nil
	})

	if res["1"].Value != "foo-1" {
		t.Errorf("expected 'foo-1', got '%s'", res["1"].Value)
	}

	clock.Add(time.Minute * 45)
	sturdyc.GetOrFetchBatch[Foo](ctx, client, records, client.BatchKeyFn("item"), func(_ context.Context, ids []string) (map[string]Foo, error) {
		values := make(map[string]Foo, len(ids))
		for _, id := range ids {
			values[id] = Foo{Value: "foo2-" + id}
		}
		return values, nil
	})

	// Flush out timers and then allow some time for the goroutines to run.
	time.Sleep(50 * time.Millisecond)
	clock.Add(batchBufferTimeout + time.Second*10)
	time.Sleep(50 * time.Millisecond)
	clock.Add(time.Minute * 45)
	time.Sleep(50 * time.Millisecond)
	clock.Add(time.Minute * 5)
	time.Sleep(50 * time.Millisecond)

	resTwo, _ := sturdyc.GetOrFetchBatch[Foo](ctx, client, records, client.BatchKeyFn("item"), func(_ context.Context, ids []string) (map[string]Foo, error) {
		values := make(map[string]Foo, len(ids))
		for _, id := range ids {
			values[id] = Foo{Value: "foo3-" + id}
		}
		return values, nil
	})

	if resTwo["1"].Value != "foo2-1" {
		t.Errorf("expected 'foo2-1', got '%s'", resTwo["1"].Value)
	}
}
