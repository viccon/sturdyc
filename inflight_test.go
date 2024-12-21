package sturdyc_test

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

func createBatchFn(prefix string, calls *atomic.Int32, cond *sync.Cond) sturdyc.BatchFetchFn[string] {
	return func(_ context.Context, ids []string) (map[string]string, error) {
		calls.Add(1)
		vals := make(map[string]string, len(ids))
		for _, id := range ids {
			vals[id] = prefix + "-" + id
		}

		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()

		return vals, nil
	}
}

func TestBatchRequestsForMissingKeysGetDeduplicated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10000
	numShards := 100
	ttl := time.Minute
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage)

	var calls atomic.Int32
	cond := sync.NewCond(&sync.Mutex{})

	for i := 0; i < 10; i++ {
		ids := make([]string, 0, 10)
		for j := 0; j < 10; j++ {
			ids = append(ids, strconv.Itoa(i*10+j))
		}

		// Fetch the batch for 5 different sets of key prefixes. We're doing this to ensure
		// that the responses don't get scrambled. The in-flight tracking has to operate with
		// both the IDs and keys, and messing that up would be devastating.
		for j := 0; j < 5; j++ {
			keyPrefix := "foo" + strconv.Itoa(j)
			keyFn := c.BatchKeyFn(keyPrefix)
			batchFn := createBatchFn(keyPrefix, &calls, cond)
			go c.GetOrFetchBatch(ctx, ids, keyFn, batchFn)
		}
	}

	// We need to make sure that these batches are in-flight before we make any more requests.
	time.Sleep(500 * time.Millisecond)

	if c.NumKeysInflight() != 500 {
		t.Fatalf("expected 500 keys in-flight; got %d", c.NumKeysInflight())
	}

	// Now, while these batches are in-flight, I'm going to create additional requests for the same IDs in a loop.
	// On each iteration, I'm going to randomize five IDs between 0 and 99. This ensures that new requests are able
	// to pick IDs from any of the in-flight batches, and then merge the response.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(5)
		for j := 0; j < 5; j++ {
			keyPrefix := "foo" + strconv.Itoa(j)
			keyFn := c.BatchKeyFn(keyPrefix)
			batchFn := createBatchFn(keyPrefix, &calls, cond)

			// We are going to get a few duplicates here too which the cache should be able to handle.
			ids := make([]string, 0, 5)
			for k := 0; k < 5; k++ {
				ids = append(ids, strconv.Itoa(rand.IntN(99)))
			}

			go func() {
				res, err := c.GetOrFetchBatch(ctx, ids, keyFn, batchFn)
				if err != nil {
					t.Errorf("expected no error got %v", err)
				}

				for _, id := range ids {
					want := keyPrefix + "-" + id
					if got, ok := res[id]; !ok || got != want {
						t.Errorf("expected %s got %s", want, got)
					}
				}
				wg.Done()
			}()
		}
	}

	// Give the goroutines some time to start.
	time.Sleep(500 * time.Millisecond)

	// Assert that none of the requests above created any additional in-flight keys.
	if c.NumKeysInflight() != 500 {
		t.Fatalf("expected 500 keys in-flight; got %d", c.NumKeysInflight())
	}

	// Broadcast that it's time for the in-flight batches to resolve.
	cond.Broadcast()
	// Wait for all of the responses to get asserted.
	wg.Wait()

	// Assert that we only made 50 calls.
	if got := calls.Load(); got != 50 {
		t.Errorf("got %d calls; wanted 50", got)
	}

	// The keys are deleted in a background routine that needs to get a lock.
	// Hence, to assert that they have been deleted properly, we'll have to give
	// that a routine a chance to run.
	time.Sleep(50 * time.Millisecond)
	if c.NumKeysInflight() > 0 {
		t.Errorf("expected no inflight keys; got %d", c.NumKeysInflight())
	}
}

func TestInflightKeysAreRemovedForBatchRequestsThatPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10000
	numShards := 100
	ttl := time.Minute
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage)

	batchFn := func(_ context.Context, _ []string) (map[string]string, error) {
		panic("boom")
	}
	ids := []string{"1", "2", "3", "4", "5"}

	_, err := c.GetOrFetchBatch(ctx, ids, c.BatchKeyFn("foo"), batchFn)
	if err == nil {
		t.Errorf("expected an error; got nil")
	}

	time.Sleep(50 * time.Millisecond)

	if c.NumKeysInflight() > 0 {
		t.Errorf("expected no inflight keys; got %d", c.NumKeysInflight())
	}

	if c.Size() > 0 {
		t.Errorf("expected no keys in cache; got %d", c.Size())
	}
}

func TestRequestsForMissingKeysGetDeduplicated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 100
	numShards := 2
	ttl := time.Minute
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage)

	ch := make(chan string)
	var calls atomic.Int32
	fn := func(_ context.Context) (string, error) {
		calls.Add(1)
		return <-ch, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			v, err := c.GetOrFetch(ctx, "id-1", fn)
			if err != nil {
				t.Error(err)
			}
			if v != "value1" {
				t.Errorf("got %q; want %q", v, "value1")
			}
			wg.Done()
		}()
	}
	time.Sleep(50 * time.Millisecond)
	if c.NumKeysInflight() != 1 {
		t.Errorf("expected 1 inflight key; got %d", c.NumKeysInflight())
	}
	ch <- "value1"
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Errorf("got %d calls; wanted 1", got)
	}
	if c.NumKeysInflight() > 0 {
		t.Errorf("expected no inflight keys; got %d", c.NumKeysInflight())
	}
}

func TestInflightKeyIsRemovedIfTheRequestPanics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10000
	numShards := 100
	ttl := time.Minute
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage)

	fetchFn := func(_ context.Context) (string, error) {
		panic("boom")
	}
	_, err := c.GetOrFetch(ctx, "key1", fetchFn)
	if err == nil {
		t.Errorf("expected an error; got nil")
	}

	if c.NumKeysInflight() > 0 {
		t.Errorf("expected no inflight keys; got %d", c.NumKeysInflight())
	}

	if c.Size() > 0 {
		t.Errorf("expected no keys in cache; got %d", c.Size())
	}
}
