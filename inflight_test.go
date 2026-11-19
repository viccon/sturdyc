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

// blockingFetch returns a fetch function that signals when it has started, and
// then blocks until it's released.
func blockingFetch(value string, calls *atomic.Int32) (fn sturdyc.FetchFn[string], started, release chan struct{}) {
	started = make(chan struct{}, 1)
	release = make(chan struct{})
	fn = func(_ context.Context) (string, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return value, nil
	}
	return fn, started, release
}

func TestDeletingAKeyWhileItIsBeingFetchedDiscardsTheResult(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	var calls atomic.Int32
	fetch, started, release := blockingFetch("stale", &calls)

	done := make(chan string)
	go func() {
		v, _ := c.GetOrFetch(context.Background(), "key", fetch)
		done <- v
	}()
	<-started

	// The data source is updated, and the key is invalidated while the fetch is in flight.
	c.Delete("key")
	close(release)

	// The caller that started the fetch still gets the value it fetched...
	if v := <-done; v != "stale" {
		t.Errorf("expected the caller to get %q, got %q", "stale", v)
	}
	// ...but it's not written to the cache.
	if v, ok := c.Get("key"); ok {
		t.Errorf("expected the fetched value to be discarded, got %q", v)
	}
}

func TestAKeyThatIsDeletedWhileBeingFetchedIsFetchedAgain(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	var calls atomic.Int32
	fetch, started, release := blockingFetch("stale", &calls)
	go func() {
		_, _ = c.GetOrFetch(context.Background(), "key", fetch)
	}()
	<-started
	c.Delete("key")

	// Requests that arrive after the deletion must not join the invalidated fetch.
	v, err := c.GetOrFetch(context.Background(), "key", func(_ context.Context) (string, error) {
		calls.Add(1)
		return "fresh", nil
	})
	close(release)
	if err != nil || v != "fresh" {
		t.Fatalf("expected %q, got %q and %v", "fresh", v, err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("expected 2 fetches, got %d", got)
	}
}

func TestSettingAKeyWhileItIsBeingFetchedIsNotOverwritten(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	var calls atomic.Int32
	fetch, started, release := blockingFetch("stale", &calls)

	done := make(chan struct{})
	go func() {
		_, _ = c.GetOrFetch(context.Background(), "key", fetch)
		close(done)
	}()
	<-started
	c.Set("key", "written-by-user")
	close(release)
	<-done

	if v, _ := c.Get("key"); v != "written-by-user" {
		t.Errorf("expected the user's write to be kept, got %q", v)
	}
}

func TestDeletingKeysWhileABatchIsBeingFetchedDiscardsTheirResults(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	keyFn := c.BatchKeyFn("item")
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_, _ = c.GetOrFetchBatch(context.Background(), []string{"1", "2"}, keyFn, func(_ context.Context, _ []string) (map[string]string, error) {
			close(started)
			<-release
			return map[string]string{"1": "stale", "2": "stale"}, nil
		})
		close(done)
	}()
	<-started
	c.Delete(keyFn("1"))
	close(release)
	<-done

	if _, ok := c.Get(keyFn("1")); ok {
		t.Error("expected the deleted key not to be cached")
	}
	if _, ok := c.Get(keyFn("2")); !ok {
		t.Error("expected the key that wasn't deleted to be cached")
	}
}

func TestWaitersRespectTheirOwnContext(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	var calls atomic.Int32
	fetch, started, release := blockingFetch("value", &calls)
	defer close(release)
	go func() {
		_, _ = c.GetOrFetch(context.Background(), "key", fetch)
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.GetOrFetch(ctx, "key", fetch)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected the waiter's own deadline to be exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("expected the waiter to return when its deadline was exceeded, it took %v", elapsed)
	}
}

func TestWaitersRetryWhenTheLeadersContextIsCancelled(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	started := make(chan struct{})
	var calls atomic.Int32
	fetch := func(ctx context.Context) (string, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		}
		return "value", nil
	}

	leaderErr := make(chan error)
	go func() {
		_, err := c.GetOrFetch(leaderCtx, "key", fetch)
		leaderErr <- err
	}()
	<-started

	waiterResult := make(chan string)
	go func() {
		v, err := c.GetOrFetch(context.Background(), "key", fetch)
		if err != nil {
			t.Errorf("expected the waiter not to get the leader's error, got %v", err)
		}
		waiterResult <- v
	}()

	// Give the waiter a chance to join the flight before the leader is cancelled.
	time.Sleep(50 * time.Millisecond)
	cancelLeader()

	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Errorf("expected the leader to get context.Canceled, got %v", err)
	}
	if v := <-waiterResult; v != "value" {
		t.Errorf("expected the waiter to retry and get %q, got %q", "value", v)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("expected 2 fetches, got %d", got)
	}
}

func TestBatchFetchesJoinSingleFetchesForTheSameKey(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	keyFn := c.BatchKeyFn("item")
	var calls atomic.Int32
	fetch, started, release := blockingFetch("value-1", &calls)
	go func() {
		_, _ = c.GetOrFetch(context.Background(), keyFn("1"), fetch)
	}()
	<-started

	var requestedIDs []string
	result := make(chan map[string]string)
	go func() {
		res, err := c.GetOrFetchBatch(context.Background(), []string{"1", "2"}, keyFn, func(_ context.Context, ids []string) (map[string]string, error) {
			calls.Add(1)
			requestedIDs = ids
			return map[string]string{"2": "value-2"}, nil
		})
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		result <- res
	}()

	time.Sleep(50 * time.Millisecond)
	close(release)
	res := <-result

	if len(requestedIDs) != 1 || requestedIDs[0] != "2" {
		t.Errorf("expected the batch to only fetch ID 2, got %v", requestedIDs)
	}
	if res["1"] != "value-1" || res["2"] != "value-2" {
		t.Errorf("expected both values, got %v", res)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("expected 2 fetches, got %d", got)
	}
}

func TestSingleFetchesJoinBatchFetchesForTheSameKey(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	keyFn := c.BatchKeyFn("item")
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = c.GetOrFetchBatch(context.Background(), []string{"1", "2"}, keyFn, func(_ context.Context, _ []string) (map[string]string, error) {
			calls.Add(1)
			close(started)
			<-release
			return map[string]string{"1": "value-1"}, nil
		})
	}()
	<-started

	type result struct {
		value string
		err   error
	}
	results := make(chan result, 2)
	for _, id := range []string{"1", "2"} {
		go func() {
			v, err := c.GetOrFetch(context.Background(), keyFn(id), func(_ context.Context) (string, error) {
				calls.Add(1)
				return "unexpected", nil
			})
			results <- result{v, err}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(release)
	got := map[string]error{}
	for i := 0; i < 2; i++ {
		r := <-results
		got[r.value] = r.err
	}

	if err, ok := got["value-1"]; !ok || err != nil {
		t.Errorf("expected value-1 without an error, got %v", got)
	}
	// ID 2 wasn't returned by the batch, which means that it doesn't exist.
	if err, ok := got[""]; !ok || !errors.Is(err, sturdyc.ErrNotFound) {
		t.Errorf("expected ErrNotFound for the ID that the batch didn't return, got %v", got)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("expected 1 fetch, got %d", n)
	}
}

func TestBatchFetchesRetryKeysWhenAJoinedSingleFetchFails(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	keyFn := c.BatchKeyFn("item")
	errBoom := errors.New("boom")
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = c.GetOrFetch(context.Background(), keyFn("1"), func(_ context.Context) (string, error) {
			close(started)
			<-release
			return "", errBoom
		})
	}()
	<-started

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	var mu sync.Mutex
	var requested [][]string
	res, err := c.GetOrFetchBatch(context.Background(), []string{"1", "2"}, keyFn, func(_ context.Context, ids []string) (map[string]string, error) {
		mu.Lock()
		requested = append(requested, ids)
		mu.Unlock()
		vals := make(map[string]string, len(ids))
		for _, id := range ids {
			vals[id] = "value-" + id
		}
		return vals, nil
	})
	// The single fetch uses another fetch function, so its error isn't a reason
	// to fail the batch. The batch fetches the key with its own function instead.
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if res["1"] != "value-1" || res["2"] != "value-2" {
		t.Errorf("expected both values, got %v", res)
	}
	if len(requested) != 2 || len(requested[0]) != 1 || requested[0][0] != "2" || len(requested[1]) != 1 || requested[1][0] != "1" {
		t.Errorf("expected the batch to fetch ID 2, and then ID 1 after the single fetch failed, got %v", requested)
	}
}

func TestBatchFetchesReturnErrorsFromJoinedBatchFetches(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	keyFn := c.BatchKeyFn("item")
	errBoom := errors.New("boom")
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = c.GetOrFetchBatch(context.Background(), []string{"1"}, keyFn, func(_ context.Context, _ []string) (map[string]string, error) {
			close(started)
			<-release
			return nil, errBoom
		})
	}()
	<-started

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	var calls atomic.Int32
	res, err := c.GetOrFetchBatch(context.Background(), []string{"1", "2"}, keyFn, func(_ context.Context, _ []string) (map[string]string, error) {
		calls.Add(1)
		return map[string]string{"2": "value-2"}, nil
	})
	// Batches that join each other fail together, as they always have.
	if !errors.Is(err, errBoom) {
		t.Errorf("expected the error from the joined batch, got %v", err)
	}
	if res["2"] != "value-2" {
		t.Errorf("expected the records that we were able to fetch, got %v", res)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("expected 1 call to our own fetch function, got %d", n)
	}
}

func TestSingleFetchesRetryOnceWhenAJoinedBatchFetchFails(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	keyFn := c.BatchKeyFn("item")
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = c.GetOrFetchBatch(context.Background(), []string{"1"}, keyFn, func(_ context.Context, _ []string) (map[string]string, error) {
			close(started)
			<-release
			return nil, errors.New("batch endpoint is down")
		})
	}()
	<-started

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	var calls atomic.Int32
	errSingle := errors.New("single endpoint is down")
	_, err := c.GetOrFetch(context.Background(), keyFn("1"), func(_ context.Context) (string, error) {
		calls.Add(1)
		return "", errSingle
	})
	if !errors.Is(err, errSingle) {
		t.Errorf("expected the error from our own fetch function, got %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("expected our own fetch function to be called once, got %d", n)
	}

	v, err := c.GetOrFetch(context.Background(), keyFn("1"), func(_ context.Context) (string, error) {
		return "value-1", nil
	})
	if err != nil || v != "value-1" {
		t.Errorf("expected value-1 without an error, got %q, %v", v, err)
	}
}

func TestFetchFunctionsCanRequestTheirOwnKeys(t *testing.T) {
	t.Parallel()

	type fetchCase struct {
		name string
		run  func(ctx context.Context, c *sturdyc.Client[string], keyFn sturdyc.KeyFn) (string, error)
	}
	cases := []fetchCase{
		{
			name: "a batch fetch that requests a single key",
			run: func(ctx context.Context, c *sturdyc.Client[string], keyFn sturdyc.KeyFn) (string, error) {
				res, err := c.GetOrFetchBatch(ctx, []string{"1", "2"}, keyFn, func(ctx context.Context, ids []string) (map[string]string, error) {
					vals := make(map[string]string, len(ids))
					for _, id := range ids {
						v, err := c.GetOrFetch(ctx, keyFn(id), func(context.Context) (string, error) {
							return "value-" + id, nil
						})
						if err != nil {
							return nil, err
						}
						vals[id] = v
					}
					return vals, nil
				})
				return res["1"], err
			},
		},
		{
			name: "a single fetch that requests a batch",
			run: func(ctx context.Context, c *sturdyc.Client[string], keyFn sturdyc.KeyFn) (string, error) {
				return c.GetOrFetch(ctx, keyFn("1"), func(ctx context.Context) (string, error) {
					res, err := c.GetOrFetchBatch(ctx, []string{"1"}, keyFn, func(context.Context, []string) (map[string]string, error) {
						return map[string]string{"1": "value-1"}, nil
					})
					return res["1"], err
				})
			},
		},
		{
			name: "a single fetch that requests its own key",
			run: func(ctx context.Context, c *sturdyc.Client[string], keyFn sturdyc.KeyFn) (string, error) {
				return c.GetOrFetch(ctx, keyFn("1"), func(ctx context.Context) (string, error) {
					return c.GetOrFetch(ctx, keyFn("1"), func(context.Context) (string, error) {
						return "value-1", nil
					})
				})
			},
		},
		{
			name: "a batch fetch that requests its own batch from another goroutine",
			run: func(ctx context.Context, c *sturdyc.Client[string], keyFn sturdyc.KeyFn) (string, error) {
				res, err := c.GetOrFetchBatch(ctx, []string{"1"}, keyFn, func(ctx context.Context, ids []string) (map[string]string, error) {
					type result struct {
						vals map[string]string
						err  error
					}
					inner := make(chan result)
					go func() {
						vals, err := c.GetOrFetchBatch(ctx, ids, keyFn, func(context.Context, []string) (map[string]string, error) {
							return map[string]string{"1": "value-1"}, nil
						})
						inner <- result{vals, err}
					}()
					r := <-inner
					return r.vals, r.err
				})
				return res["1"], err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
			keyFn := c.BatchKeyFn("item")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			v, err := tc.run(ctx, c, keyFn)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if v != "value-1" {
				t.Errorf("expected value-1, got %q", v)
			}
			if cached, ok := c.Get(keyFn("1")); !ok || cached != "value-1" {
				t.Errorf("expected value-1 to be cached, got %q", cached)
			}
			if n := c.NumKeysInflight(); n != 0 {
				t.Errorf("expected no keys in flight, got %d", n)
			}
		})
	}
}

func TestNilContextsAreTreatedAsBackground(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10, sturdyc.WithNoContinuousEvictions())
	errBoom := errors.New("boom")

	//nolint:staticcheck // We're testing that a nil context works.
	_, err := c.GetOrFetch(nil, "failing", func(context.Context) (string, error) {
		return "", errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Errorf("expected the fetch function's error, got %v", err)
	}

	//nolint:staticcheck // We're testing that a nil context works.
	res, err := c.GetOrFetchBatch(nil, []string{"1"}, c.BatchKeyFn("item"), func(context.Context, []string) (map[string]string, error) {
		return nil, errBoom
	})
	if !errors.Is(err, errBoom) || len(res) != 0 {
		t.Errorf("expected the batch fetch function's error, got %v, %v", res, err)
	}

	// A waiter with a nil context.
	var calls atomic.Int32
	fetch, started, release := blockingFetch("value", &calls)
	go func() {
		_, _ = c.GetOrFetch(context.Background(), "key", fetch)
	}()
	<-started
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	//nolint:staticcheck // We're testing that a nil context works.
	v, err := c.GetOrFetch(nil, "key", fetch)
	if err != nil || v != "value" {
		t.Errorf("expected the waiter to get the leader's value, got %q, %v", v, err)
	}
}

func TestWaitersAreReleasedIfABatchPanicsAfterTheFetch(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[string](100, 1, time.Hour, 10,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithLog(&sturdyc.NoopLogger{}),
	)
	// The key function panics for an ID that the fetch function returns
	// without being asked for it, which happens after the flights are committed.
	keyFn := func(id string) string {
		if id == "unrequested" {
			panic("unexpected ID")
		}
		return "item-" + id
	}
	started := make(chan struct{})
	release := make(chan struct{})
	batchResult := make(chan map[string]string)
	go func() {
		res, err := c.GetOrFetchBatch(context.Background(), []string{"1"}, keyFn, func(context.Context, []string) (map[string]string, error) {
			close(started)
			<-release
			return map[string]string{"1": "value-1", "unrequested": "value"}, nil
		})
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		batchResult <- res
	}()
	<-started

	waiterResult := make(chan string)
	go func() {
		v, _ := c.GetOrFetch(context.Background(), "item-1", func(context.Context) (string, error) {
			return "unexpected", nil
		})
		waiterResult <- v
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)

	select {
	case v := <-waiterResult:
		if v != "value-1" {
			t.Errorf("expected the waiter to get value-1, got %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter was never released")
	}
	if res := <-batchResult; res["1"] != "value-1" {
		t.Errorf("expected the batch to get value-1, got %v", res)
	}
	if n := c.NumKeysInflight(); n != 0 {
		t.Errorf("expected no keys in flight, got %d", n)
	}
}

func TestSynchronousRefreshesJoinBackgroundRefreshes(t *testing.T) {
	t.Parallel()

	clock := sturdyc.NewTestClock(time.Now())
	c := sturdyc.New[string](100, 1, time.Hour, 10,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithClock(clock),
		sturdyc.WithEarlyRefreshes(time.Second, time.Second, 10*time.Second, time.Minute),
	)
	c.Set("key", "value-1")

	var calls atomic.Int32
	fetch, started, release := blockingFetch("value-2", &calls)

	// This read schedules a background refresh.
	clock.Add(2 * time.Second)
	if v, _ := c.GetOrFetch(context.Background(), "key", fetch); v != "value-1" {
		t.Fatalf("expected the cached value, got %q", v)
	}
	<-started

	// This read has to refresh synchronously, and it should join the background refresh.
	clock.Add(10 * time.Second)
	result := make(chan string)
	go func() {
		v, _ := c.GetOrFetch(context.Background(), "key", fetch)
		result <- v
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)

	if v := <-result; v != "value-2" {
		t.Errorf("expected the refreshed value, got %q", v)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 fetch, got %d", got)
	}
}
