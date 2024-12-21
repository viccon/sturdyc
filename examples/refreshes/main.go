package main

import (
	"context"
	"log"
	"time"

	"github.com/viccon/sturdyc"
)

type API struct {
	*sturdyc.Client[string]
}

func NewAPI(c *sturdyc.Client[string]) *API {
	return &API{c}
}

func (a *API) Get(ctx context.Context, key string) (string, error) {
	// This could be an API call, a database query, etc. The only requirement is
	// that the function adheres to the `sturdyc.FetchFn` signature. Remember
	// that you can use closures to capture additional values.
	fetchFn := func(_ context.Context) (string, error) {
		log.Printf("Fetching value for key: %s\n", key)
		return "value", nil
	}
	return a.GetOrFetch(ctx, key, fetchFn)
}

func main() {
	// ===========================================================
	// ===================== Basic configuration =================
	// ===========================================================
	// Maximum number of entries in the sturdyc.
	capacity := 10000
	// Number of shards to use for the sturdyc.
	numShards := 10
	// Time-to-live for cache entries.
	ttl := 2 * time.Hour
	// Percentage of entries to evict when the cache is full. Setting this
	// to 0 will make set a no-op if the cache has reached its capacity.
	evictionPercentage := 10

	// ===========================================================
	// =================== Background refreshes ==================
	// ===========================================================
	// Set a minimum and maximum refresh delay for the record. This is
	// used to spread out the refreshes of our entries evenly over time.
	// We don't want our outgoing requests graph to look like a comb.
	minRefreshDelay := time.Millisecond * 10
	maxRefreshDelay := time.Millisecond * 30
	// The base used for exponential backoff when retrying a refresh. Most of the
	// time, we perform refreshes well in advance of the records expiry time.
	// Hence, we can use this to make it easier for a system that is having
	// trouble to get back on it's feet by making fewer refreshes when we're
	// seeing a lot of errors. Once we receive a successful response, the
	// refreshes return to their original frequency. You can set this to 0
	// if you don't want this behavior.
	retryBaseDelay := time.Millisecond * 10

	// Create a cache client with the specified configuration.
	cacheClient := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, retryBaseDelay),
	)

	// Create a new API instance with the cache client.
	api := NewAPI(cacheClient)

	// We are going to retrieve the values every 10 milliseconds, however the
	// logs will reveal that actual refreshes fluctuate randomly within a 10-30
	// millisecond range. Even if this loop is executed across multiple goroutines,
	// the API call frequency will maintain this variability.
	for i := 0; i < 100; i++ {
		val, err := api.Get(context.Background(), "key")
		if err != nil {
			log.Println("Failed to  retrieve the record from the cache.")
			continue
		}
		log.Printf("Value: %s\n", val)
		time.Sleep(minRefreshDelay)
	}
}
