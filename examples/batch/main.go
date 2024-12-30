package main

import (
	"context"
	"log"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/viccon/sturdyc"
)

type API struct {
	*sturdyc.Client[string]
}

func NewAPI(c *sturdyc.Client[string]) *API {
	return &API{c}
}

func (a *API) GetBatch(ctx context.Context, ids []string) (map[string]string, error) {
	// We are going to pass the cache a key function that prefixes each id.
	// This makes it possible to save the same id for different data sources.
	cacheKeyFn := a.BatchKeyFn("some-prefix")

	// The fetchFn is only going to retrieve the IDs that are not in the cache.
	fetchFn := func(_ context.Context, cacheMisses []string) (map[string]string, error) {
		log.Printf("Cache miss. Fetching ids: %s\n", strings.Join(cacheMisses, ", "))
		// Batch functions should return a map where the key is the id of the record.
		// If you have storage of missing records enabled, any ID that isn't present
		// in this map is going to be stored as a cache miss.
		response := make(map[string]string, len(cacheMisses))
		for _, id := range cacheMisses {
			response[id] = "value"
		}
		return response, nil
	}

	return a.GetOrFetchBatch(ctx, ids, cacheKeyFn, fetchFn)
}

func main() {
	// Maximum number of entries in the sturdyc.
	capacity := 10000
	// Number of shards to use for the sturdyc.
	numShards := 10
	// Time-to-live for cache entries.
	ttl := 2 * time.Hour
	// Percentage of entries to evict when the cache is full. Setting this
	// to 0 will make set a no-op if the cache has reached its capacity.
	evictionPercentage := 10
	// Set a minimum and maximum refresh delay for the sturdyc. This is
	// used to spread out the refreshes for entries evenly over time.
	minRefreshDelay := time.Second
	maxRefreshDelay := time.Second * 2
	// Set a synchronous refresh delay for when we want a refresh to happen synchronously.
	synchronousRefreshDelay := time.Second * 30
	// The base for exponential backoff when retrying a refresh.
	retryBaseDelay := time.Millisecond * 10

	// Create a cache client with the specified configuration.
	cacheClient := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, retryBaseDelay),
	)

	// Create a new API instance with the cache client.
	api := NewAPI(cacheClient)

	// Seed the cache with ids 1-10.
	log.Println("Seeding ids 1-10")
	ids := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	api.GetBatch(context.Background(), ids)
	log.Println("Seed completed")

	// To demonstrate that the records have been cached individually, we can continue
	// fetching a random subset of records from the original batch, plus a new
	// ID. By examining the logs, we should be able to see that the cache only
	// fetches the ID that wasn't present in the original batch, indicating that
	// the batch itself isn't part of the key.
	for i := 1; i <= 100; i++ {
		// Get N ids from the original batch.
		recordsToFetch := rand.IntN(10) + 1
		batch := make([]string, recordsToFetch)
		copy(batch, ids[:recordsToFetch])
		// Add a random ID between 1 and 100 to the batch.
		batch = append(batch, strconv.Itoa(rand.IntN(1000)+10))
		values, _ := api.GetBatch(context.Background(), batch)
		log.Println(values)
	}
}
