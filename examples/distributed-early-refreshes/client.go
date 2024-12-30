package main

import (
	"context"
	"log"
	"math/rand/v2"
	"time"

	"github.com/viccon/sturdyc"
)

// Basic configuration for the cache.
const (
	capacity                           = 100_000
	numberOfShards                     = 100
	ttl                                = 5 * time.Minute
	percentageOfRecordsToEvictWhenFull = 25
)

// Configuration for the early in-memory refreshes.
const (
	minRefreshTime         = 2 * time.Second
	maxRefreshTime         = 4 * time.Second
	synchronousRefreshTime = 30 * time.Second
	retryBaseDelay         = 5 * time.Second
)

// Configuration for the refresh coalescing.
const (
	idealBufferSize = 3
	bufferTimeout   = 2 * time.Second
)

// Configuration for how early we want to refresh records in the distributed storage.
const refreshAfter = time.Second

func newAPIClient(distributedStorage sturdyc.DistributedStorageWithDeletions) *apiClient {
	return &apiClient{
		cache: sturdyc.New[any](capacity, numberOfShards, ttl, percentageOfRecordsToEvictWhenFull,
			sturdyc.WithEarlyRefreshes(minRefreshTime, maxRefreshTime, synchronousRefreshTime, retryBaseDelay),
			sturdyc.WithRefreshCoalescing(idealBufferSize, bufferTimeout),
			sturdyc.WithDistributedStorageEarlyRefreshes(distributedStorage, refreshAfter),
			// NOTE: Uncommenting this line will make the cache mark the records as
			// missing rather than delete them. It will prevent the cache from making
			// any outgoing requests until it's due for a refresh.
			// sturdyc.WithMissingRecordStorage(),
		),
	}
}

type apiClient struct {
	cache *sturdyc.Client[any]
}

type options struct {
	SortOrder string
}

func (c *apiClient) GetShippingOptions(ctx context.Context, containerIndex int, ids []string, sortOrder string) (map[string][]string, error) {
	cacheKeyFn := c.cache.PermutatedBatchKeyFn("shipping-options", options{sortOrder})
	fetchFn := func(_ context.Context, ids []string) (map[string][]string, error) {
		response := make(map[string][]string, len(ids))

		for _, id := range ids {
			// Excluding an ID will make it get delete if it has been cached before.
			// Following this log statament, you should see that it gets deleted, and the
			// next time it's requested it leads to a "outgoing" request which means that
			// it wasn't found in the distributed key-value store.
			if rand.IntN(2) == 0 {
				log.Printf("Excluding ID: %s from the response\n", id)
				continue
			}

			log.Printf("ID: %s was fetched by container %d\n", id, containerIndex)
			response[id] = []string{"standard", "express", "next-day"}
		}

		return response, nil
	}
	return sturdyc.GetOrFetchBatch(ctx, c.cache, ids, cacheKeyFn, fetchFn)
}
