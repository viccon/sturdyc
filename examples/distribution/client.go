package main

import (
	"context"
	"log"
	"time"

	"github.com/viccon/sturdyc"
)

// NOTE: We won't use all of these features in this example. However, I thought
// that it would be interesting to show how a cache client that makes full use
// of this functionality might be configured.

// Basic configuration for the cache.
const (
	capacity                           = 100_000
	numberOfShards                     = 100
	ttl                                = 5 * time.Minute
	percentageOfRecordsToEvictWhenFull = 25
)

// Configuration for the early in-memory refreshes.
const (
	minRefreshTime         = 100 * time.Millisecond
	maxRefreshTime         = 500 * time.Millisecond
	synchronousRefreshTime = 30 * time.Second
	retryBaseDelay         = time.Second
)

// Configuration for the refresh coalescing.
const (
	idealBufferSize = 50
	bufferTimeout   = 15 * time.Second
)

func newAPIClient(distributedStorage sturdyc.DistributedStorage) *apiClient {
	return &apiClient{
		cache: sturdyc.New[any](capacity, numberOfShards, ttl, percentageOfRecordsToEvictWhenFull,
			sturdyc.WithMissingRecordStorage(),
			sturdyc.WithEarlyRefreshes(minRefreshTime, maxRefreshTime, synchronousRefreshTime, retryBaseDelay),
			sturdyc.WithRefreshCoalescing(idealBufferSize, bufferTimeout),
			sturdyc.WithDistributedStorage(distributedStorage),
		),
	}
}

type apiClient struct {
	cache *sturdyc.Client[any]
}

type options struct {
	ID        string
	SortOrder string
}

func (c *apiClient) GetShippingOptions(ctx context.Context, id string, sortOrder string) ([]string, error) {
	cacheKey := c.cache.PermutatedKey("shipping-options", options{ID: id, SortOrder: sortOrder})
	fetchFn := func(_ context.Context) ([]string, error) {
		log.Println("Fetching shipping options from the underlying data source")
		return []string{"standard", "express", "next-day"}, nil
	}
	return sturdyc.GetOrFetch(ctx, c.cache, cacheKey, fetchFn)
}
