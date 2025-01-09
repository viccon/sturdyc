package main

import (
	"context"
	"log"
	"time"

	"github.com/viccon/sturdyc"
)

type OrderAPI struct {
	cacheClient *sturdyc.Client[any]
}

func NewOrderAPI(c *sturdyc.Client[any]) *OrderAPI {
	return &OrderAPI{cacheClient: c}
}

func (a *OrderAPI) OrderStatus(ctx context.Context, ids []string) (map[string]string, error) {
	cacheKeyFn := a.cacheClient.BatchKeyFn("order-status")
	fetchFn := func(_ context.Context, cacheMisses []string) (map[string]string, error) {
		response := make(map[string]string, len(ids))
		for _, id := range cacheMisses {
			response[id] = "Order status: pending"
		}
		return response, nil
	}
	return sturdyc.GetOrFetchBatch(ctx, a.cacheClient, ids, cacheKeyFn, fetchFn)
}

func (a *OrderAPI) DeliveryTime(ctx context.Context, ids []string) (map[string]time.Time, error) {
	cacheKeyFn := a.cacheClient.BatchKeyFn("delivery-time")
	fetchFn := func(_ context.Context, cacheMisses []string) (map[string]time.Time, error) {
		response := make(map[string]time.Time, len(ids))
		for _, id := range cacheMisses {
			response[id] = time.Now()
		}
		return response, nil
	}
	return sturdyc.GetOrFetchBatch(ctx, a.cacheClient, ids, cacheKeyFn, fetchFn)
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

	// Create a new cache client with the specified configuration.
	cacheClient := sturdyc.New[any](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, retryBaseDelay),
		sturdyc.WithRefreshCoalescing(10, time.Second*15),
	)

	api := NewOrderAPI(cacheClient)
	ids := []string{"1", "2", "3"}
	ctx := context.Background()
	if orders, err := api.OrderStatus(ctx, ids); err == nil {
		log.Println(orders)
	}
	if deliveryTimes, err := api.DeliveryTime(ctx, ids); err == nil {
		log.Println(deliveryTimes)
	}
}
