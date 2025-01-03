package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/viccon/sturdyc"
)

type OrderOptions struct {
	CarrierName        string
	LatestDeliveryTime string
}

type OrderAPI struct {
	*sturdyc.Client[string]
}

func NewOrderAPI(c *sturdyc.Client[string]) *OrderAPI {
	return &OrderAPI{c}
}

func (a *OrderAPI) OrderStatus(ctx context.Context, ids []string, opts OrderOptions) (map[string]string, error) {
	// We use the PermutedBatchKeyFn when an ID isn't enough to uniquely identify a
	// record. The cache is going to store each id once per set of options. In a more
	// realistic scenario, the opts would be query params or arguments to a DB query.
	cacheKeyFn := a.PermutatedBatchKeyFn("key", opts)

	// We'll create a fetchFn with a closure that captures the options. For this
	// simple example, it logs and returns the status for each order, but you could
	// just as easily have called an external API.
	fetchFn := func(_ context.Context, cacheMisses []string) (map[string]string, error) {
		log.Printf("Fetching: %v, carrier: %s, delivery time: %s\n", cacheMisses, opts.CarrierName, opts.LatestDeliveryTime)
		response := map[string]string{}
		for _, id := range cacheMisses {
			response[id] = fmt.Sprintf("Available for %s", opts.CarrierName)
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

	// Create a new cache client with the specified configuration.
	cacheClient := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithEarlyRefreshes(minRefreshDelay, maxRefreshDelay, synchronousRefreshDelay, retryBaseDelay),
	)

	// We will fetch these IDs using various option sets, meaning that the ID alone
	// won't be enough to uniquely identify a record. Instead, the cache is going to
	// store each record by combining the ID with the permutations of the options.
	ids := []string{"id1", "id2", "id3"}
	optionSetOne := OrderOptions{CarrierName: "FEDEX", LatestDeliveryTime: "2024-04-06"}
	optionSetTwo := OrderOptions{CarrierName: "DHL", LatestDeliveryTime: "2024-04-07"}
	optionSetThree := OrderOptions{CarrierName: "UPS", LatestDeliveryTime: "2024-04-08"}

	orderClient := NewOrderAPI(cacheClient)
	ctx := context.Background()

	// Next, we'll fetch the entire list of IDs for all options sets.
	log.Println("Filling the cache with all IDs for all option sets")
	orderClient.OrderStatus(ctx, ids, optionSetOne)
	orderClient.OrderStatus(ctx, ids, optionSetTwo)
	orderClient.OrderStatus(ctx, ids, optionSetThree)
	log.Println("Cache filled")

	// At this point, the cache has stored each record individually for each option set:
	// FEDEX-2024-04-06-id1
	// DHL-2024-04-07-id1
	// UPS-2024-04-08-id1
	// etc..

	// Sleep to make sure that all records are due for a refresh.
	time.Sleep(maxRefreshDelay + 1)

	// Fetch each id for each option set.
	for i := 0; i < len(ids); i++ {
		orderClient.OrderStatus(ctx, []string{ids[i]}, optionSetOne)
		orderClient.OrderStatus(ctx, []string{ids[i]}, optionSetTwo)
		orderClient.OrderStatus(ctx, []string{ids[i]}, optionSetThree)
	}

	// Sleep for a second to allow the refresh logs to print.
	time.Sleep(time.Second)
}
