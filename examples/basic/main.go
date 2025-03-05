package main

import (
	"context"
	"log"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/viccon/sturdyc"
)

func demonstrateGetOrFetch(cacheClient *sturdyc.Client[int]) {
	// The cache has built-in stampede protection where it tracks in-flight
	// requests for every key.
	var count atomic.Int32
	fetchFn := func(_ context.Context) (int, error) {
		count.Add(1)
		time.Sleep(time.Second)
		return 1337, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			// We can ignore the error given the fetchFn we're using.
			val, _ := cacheClient.GetOrFetch(context.Background(), "key2", fetchFn)
			log.Printf("got value: %d\n", val)
			wg.Done()
		}()
	}
	wg.Wait()

	log.Printf("fetchFn was called %d time\n", count.Load())
	log.Println(cacheClient.Get("key2"))
	log.Println("")
}

func demonstrateGetOrFetchBatch(cacheClient *sturdyc.Client[int]) {
	var count atomic.Int32
	fetchFn := func(_ context.Context, ids []string) (map[string]int, error) {
		count.Add(1)
		time.Sleep(time.Second * 5)

		response := make(map[string]int, len(ids))
		for _, id := range ids {
			num, _ := strconv.Atoi(id)
			response[id] = num
		}

		return response, nil
	}

	batches := [][]string{
		{"1", "2", "3", "4", "5"},
		{"6", "7", "8", "9", "10"},
		{"11", "12", "13", "14", "15"},
	}

	// We are going to pass a cache a key function that prefixes each id with
	// the string "my-data-source", and adds an -ID- separator before the actual
	// id. This makes it possible to save the same id for different data
	// sources as the keys would look something like this: my-data-source-ID-1
	keyPrefixFn := cacheClient.BatchKeyFn("my-data-source")

	// Request the keys  for each batch.
	for _, batch := range batches {
		go func() {
			res, _ := cacheClient.GetOrFetchBatch(context.Background(), batch, keyPrefixFn, fetchFn)
			log.Printf("got batch: %v\n", res)
		}()
	}

	// Give the goroutines above a chance to run to ensure that the batches are in-flight.
	time.Sleep(time.Second * 3)

	// Launch another 5 goroutines that are going to pick two random IDs from any of the batches.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			ids := []string{batches[rand.IntN(3)][rand.IntN(5)], batches[rand.IntN(3)][rand.IntN(5)]}
			res, _ := cacheClient.GetOrFetchBatch(context.Background(), ids, keyPrefixFn, fetchFn)
			log.Printf("got batch: %v\n", res)
			wg.Done()
		}()
	}

	wg.Wait()
	log.Printf("fetchFn was called %d times\n", count.Load())
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

	// Create a cache client with the specified configuration.
	cacheClient := sturdyc.New[int](capacity, numShards, ttl, evictionPercentage)

	cacheClient.Set("key1", 99)
	log.Println(cacheClient.Size())
	log.Println(cacheClient.Get("key1"))

	demonstrateGetOrFetch(cacheClient)
	demonstrateGetOrFetchBatch(cacheClient)
}
