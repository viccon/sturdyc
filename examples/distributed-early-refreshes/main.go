package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/viccon/sturdyc"
)

var ids = []string{"1111", "2222", "3333"}

// launchContainer will simulate having a container that runs
// its own in-memory cache, but shares a distributed storage.
func launchContainer(containerIndex int, storage sturdyc.DistributedStorageWithDeletions) {
	apiClient := newAPIClient(storage)
	for i := 0; i < 100; i++ {
		_, err := apiClient.GetShippingOptions(context.Background(), containerIndex, ids, "asc")
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Container", containerIndex, "finished fetching shipping options")
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	distributedStorage := newDistributedStorage()
	numContainers := 5

	wg := &sync.WaitGroup{}
	wg.Add(numContainers)
	for i := 0; i < numContainers; i++ {
		go func() {
			launchContainer(i+1, distributedStorage)
			wg.Done()
		}()
		time.Sleep(50 * time.Millisecond)
	}
	wg.Wait()
}
