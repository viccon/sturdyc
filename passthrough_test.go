package sturdyc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/viccon/sturdyc"
)

func TestPassthrough(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10000
	numShards := 100
	ttl := time.Minute
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)

	id := "1"
	numPassthroughs := 1000
	fetchObserver := NewFetchObserver(numPassthroughs + 1)
	fetchObserver.Response(id)

	res, err := sturdyc.Passthrough(ctx, c, id, fetchObserver.Fetch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != "value1" {
		t.Errorf("expected value1, got %v", res)
	}

	for i := 0; i < numPassthroughs; i++ {
		res, passthroughErr := sturdyc.Passthrough(ctx, c, id, fetchObserver.Fetch)
		if passthroughErr != nil {
			t.Fatalf("expected no error, got %v", passthroughErr)
		}

		if res != "value1" {
			t.Errorf("expected value1, got %v", res)
		}
	}

	for i := 0; i < numPassthroughs; i++ {
		<-fetchObserver.FetchCompleted
	}

	fetchObserver.AssertFetchCount(t, numPassthroughs+1)

	fetchObserver.Clear()
	fetchObserver.Err(errors.New("error"))
	cachedRes, err := sturdyc.Passthrough(ctx, c, id, fetchObserver.Fetch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatal(err)
	}
	if cachedRes != "value1" {
		t.Errorf("expected value1, got %v", cachedRes)
	}
	fetchObserver.AssertFetchCount(t, numPassthroughs+2)
	if c.NumKeysInflight() > 0 {
		t.Errorf("expected no inflight keys, got %v", c.NumKeysInflight())
	}
}

func TestPassthroughBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	capacity := 10000
	numShards := 100
	ttl := time.Minute
	evictionPercentage := 10
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)

	idBatch := []string{"1", "2", "3"}
	numPassthroughs := 200
	fetchObserver := NewFetchObserver(numPassthroughs + 2)
	fetchObserver.BatchResponse(idBatch)

	res, err := sturdyc.PassthroughBatch(ctx, c, idBatch, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !cmp.Equal(res, map[string]string{"1": "value1", "2": "value2", "3": "value3"}) {
		t.Errorf("expected value1, value2, value3, got %v", res)
	}

	for i := 0; i < numPassthroughs; i++ {
		res, passthroughErr := sturdyc.PassthroughBatch(ctx, c, idBatch, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
		if passthroughErr != nil {
			t.Fatalf("expected no error, got %v", passthroughErr)
		}
		if !cmp.Equal(res, map[string]string{"1": "value1", "2": "value2", "3": "value3"}) {
			t.Errorf("expected value1, value2, value3, got %v", res)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// We can't say exactly how many got through because there are multiple
	// goroutines running and it requires a lock to remove an in-flight key.
	fetchObserver.AssertMinFetchCount(t, numPassthroughs/2)
	fetchObserver.AssertMaxFetchCount(t, numPassthroughs+1)

	fetchObserver.Clear()
	fetchObserver.Err(errors.New("error"))
	cachedRes, err := sturdyc.PassthroughBatch(ctx, c, idBatch, c.BatchKeyFn("item"), fetchObserver.FetchBatch)
	<-fetchObserver.FetchCompleted
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(cachedRes, map[string]string{"1": "value1", "2": "value2", "3": "value3"}) {
		t.Errorf("expected value1, value2, value3, got %v", cachedRes)
	}
	fetchObserver.AssertFetchCount(t, numPassthroughs+2)
	if c.NumKeysInflight() > 0 {
		t.Errorf("expected no inflight keys, got %v", c.NumKeysInflight())
	}
}
