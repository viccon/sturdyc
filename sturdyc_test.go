package sturdyc_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randKey(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

type TestMetricsRecorder struct {
	sync.Mutex
	cacheHits            int
	cacheMisses          int
	backgroundRefreshes  int
	synchronousRefreshes int
	missingRecords       int
	evictions            int
	forcedEvictions      int
	evictedEntries       int
	shards               map[int]int
	batchSizes           []int
}

func newTestMetricsRecorder(numShards int) *TestMetricsRecorder {
	return &TestMetricsRecorder{
		shards:     make(map[int]int, numShards),
		batchSizes: make([]int, 0),
	}
}

func (r *TestMetricsRecorder) CacheHit() {
	r.Lock()
	defer r.Unlock()
	r.cacheHits++
}

func (r *TestMetricsRecorder) CacheMiss() {
	r.Lock()
	defer r.Unlock()
	r.cacheMisses++
}

func (r *TestMetricsRecorder) BackgroundRefresh() {
	r.Lock()
	defer r.Unlock()
	r.backgroundRefreshes++
}

func (r *TestMetricsRecorder) SynchronousRefresh() {
	r.Lock()
	defer r.Unlock()
	r.synchronousRefreshes++
}

func (r *TestMetricsRecorder) MissingRecord() {
	r.Lock()
	defer r.Unlock()
	r.missingRecords++
}

func (r *TestMetricsRecorder) ObserveCacheSize(_ func() int) {}

func (r *TestMetricsRecorder) CacheBatchRefreshSize(n int) {
	r.Lock()
	defer r.Unlock()
	r.batchSizes = append(r.batchSizes, n)
}

func (r *TestMetricsRecorder) Eviction() {
	r.Lock()
	defer r.Unlock()
	r.evictions++
}

func (r *TestMetricsRecorder) ForcedEviction() {
	r.Lock()
	defer r.Unlock()
	r.forcedEvictions++
}

func (r *TestMetricsRecorder) EntriesEvicted(n int) {
	r.Lock()
	defer r.Unlock()
	r.evictedEntries += n
}

func (r *TestMetricsRecorder) ShardIndex(index int) {
	r.Lock()
	defer r.Unlock()
	r.shards[index]++
}

func (r *TestMetricsRecorder) validateShardDistribution(t *testing.T, tolerancePercentage int) {
	t.Helper()

	var sum int
	for _, value := range r.shards {
		sum += value
	}
	mean := float64(sum) / float64(len(r.shards))
	upperLimit := mean * (1 + float64(tolerancePercentage)/100)
	lowerLimit := mean * (1 - float64(tolerancePercentage)/100)
	var sb strings.Builder
	for shardIndex, shardSize := range r.shards {
		if float64(shardSize) > upperLimit || float64(shardSize) < lowerLimit {
			if sb.Len() < 1 {
				sb.WriteString("\n")
				sb.WriteString("shard distribution is outside of acceptable tolerance.\n")
				sb.WriteString(fmt.Sprintf("mean: %.1f; tolerance: ±%d percent; limits: [%.1f, %.1f]\n",
					mean, tolerancePercentage, lowerLimit, upperLimit,
				))
			}
			sb.WriteString(fmt.Sprintf("shard%d size: %d\n", shardIndex, shardSize))
		}
	}
	if sb.Len() > 0 {
		t.Error(sb.String())
	}
}

type FetchObserver struct {
	sync.Mutex
	fetchCount       int
	requestedRecords []string
	response         string
	batchResponse    map[string]string
	err              error
	FetchCompleted   chan struct{}
}

func NewFetchObserver(bufferSize int) *FetchObserver {
	return &FetchObserver{
		requestedRecords: make([]string, 0),
		FetchCompleted:   make(chan struct{}, bufferSize),
	}
}

func (f *FetchObserver) Response(id string) {
	f.Lock()
	defer f.Unlock()
	f.response = "value" + id
}

// BatchResponse adds a response to the response cache for each id in ids.
func (f *FetchObserver) BatchResponse(ids []string) {
	f.Lock()
	defer f.Unlock()

	responseMap := make(map[string]string, len(ids))
	for _, id := range ids {
		responseMap[id] = "value" + id
	}
	f.batchResponse = responseMap
}

func (f *FetchObserver) Err(err error) {
	f.err = err
}

// Clear resets the responses, requested records, and error state.
func (f *FetchObserver) Clear() {
	f.Lock()
	defer f.Unlock()

	f.response = ""
	f.batchResponse = make(map[string]string)
	f.requestedRecords = make([]string, 0)
	f.err = nil
}

func (f *FetchObserver) Fetch(_ context.Context) (string, error) {
	f.Lock()
	defer func() {
		f.FetchCompleted <- struct{}{}
		f.Unlock()
	}()

	f.fetchCount++
	return f.response, f.err
}

func (f *FetchObserver) FetchBatch(_ context.Context, ids []string) (map[string]string, error) {
	f.Lock()
	defer func() {
		f.FetchCompleted <- struct{}{}
		f.Unlock()
	}()

	copiedIDs := make([]string, len(ids))
	copy(copiedIDs, ids)
	f.requestedRecords = copiedIDs
	f.fetchCount++

	if f.err != nil {
		return nil, f.err
	}

	response := make(map[string]string)
	for _, id := range ids {
		if val, ok := f.batchResponse[id]; ok {
			response[id] = val
		}
	}

	return response, f.err
}

func (f *FetchObserver) AssertRequestedRecords(t *testing.T, ids []string) {
	t.Helper()
	f.Lock()
	defer f.Unlock()

	sort.Strings(ids)
	sort.Strings(f.requestedRecords)
	if !cmp.Equal(ids, f.requestedRecords) {
		t.Error(cmp.Diff(ids, f.requestedRecords))
	}
}

func (f *FetchObserver) AssertFetchCount(t *testing.T, count int) {
	t.Helper()
	f.Lock()
	defer f.Unlock()

	if count != f.fetchCount {
		t.Errorf("expected fetch count %d, got %d", count, f.fetchCount)
	}
}

func (f *FetchObserver) AssertMaxFetchCount(t *testing.T, count int) {
	t.Helper()
	f.Lock()
	defer f.Unlock()

	if f.fetchCount > count {
		t.Errorf("expected fetch count to be at most %d, got %d", count, f.fetchCount)
	}
}

func (f *FetchObserver) AssertMinFetchCount(t *testing.T, count int) {
	t.Helper()
	f.Lock()
	defer f.Unlock()

	if f.fetchCount < count {
		t.Errorf("expected fetch count to be at minimum %d, got %d", count, f.fetchCount)
	}
}
