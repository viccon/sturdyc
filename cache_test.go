package sturdyc_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

type distributionTestCase struct {
	name                string
	capacity            int
	numShards           int
	tolerancePercentage int
	keyLength           int
}

func TestShardDistribution(t *testing.T) {
	t.Parallel()

	testCases := []distributionTestCase{
		{
			name:                "1_000_000 capacity, 100 shards, 12% tolerance, 16 key length",
			capacity:            1_000_000,
			numShards:           100,
			tolerancePercentage: 12,
			keyLength:           16,
		},
		{
			name:                "1000 capacity, 2 shards, 12% tolerance, 14 key length",
			capacity:            1000,
			numShards:           2,
			tolerancePercentage: 12,
			keyLength:           14,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recorder := newTestMetricsRecorder(tc.numShards)
			c := sturdyc.New[string](tc.capacity, tc.numShards, time.Hour, 5,
				sturdyc.WithNoContinuousEvictions(),
				sturdyc.WithMetrics(recorder),
			)
			for i := 0; i < tc.capacity; i++ {
				key := randKey(tc.keyLength)
				c.Set(key, "value")
			}
			recorder.validateShardDistribution(t, tc.tolerancePercentage)
		})
	}
}

func TestTimeBasedEviction(t *testing.T) {
	t.Parallel()
	capacity := 10_000
	numShards := 100
	ttl := time.Hour
	evictionPercentage := 5
	evictionInterval := time.Second
	clock := sturdyc.NewTestClock(time.Now())
	metricRecorder := newTestMetricsRecorder(numShards)
	c := sturdyc.New[string](
		capacity,
		numShards,
		ttl,
		evictionPercentage,
		sturdyc.WithMetrics(metricRecorder),
		sturdyc.WithClock(clock),
		sturdyc.WithEvictionInterval(evictionInterval),
	)

	for i := 0; i < capacity; i++ {
		c.Set(randKey(12), "value")
	}

	// Expire all entries.
	clock.Add(ttl + 1)

	// Next, we'll loop through each shard while moving the clock by the evictionInterval. We'll
	// sleep for a brief duration to allow the goroutines that were waiting for the timer to run.
	for i := 0; i < numShards; i++ {
		clock.Add(time.Second + 1)
		time.Sleep(5 * time.Millisecond)
	}

	metricRecorder.Lock()
	defer metricRecorder.Unlock()
	if metricRecorder.evictedEntries != capacity {
		t.Errorf("expected %d evicted entries, got %d", capacity, metricRecorder.evictedEntries)
	}
}

type forcedEvictionTestCase struct {
	name               string
	capacity           int
	writes             int
	numShards          int
	evictionPercentage int
	minEvictions       int
	maxEvictions       int
}

func TestForcedEvictions(t *testing.T) {
	t.Parallel()

	testCases := []forcedEvictionTestCase{
		{
			name:               "1000 capacity, 100_000 writes, 100 shards, 5% forced evictions",
			capacity:           10_000,
			writes:             100_000,
			numShards:          100,
			evictionPercentage: 5,
			minEvictions:       20_000, // Perfect shard distribution.
			maxEvictions:       20_800, // Accounting for a 4% tolerance.
		},
		{
			name:               "100 capacity, 10_000 writes, 10 shards, 1% forced evictions",
			capacity:           100,
			writes:             10_000,
			numShards:          10,
			evictionPercentage: 1,
			minEvictions:       9999,
			maxEvictions:       10001,
		},
		{
			name:               "100 capacity, 1000 writes, 10 shards, 100% forced evictions",
			capacity:           100,
			writes:             1000,
			numShards:          10,
			evictionPercentage: 100,
			minEvictions:       100,
			maxEvictions:       120,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := newTestMetricsRecorder(tc.numShards)
			c := sturdyc.New[string](tc.capacity,
				tc.numShards,
				time.Hour,
				tc.evictionPercentage,
				sturdyc.WithMetrics(recorder),
				sturdyc.WithNoContinuousEvictions(),
			)

			// Start by filling the sturdyc.
			for i := 0; i < tc.capacity; i++ {
				key := randKey(12)
				c.Set(key, "value")
			}

			// Next, we'll write to the cache to force evictions.
			for i := 0; i < tc.writes; i++ {
				key := randKey(12)
				c.Set(key, "value")
			}

			if recorder.forcedEvictions < tc.minEvictions || recorder.forcedEvictions > tc.maxEvictions {
				t.Errorf(
					"expected forced evictions between %d and %d, got %d",
					tc.minEvictions, tc.maxEvictions, recorder.forcedEvictions,
				)
			}
		})
	}
}

func TestDisablingForcedEvictionMakesSetANoop(t *testing.T) {
	t.Parallel()

	capacity := 100
	numShards := 10
	ttl := time.Hour
	// Setting the eviction percentage to 0 should disable forced evictions.
	evictionpercentage := 0
	metricRecorder := newTestMetricsRecorder(numShards)
	c := sturdyc.New[string](
		capacity,
		numShards,
		ttl,
		evictionpercentage,
		sturdyc.WithMetrics(metricRecorder),
	)

	for i := 0; i < capacity*10; i++ {
		c.Set(randKey(12), "value")
	}

	metricRecorder.Lock()
	defer metricRecorder.Unlock()
	if metricRecorder.forcedEvictions > 0 {
		t.Errorf("expected no forced evictions, got %d", metricRecorder.forcedEvictions)
	}
}

func TestSetMany(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[int](1000, 10, time.Hour, 5)

	if c.Size() != 0 {
		t.Errorf("expected cache size to be 0, got %d", c.Size())
	}

	records := make(map[string]int, 10)
	for i := 0; i < 10; i++ {
		records[strconv.Itoa(i)] = i
	}
	c.SetMany(records)

	if c.Size() != 10 {
		t.Errorf("expected cache size to be 10, got %d", c.Size())
	}

	keys := c.ScanKeys()
	if len(keys) != 10 {
		t.Errorf("expected 10 keys, got %d", len(keys))
	}
	for _, key := range keys {
		if _, ok := records[key]; !ok {
			t.Errorf("expected key %s to be in the cache", key)
		}
	}
}

func TestSetManyKeyFn(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[int](1000, 10, time.Hour, 5)

	if c.Size() != 0 {
		t.Errorf("expected cache size to be 0, got %d", c.Size())
	}

	records := make(map[string]int, 10)
	for i := 0; i < 10; i++ {
		records[strconv.Itoa(i)] = i
	}
	c.SetManyKeyFn(records, c.BatchKeyFn("foo"))

	if c.Size() != 10 {
		t.Errorf("expected cache size to be 10, got %d", c.Size())
	}

	keys := c.ScanKeys()
	if len(keys) != 10 {
		t.Errorf("expected 10 keys, got %d", len(keys))
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, "foo") {
			t.Errorf("expected key %s to start with foo", key)
		}
	}
}

func TestGetMany(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[int](1000, 10, time.Hour, 5)

	if c.Size() != 0 {
		t.Errorf("expected cache size to be 0, got %d", c.Size())
	}

	records := make(map[string]int, 10)
	for i := 0; i < 10; i++ {
		records[strconv.Itoa(i)] = i
	}
	c.SetMany(records)

	keys := make([]string, 0, 10)
	for key := range records {
		keys = append(keys, key)
	}

	cacheHits := c.GetMany(keys)
	if len(cacheHits) != 10 {
		for key := range records {
			if _, ok := cacheHits[key]; !ok {
				t.Errorf("expected key %s to be in the cache", key)
			}
		}
	}
}

func TestEvictsAndReturnsTheCorrectSize(t *testing.T) {
	t.Parallel()

	// Let's create a cache with a capacity of 100 and a
	// single shard. We'll set the eviction percentage to 10%.
	client := sturdyc.New[int](100, 1, time.Hour, 10)

	// Now, if we were to write 101 items, which is 1 more
	// than our capacity, we expect 10% to have been evicted.
	for i := 0; i < 101; i++ {
		client.Set(strconv.Itoa(i), i)
	}

	if client.Size() != 91 {
		t.Errorf("expected cache size to be 91, got %d", client.Size())
	}
}

func TestDeletesAllItemsAcrossMultipleShards(t *testing.T) {
	t.Parallel()

	client := sturdyc.New[string](1_000_000, 1000, time.Hour, 10)

	ids := make([]string, 0, 10_000)
	for i := 0; i < 10_000; i++ {
		id := randKey(12)
		ids = append(ids, id)
		client.Set(id, "value")
	}

	if client.Size() != 10_000 {
		t.Errorf("expected cache size to be 10_000, got %d", client.Size())
	}

	for _, id := range ids {
		client.Delete(id)
	}

	if client.Size() != 0 {
		t.Errorf("expected cache size to be 0, got %d", client.Size())
	}
}

func TestReportsMetricsForHitsAndMisses(t *testing.T) {
	t.Parallel()

	metricsRecorder := newTestMetricsRecorder(10)
	client := sturdyc.New[string](100, 10, time.Hour, 5,
		sturdyc.WithMetrics(metricsRecorder),
	)

	client.Set("existing-key", "value")
	client.Get("existing-key")
	client.Get("non-existent-key")

	if metricsRecorder.cacheHits != 1 {
		t.Errorf("expected 1 cache hit, got %d", metricsRecorder.cacheHits)
	}

	if metricsRecorder.cacheMisses != 1 {
		t.Errorf("expected 1 cache miss, got %d", metricsRecorder.cacheMisses)
	}
}

type AliasTestItem struct {
	key         string
	value       string
	shouldExist bool
}

type AliasTestCase struct {
	name        string
	init        func(client *sturdyc.Client[*TestCacheItem])
	items       []AliasTestItem
	shouldPanic bool
}

type TestCacheItem struct {
	sturdyc.ISturdyCItem
	key     string
	aliases []string
}

func NewTestCacheItem(key string, aliases []string) *TestCacheItem {
	return &TestCacheItem{
		key:     key,
		aliases: aliases,
	}
}

func (t *TestCacheItem) GetCacheKey() string {
	return t.key
}

func (t *TestCacheItem) GetCacheAliasKeys() []string {
	return t.aliases
}

func TestAliases(t *testing.T) {

	testCases := []AliasTestCase{
		{
			name: "A key can be retrieved by its original key and its aliases",
			init: func(client *sturdyc.Client[*TestCacheItem]) {
				client.Set("key-with-aliases", NewTestCacheItem("key-with-aliases", []string{"alias1", "alias2"}))
			},
			items: []AliasTestItem{
				{key: "key-with-aliases", value: "value", shouldExist: true},
				{key: "alias1", value: "value", shouldExist: true},
				{key: "alias2", value: "value", shouldExist: true},
				{key: "alias3", value: "", shouldExist: false},
			},
		},
		{
			name: "A previously created key can be deleted and that all aliases are deleted",
			init: func(client *sturdyc.Client[*TestCacheItem]) {
				client.Set("all-aliases-deleted", NewTestCacheItem("all-aliases-deleted", []string{"alias1", "alias2"}))
				client.Delete("all-aliases-deleted")
			},
			items: []AliasTestItem{
				{key: "all-aliases-deleted", value: "", shouldExist: false},
				{key: "alias1", value: "", shouldExist: false},
				{key: "alias2", value: "", shouldExist: false},
			},
		},
		{
			name: "A key can be created with aliases, then updated with new aliases, and the old ones are deleted",
			init: func(client *sturdyc.Client[*TestCacheItem]) {
				client.Set("old-aliases-test", NewTestCacheItem("old-aliases-test", []string{"alias1", "alias2"}))
				client.Set("old-aliases-test", NewTestCacheItem("old-aliases-test", []string{"alias3", "alias4"}))
			},
			items: []AliasTestItem{
				{key: "old-aliases-test", value: "value", shouldExist: true},
				{key: "alias1", value: "", shouldExist: false},
				{key: "alias2", value: "", shouldExist: false},
				{key: "alias3", value: "value", shouldExist: true},
				{key: "alias4", value: "value", shouldExist: true},
			},
		},
		{
			name: "Panic if a different key is inserted with the same alias",
			init: func(client *sturdyc.Client[*TestCacheItem]) {
				client.Set("should-not-panic", NewTestCacheItem("should-not-panic", []string{"alias1", "alias2"}))
				client.Set("should-panic", NewTestCacheItem("should-panic", []string{"alias1", "alias2"}))
			},
			shouldPanic: true,
		},
		{
			name: "Key functions override input keys",
			init: func(client *sturdyc.Client[*TestCacheItem]) {
				client.Set("key-with-aliases", NewTestCacheItem("prefix:key-with-aliases", []string{"prefix:alias1", "prefix:alias2"}))
			},
			items: []AliasTestItem{
				{key: "prefix:key-with-aliases", value: "value", shouldExist: true},
				{key: "prefix:alias1", value: "value", shouldExist: true},
				{key: "prefix:alias2", value: "value", shouldExist: true},
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			metricsRecorder := newTestMetricsRecorder(10)
			client := sturdyc.New[*TestCacheItem](100, 10, time.Hour, 5,
				sturdyc.WithMetrics(metricsRecorder),
			)

			initErr := func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("panic occurred: %v", r)
					}
				}()
				testCase.init(client)
				return nil
			}()
			if testCase.shouldPanic && initErr == nil {
				t.Errorf("expected panic, but no panic occurred")
				return
			}
			if !testCase.shouldPanic && initErr != nil {
				t.Errorf("unexpected panic: %v", initErr)
				return
			}
			for _, itemData := range testCase.items {
				value, ok := client.Get(itemData.key)
				if itemData.shouldExist && !ok {
					t.Errorf("expected key '%s' to be in the cache with value '%s', but it was not", itemData.key, itemData.value)
					continue
				}
				if !itemData.shouldExist && ok {
					t.Errorf("expected key '%s' to not be in the cache, but it was with value '%s'", itemData.key, value)
					continue
				}
			}
		})
	}
}
