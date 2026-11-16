package sturdyc_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

func assertSizeBytes(t *testing.T, recorder *TestMetricsRecorder, want int64) {
	t.Helper()
	if got := recorder.sizeBytes(); got != want {
		t.Errorf("expected the cache size to be %d bytes, got %d", want, got)
	}
}

// sizeOf returns the number of bytes that a cache reports after a single
// value has been written to it. Comparing the result for two different values
// allows us to test the estimation without depending on the exact overhead
// of the entries, which varies between architectures.
func sizeOf[T any](value T) int64 {
	recorder := newTestMetricsRecorder(1)
	c := sturdyc.New[T](10, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMetrics(recorder),
	)
	c.Set("key", value)
	return recorder.sizeBytes()
}

func TestSizeBytesTracksWritesOverwritesAndDeletes(t *testing.T) {
	t.Parallel()

	recorder := newTestMetricsRecorder(10)
	c := sturdyc.New[string](1000, 10, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMetrics(recorder),
	)
	assertSizeBytes(t, recorder, 0)

	c.Set("key1", "value")
	entrySize := recorder.sizeBytes()
	if entrySize <= int64(len("key1")+len("value")) {
		t.Fatalf("expected the entry size to include the overhead of the entry, got %d bytes", entrySize)
	}

	// An entry with the same key and value lengths should be of the same size.
	c.Set("key2", "value")
	assertSizeBytes(t, recorder, 2*entrySize)

	// Overwriting an entry with a longer value should only add the difference.
	c.Set("key1", "value-longer")
	assertSizeBytes(t, recorder, 2*entrySize+int64(len("-longer")))

	// Overwriting an entry with the same value shouldn't change the size.
	c.Set("key1", "value-longer")
	assertSizeBytes(t, recorder, 2*entrySize+int64(len("-longer")))

	c.Delete("key1")
	assertSizeBytes(t, recorder, entrySize)

	// Deleting a key that doesn't exist should be a no-op.
	c.Delete("key3")
	assertSizeBytes(t, recorder, entrySize)

	// The key should be included in the size of the entry.
	c.Set("longer-key2", "value")
	assertSizeBytes(t, recorder, 2*entrySize+int64(len("longer-")))

	c.Delete("key2")
	c.Delete("longer-key2")
	assertSizeBytes(t, recorder, 0)
}

func TestSizeBytesMissingRecords(t *testing.T) {
	t.Parallel()

	recorder := newTestMetricsRecorder(1)
	c := sturdyc.New[string](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMissingRecordStorage(),
		sturdyc.WithMetrics(recorder),
	)

	c.StoreMissingRecord("key1")
	missingRecordSize := recorder.sizeBytes()
	if missingRecordSize <= 0 {
		t.Fatalf("expected missing records to occupy memory, got %d bytes", missingRecordSize)
	}

	// A missing record should be the same size as an entry with a zero value.
	c.Set("key2", "")
	assertSizeBytes(t, recorder, 2*missingRecordSize)

	// Replacing the missing record with an actual value should add the size of the value.
	c.Set("key1", "value")
	assertSizeBytes(t, recorder, 2*missingRecordSize+int64(len("value")))
}

func TestSizeBytesTimeBasedEvictions(t *testing.T) {
	t.Parallel()

	ttl := time.Hour
	evictionInterval := time.Second
	clock := sturdyc.NewTestClock(time.Now())
	recorder := newTestMetricsRecorder(4)
	c := sturdyc.New[string](1000, 4, ttl, 5,
		sturdyc.WithClock(clock),
		sturdyc.WithEvictionInterval(evictionInterval),
		sturdyc.WithMetrics(recorder),
	)

	c.Set("key-0000", "value")
	entrySize := recorder.sizeBytes()

	// Write one batch of entries that are going to expire, and
	// another batch that is going to be written later on.
	for i := 1; i < 100; i++ {
		c.Set(fmt.Sprintf("key-%04d", i), "value")
	}
	clock.Add(ttl / 2)
	for i := 100; i < 150; i++ {
		c.Set(fmt.Sprintf("key-%04d", i), "value")
	}
	clock.Add(ttl/2 + 1)

	// Next, we'll advance the clock until the first batch has been evicted
	// from every shard. The ticker drops ticks if the eviction goroutine is
	// busy, so we'll keep going until we reach the expected size.
	deadline := time.Now().Add(5 * time.Second)
	for c.Size() > 50 && time.Now().Before(deadline) {
		clock.Add(evictionInterval)
		time.Sleep(time.Millisecond)
	}

	if c.Size() != 50 {
		t.Fatalf("expected 50 entries to remain in the cache, got %d", c.Size())
	}
	assertSizeBytes(t, recorder, 50*entrySize)
}

func TestSizeBytesForcedEvictions(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		evictionPercentage int
		uniqueKeys         int
	}{
		{name: "evict 0 percent", evictionPercentage: 0, uniqueKeys: 1000},
		{name: "evict 5 percent", evictionPercentage: 5, uniqueKeys: 1000},
		{name: "evict 50 percent", evictionPercentage: 50, uniqueKeys: 1000},
		{name: "evict 100 percent", evictionPercentage: 100, uniqueKeys: 1000},
		{name: "evict 5 percent with overwrites", evictionPercentage: 5, uniqueKeys: 150},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := newTestMetricsRecorder(2)
			c := sturdyc.New[string](100, 2, time.Hour, tc.evictionPercentage,
				sturdyc.WithNoContinuousEvictions(),
				sturdyc.WithMetrics(recorder),
			)

			c.Set("key-0000", "value")
			entrySize := recorder.sizeBytes()

			// Every entry is of the same size, which means that the size of the
			// cache should always be the number of entries times the entry size.
			for i := 0; i < 1000; i++ {
				c.Set(fmt.Sprintf("key-%04d", i%tc.uniqueKeys), "value")
				if got, want := recorder.sizeBytes(), int64(c.Size())*entrySize; got != want {
					t.Fatalf("write %d: expected the cache size to be %d bytes, got %d", i, want, got)
				}
			}
		})
	}
}

type sizeTestNode struct {
	name string
	next *sizeTestNode
}

// newSizeTestList returns a linked list of n nodes, where
// the last node points back to the first if cyclic is true.
func newSizeTestList(n int, cyclic bool) *sizeTestNode {
	head := &sizeTestNode{name: "node"}
	current := head
	for i := 1; i < n; i++ {
		current.next = &sizeTestNode{name: "node"}
		current = current.next
	}
	if cyclic {
		current.next = head
	}
	return head
}

type sizeTestItem struct {
	ID string
}

type sizeTestPair struct {
	A *[256]byte
	B *[256]byte
}

func TestSizeBytesEstimation(t *testing.T) {
	t.Parallel()

	shared := &[256]byte{}

	cyclicA := &sizeTestNode{name: "a"}
	cyclicB := &sizeTestNode{name: "b", next: cyclicA}
	cyclicA.next = cyclicB
	chain := &sizeTestNode{name: "a", next: &sizeTestNode{name: "b"}}

	testCases := []struct {
		name string
		diff int64
		want int64
	}{
		{
			name: "string length",
			diff: sizeOf("hello world") - sizeOf(""),
			want: int64(len("hello world")),
		},
		{
			name: "slice capacity",
			diff: sizeOf(make([]byte, 0, 1024)) - sizeOf([]byte(nil)),
			want: 1024,
		},
		{
			name: "map contents",
			diff: sizeOf(map[string]string{"aaaa": "bbbbb"}) - sizeOf(map[string]string{"a": "bb"}),
			want: 6,
		},
		{
			name: "interface values",
			diff: sizeOf[any]("hellohello") - sizeOf[any]("hello"),
			want: 5,
		},
		{
			name: "strings in a slice of structs",
			diff: sizeOf([]sizeTestItem{{ID: "aaa"}, {ID: "bbbb"}}) - sizeOf([]sizeTestItem{{ID: "a"}, {ID: "bb"}}),
			want: 4,
		},
		{
			name: "unexported fields behind a pointer",
			diff: sizeOf(&sizeTestNode{name: "abcdef"}) - sizeOf(&sizeTestNode{name: "a"}),
			want: 5,
		},
		{
			name: "shared pointers are counted once",
			diff: sizeOf(sizeTestPair{A: shared, B: shared}) - sizeOf(sizeTestPair{A: shared}),
			want: 0,
		},
		{
			name: "distinct pointers are counted separately",
			diff: sizeOf(sizeTestPair{A: &[256]byte{}, B: &[256]byte{}}) - sizeOf(sizeTestPair{A: &[256]byte{}}),
			want: 256,
		},
		{
			name: "cyclic references are counted once",
			diff: sizeOf(cyclicA) - sizeOf(chain),
			want: 0,
		},
		{
			name: "cyclic references in values with many pointers are counted once",
			diff: sizeOf(newSizeTestList(100, true)) - sizeOf(newSizeTestList(100, false)),
			want: 0,
		},
		{
			name: "every node in a long list is counted",
			diff: sizeOf(newSizeTestList(100, false)) - sizeOf(newSizeTestList(99, false)),
			want: sizeOf(newSizeTestList(1, false)) - sizeOf[*sizeTestNode](nil),
		},
	}

	for _, tc := range testCases {
		if tc.diff != tc.want {
			t.Errorf("%s: expected a difference of %d bytes, got %d", tc.name, tc.want, tc.diff)
		}
	}

	// Values that hold references should be larger than their zero values.
	if sizeOf[any](nil) >= sizeOf[any](1) {
		t.Errorf("expected a boxed int to be larger than a nil interface")
	}
	if sizeOf[*sizeTestNode](nil) >= sizeOf(&sizeTestNode{}) {
		t.Errorf("expected a pointer to a struct to be larger than a nil pointer")
	}
	if sizeOf[map[string]int](nil) >= sizeOf(map[string]int{"a": 1}) {
		t.Errorf("expected a map with entries to be larger than a nil map")
	}
}

type testDistributedMetricsRecorder struct {
	*TestMetricsRecorder
}

func (r *testDistributedMetricsRecorder) DistributedCacheHit()      {}
func (r *testDistributedMetricsRecorder) DistributedCacheMiss()     {}
func (r *testDistributedMetricsRecorder) DistributedRefresh()       {}
func (r *testDistributedMetricsRecorder) DistributedMissingRecord() {}
func (r *testDistributedMetricsRecorder) DistributedFallback()      {}

func TestSizeBytesDistributedMetrics(t *testing.T) {
	t.Parallel()

	recorder := newTestMetricsRecorder(1)
	c := sturdyc.New[string](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithDistributedMetrics(&testDistributedMetricsRecorder{recorder}),
	)

	c.Set("key", "value")
	if recorder.sizeBytes() <= 0 {
		t.Fatalf("expected the size to be reported through the distributed metrics recorder")
	}
	c.Delete("key")
	assertSizeBytes(t, recorder, 0)
}
