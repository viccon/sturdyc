package sturdyc_test

import (
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

type benchmarkMetric[T any] struct {
	getOps    int
	setOps    int
	hits      int
	evictions int
}

func (b *benchmarkMetric[T]) recordGet(c *sturdyc.Client[T], key string) {
	b.getOps++
	_, ok := c.Get(key)
	if ok {
		b.hits++
	}
}

func (b *benchmarkMetric[T]) recordSet(c *sturdyc.Client[T], key string, value T) {
	b.setOps++
	evict := c.Set(key, value)
	if evict {
		b.evictions++
	}
}

type benchmarkMetrics[T any] []benchmarkMetric[T]

func (metrics benchmarkMetrics[T]) hitRate() (float64, string) {
	var ops, hits int
	for _, metrics := range metrics {
		ops += metrics.getOps
		hits += metrics.hits
	}
	return float64(hits) / float64(ops), "hits/op"
}

func (metrics benchmarkMetrics[T]) evictions() (float64, string) {
	var ops, evictions int
	for _, metrics := range metrics {
		ops += metrics.setOps
		evictions += metrics.evictions
	}
	return float64(evictions) / float64(ops), "evictions/op"
}

func BenchmarkGetConcurrent(b *testing.B) {
	cacheKey := "key"
	capacity := 1_000_000
	numShards := 100
	ttl := time.Hour
	evictionPercentage := 5
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)
	c.Set(cacheKey, "value")

	metrics := make(benchmarkMetrics[string], 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var metric benchmarkMetric[string]
		for pb.Next() {
			metric.recordGet(c, cacheKey)
		}
		metrics = append(metrics, metric)
	})
	b.StopTimer()
	b.ReportMetric(metrics.hitRate())
}

func BenchmarkSetConcurrent(b *testing.B) {
	capacity := 10_000_000
	numShards := 10_000
	ttl := time.Hour
	evictionPercentage := 5
	c := sturdyc.New[string](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
	)

	metrics := make(benchmarkMetrics[string], 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var metric benchmarkMetric[string]
		for pb.Next() {
			// NOTE: The benchmark includes the time for generating random keys.
			key := randKey(16)
			metric.recordSet(c, key, "value")
		}
		metrics = append(metrics, metric)
	})
	b.StopTimer()
	b.ReportMetric(metrics.evictions())
}
