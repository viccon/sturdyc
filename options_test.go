package sturdyc_test

import (
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

func TestPanicsIfCapacityIsLessThanZero(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when capacity is less than zero")
		}
	}()
	sturdyc.New[string](-1, 1, time.Hour, 5)
}

func TestPanicsIfTheNumberOfShardsIsLessThanOne(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use zero shards")
		}
	}()
	sturdyc.New[string](100, 0, time.Hour, 5)
}

func TestPanicsIfTTLIsLessThanOne(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use zero as TTL")
		}
	}()
	sturdyc.New[string](100, 2, 0, 5)
}

func TestPanicsIfEvictionPercentageIsLessThanZero(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use -1 as eviction percentage")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, -1)
}

func TestPanicsIfEvictionPercentageIsGreaterThanOneHundred(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use 101 as eviction percentage")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, 101)
}

func TestPanicsIfRefreshBufferingIsEnabledWithoutStampedeProtection(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use buffered refreshes without stampede protection")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, 5,
		sturdyc.WithRefreshCoalescing(10, time.Minute),
	)
}

func TestPanicsIfTheRefreshBufferSizeIsLessThanOne(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use buffered refreshes with zero as size")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, 5,
		sturdyc.WithEarlyRefreshes(time.Minute, time.Hour, time.Hour*2, time.Second),
		sturdyc.WithRefreshCoalescing(0, time.Minute),
	)
}

func TestPanicsIfTheRefreshBufferTimeoutIsLessThanOne(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use 0 as buffer timeout")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, 5,
		sturdyc.WithEarlyRefreshes(time.Minute, time.Hour, time.Hour*2, time.Second),
		sturdyc.WithRefreshCoalescing(10, 0),
	)
}

func TestPanicsIfTheEvictionIntervalIsSetToLessThanOne(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use 0 as eviction interval")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, 5,
		sturdyc.WithEvictionInterval(0),
	)
}

func TestPanicsIfTheMinRefreshTimeIsGreaterThanTheMaxRefreshTime(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use hour as min refresh time and minute as max")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, 5,
		sturdyc.WithEarlyRefreshes(time.Hour, time.Minute, time.Hour*2, time.Second),
	)
}

func TestPanicsIfTheBackgroundRefreshTimeIsGreaterThanTheSynchronousRefreshTime(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use a greater background refresh time than the synchronous refresh time")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, 5,
		sturdyc.WithEarlyRefreshes(time.Minute, time.Hour*2, time.Hour*1, time.Second),
	)
}

func TestPanicsIfTheRetryBaseDelayIsLessThanZero(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("expected a panic when trying to use -1 as retry base delay")
		}
	}()
	sturdyc.New[string](100, 10, time.Minute, 5,
		sturdyc.WithEarlyRefreshes(time.Minute, time.Hour, time.Hour*2, -1),
	)
}
