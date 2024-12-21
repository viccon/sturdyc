package sturdyc_test

import (
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

type testCutoffCase struct {
	name       string
	numEntries int
	percentile float64
	kIndex     int
}

func TestCutoff(t *testing.T) {
	t.Parallel()

	testCases := []testCutoffCase{
		{
			name:       "1000 entries, 50th percentile",
			numEntries: 1000,
			percentile: 0.5,
			kIndex:     500,
		},
		{
			name:       "1000 entries, 90th percentile",
			numEntries: 1000,
			percentile: 0.9,
			kIndex:     900,
		},
		{
			name:       "999 entries, 16th percentile",
			numEntries: 999,
			percentile: 0.16,
			kIndex:     159,
		},
		{
			name:       "13 entries, 2nd percentile",
			numEntries: 13,
			percentile: 0.02,
			kIndex:     0,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Now()
			timestamps := make([]time.Time, 0, tc.numEntries)
			for i := 0; i < tc.numEntries; i++ {
				newTime := now.Add(time.Duration(i) * time.Second)
				timestamps = append(timestamps, newTime)
			}
			cutoff := sturdyc.FindCutoff(timestamps, tc.percentile)
			if cutoff != timestamps[tc.kIndex] {
				t.Errorf("expected cutoff to be %v, got %v", timestamps[tc.kIndex], cutoff)
			}
		})
	}
}

func TestReturnsEmptyTimeIfArgumentsAreInvalid(t *testing.T) {
	t.Parallel()

	values := make([]time.Time, 0, 10)
	for i := 0; i < 10; i++ {
		values = append(values, time.Now().Add(time.Duration(i)*time.Second))
	}

	if k := sturdyc.FindCutoff(values, -1); !k.IsZero() {
		t.Errorf("expected zero time, got %v", k)
	}

	if k := sturdyc.FindCutoff(values, 1.1); !k.IsZero() {
		t.Errorf("expected zero time, got %v", k)
	}

	if k := sturdyc.FindCutoff([]time.Time{}, 0.4); !k.IsZero() {
		t.Errorf("expected zero time, got %v", k)
	}
}
