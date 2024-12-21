package sturdyc_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

func TestTimeBasedKeys(t *testing.T) {
	t.Parallel()

	timeValue := time.Now()
	regularTimeClient := sturdyc.New[any](100, 1, time.Hour, 5)

	clientSecondClock := sturdyc.NewTestClock(time.Now().Truncate(time.Second))
	relativeTimeClientSecond := sturdyc.New[any](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithRelativeTimeKeyFormat(time.Second),
		sturdyc.WithClock(clientSecondClock),
	)

	clientMinuteClock := sturdyc.NewTestClock(time.Now().Truncate(time.Minute))
	relativeTimeClientMinute := sturdyc.New[any](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithRelativeTimeKeyFormat(time.Minute),
		sturdyc.WithClock(clientMinuteClock),
	)

	clientHourClock := sturdyc.NewTestClock(time.Now().Truncate(time.Hour))
	relativeTimeClientHour := sturdyc.New[any](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithRelativeTimeKeyFormat(time.Hour),
		sturdyc.WithClock(clientHourClock),
	)

	type opts struct {
		Time time.Time
	}

	epochString := strconv.FormatInt(timeValue.Unix(), 10)
	keyOne := regularTimeClient.PermutatedKey("keyPrefix", opts{timeValue})
	if keyOne != "keyPrefix-"+epochString {
		t.Errorf("got: %s wanted: %s", keyOne, "keyPrefix-"+epochString)
	}

	// Adding 1 minute and 10 seconds to the time. No truncation should occur.
	secondTimeValue := clientSecondClock.Now()
	clientSecondClock.Add(time.Second * 70)
	secondKey := relativeTimeClientSecond.PermutatedKey("keyPrefix", opts{secondTimeValue})
	wantSecondKey := "keyPrefix-(-)0h01m10s"
	if secondKey != wantSecondKey {
		t.Errorf("got: %s wanted: %s", secondKey, wantSecondKey)
	}

	// Adding 1h, 1 minute and 10 seconds to the time. The seconds should be truncated.
	minuteTimeValue := clientMinuteClock.Now()
	clientMinuteClock.Add(time.Hour + time.Minute + (time.Second * 10))
	minuteKey := relativeTimeClientMinute.PermutatedKey("keyPrefix", opts{minuteTimeValue})
	wantMinuteKey := "keyPrefix-(-)1h01m00s"
	if minuteKey != wantMinuteKey {
		t.Errorf("got: %s wanted: %s", minuteKey, wantMinuteKey)
	}

	// Adding 72h to the time. The minutes and seconds should be truncated.
	hourTimeValue := clientHourClock.Now()
	clientHourClock.Add((72 * time.Hour) + time.Minute + time.Second)
	hourKey := relativeTimeClientHour.PermutatedKey("keyPrefix", opts{hourTimeValue})
	wantHourKey := "keyPrefix-(-)72h00m00s"
	if hourKey != wantHourKey {
		t.Errorf("got: %s wanted: %s", hourKey, wantHourKey)
	}
}

func TestPermutatedRelativeTimeKeys(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[any](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithRelativeTimeKeyFormat(time.Minute),
	)
	prefix := "cache-key"
	stringValue := "string"
	intValue := 1
	stringSliceValue := []string{"string1", "string2"}
	intSliceValue := []int{1, 2}
	boolValue := true
	negativeTimeValue := time.Now().Add(-time.Hour)
	positiveTimeValue := time.Now().Add(90 * time.Minute)

	type queryParams struct {
		String                string
		StringPointer         *string
		StringNilPointer      *string
		Int                   int
		IntPointer            *int
		IntNilPointer         *int
		StringSlice           []string
		StringNilSlice        []string
		StringSlicePointer    *[]string
		StringNilSlicePointer *[]string
		IntSlice              []int
		IntNilSlice           []int
		IntSlicePointer       *[]int
		IntNilSlicePointer    *[]int
		Bool                  bool
		BoolPointer           *bool
		BoolNilPointer        *bool
		Time                  time.Time
		TimePointer           *time.Time
		TimeNilPointer        *time.Time
	}

	// We'll specify the fields in a different order than they are defined
	// in the struct. This should have no impact on the cache key.
	queryOne := queryParams{
		Time:                  negativeTimeValue,
		TimePointer:           &positiveTimeValue,
		TimeNilPointer:        nil,
		String:                stringValue,
		StringPointer:         &stringValue,
		StringNilPointer:      nil,
		Int:                   intValue,
		IntPointer:            &intValue,
		IntNilPointer:         nil,
		StringSlice:           stringSliceValue,
		StringNilSlice:        nil,
		StringSlicePointer:    &stringSliceValue,
		StringNilSlicePointer: nil,
		IntSlice:              intSliceValue,
		IntNilSlice:           nil,
		IntSlicePointer:       &intSliceValue,
		IntNilSlicePointer:    nil,
		Bool:                  boolValue,
		BoolPointer:           &boolValue,
		BoolNilPointer:        nil,
	}

	//nolint:lll // This is a test case, we want to keep the line length.
	want := "cache-key-string-string-nil-1-1-nil-string1,string2-nil-string1,string2-nil-1,2-nil-1,2-nil-true-true-nil-(-)1h00m00s-(+)1h30m00s-nil"
	got := c.PermutatedKey(prefix, queryOne)

	if got != want {
		t.Errorf("got: %s wanted: %s", got, want)
	}
}

func TestPermutatedKeyHandlesEmptySlices(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[any](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
	)
	type queryParams struct {
		StringValues []string
		IntValues    []int
	}
	params := queryParams{
		StringValues: []string{},
		IntValues:    []int{},
	}

	want := "cache-key-empty-empty"
	got := c.PermutatedKey("cache-key", params)

	if got != want {
		t.Errorf("got: %s wanted: %s", got, want)
	}
}

func TestPermutatedBatchKeyFn(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[any](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
	)
	type queryParams struct {
		IncludeUpcoming bool
		// Note that limit isn't exported (lowercase), hence it should be omitted from the key.
		limit int
	}

	cacheKeyFunc := c.PermutatedBatchKeyFn("cache-key", queryParams{
		IncludeUpcoming: true,
		limit:           2,
	})
	want := "cache-key-true-ID-1"
	got := cacheKeyFunc("1")

	if got != want {
		t.Errorf("got: %s wanted: %s", got, want)
	}
}

func TestTimePointers(t *testing.T) {
	t.Parallel()

	type opts struct{ Time *time.Time }
	cache := sturdyc.New[any](100, 1, time.Hour, 5,
		sturdyc.WithNoContinuousEvictions(),
	)

	now := time.Now().Truncate(time.Hour)
	got := cache.PermutatedKey("key", opts{Time: &now})
	want := "key-" + strconv.Itoa(int(now.Unix()))
	if got != want {
		t.Errorf("got: %s wanted: %s", got, want)
	}

	got = cache.PermutatedKey("key", opts{Time: nil})
	want = "key-nil"
	if got != want {
		t.Errorf("got: %s wanted: %s", got, want)
	}

	empty := time.Time{}
	got = cache.PermutatedKey("key", opts{Time: &empty})
	want = "key-empty-time"
	if got != want {
		t.Errorf("got: %s wanted: %s", got, want)
	}
}
