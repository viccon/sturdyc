package sturdyc

import (
	"context"
	"errors"
	"fmt"
)

// cacheOp describes what a completed fetch should do to the cache.
type cacheOp int

const (
	opNone cacheOp = iota
	opWrite
	opWriteMissing
	opDelete
)

// fetchKind is the kind of request that started a fetch.
type fetchKind uint8

const (
	// kindSingle is a fetch started by GetOrFetch or Passthrough.
	kindSingle fetchKind = iota
	// kindBatch is a fetch started by GetOrFetchBatch or PassthroughBatch.
	kindBatch
	// kindRefresh is a background refresh of one or more keys.
	kindRefresh
)

// call is shared by every key that one invocation of a fetch function is
// responsible for. A single fetch has one key, a batch fetch has many.
type call struct {
	done chan struct{}
	// kind is the kind of request that started the fetch. A request that joins
	// a fetch of another kind is waiting for a different fetch function than
	// its own.
	kind fetchKind
	// leaderCancelled is set before done is closed when the fetch failed after
	// the context of the caller that started it had been cancelled. Waiters
	// whose own context is still alive will retry the fetch rather than
	// returning an error that belongs to another caller.
	leaderCancelled bool
}

func newCall(kind fetchKind) *call {
	return &call{done: make(chan struct{}), kind: kind}
}

// completedCall is used for flights that hold an entry that was already in the
// cache. It's never modified.
//
//nolint:gochecknoglobals // It's immutable, and it saves an allocation per flight.
var completedCall = func() *call {
	c := newCall(kindSingle)
	close(c.done)
	return c
}()

// ownerKey marks a context as the one that was passed to the fetch function
// of a call.
type ownerKey struct{ c *call }

// withOwner returns the context that is passed to the fetch function of the
// call. If the fetch function requests one of the call's own keys with it, or
// with a context derived from it, the request can tell that it would be
// waiting for itself.
func withOwner(ctx context.Context, c *call) context.Context {
	return context.WithValue(ctx, ownerKey{c}, true)
}

// ownedBy reports whether the context was passed to the fetch function of the
// call, or derived from a context that was.
func ownedBy(ctx context.Context, c *call) bool {
	return ctx.Value(ownerKey{c}) != nil
}

// orBackground replaces a nil context with context.Background. The fetch
// functions have always been called with whatever context the user passed in,
// and a nil context used to work as long as the fetch function didn't use it.
func orBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// fetchOpts controls how a fetch interacts with the cache.
type fetchOpts struct {
	// deleteNotFound removes keys that the fetch function reports as not
	// found from the cache, unless missing record storage is enabled, in
	// which case they're stored as missing records instead.
	deleteNotFound bool
	// reuseFresh returns an entry that was written after the caller looked
	// up the key, instead of fetching it again.
	reuseFresh bool
}

//nolint:gochecknoglobals // These are immutable presets.
var (
	getOrFetchOpts  = fetchOpts{deleteNotFound: true, reuseFresh: true}
	passthroughOpts = fetchOpts{}
	refreshOpts     = fetchOpts{deleteNotFound: true}
)

// flight holds the result of an in-flight fetch for a single cache key. The
// result fields are written by the leader before call.done is closed.
type flight[T any] struct {
	call *call
	// val is the value that GetOrFetch would return for this key.
	val T
	// found reports whether the fetch returned a value for the key.
	found bool
	// err is the error that GetOrFetch would return for this key.
	err error
	// invalidated is guarded by the lock of the shard that owns the key. It's
	// set when the key is written or deleted while the fetch is in flight,
	// which means the result may be older than the cache and won't be stored.
	invalidated bool
}

// wait blocks until the flight completes or the context is done. It returns
// retry=true if the fetch failed because the leader's context was cancelled
// while the waiter's own context is still alive.
func (f *flight[T]) wait(ctx context.Context) (retry bool, err error) {
	// Prefer the result if it's already available, even if the context is done.
	select {
	case <-f.call.done:
		return f.call.leaderCancelled && ctx.Err() == nil, nil
	default:
	}

	select {
	case <-f.call.done:
		return f.call.leaderCancelled && ctx.Err() == nil, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// failedElsewhere reports whether a request of the given kind should fetch the
// key with its own fetch function after the flight it joined failed. Flights
// of another kind use another fetch function, and it could fail where ours
// succeeds. A flight of the same kind fails the same way it always has.
func (f *flight[T]) failedElsewhere(kind fetchKind) bool {
	return f.call.kind != kind && isHardErr(f.err)
}

// isAbsent reports whether an error means that the record doesn't exist.
func isAbsent(err error) bool {
	return errors.Is(err, ErrNotFound) || errors.Is(err, ErrMissingRecord)
}

// isHardErr reports whether an error means that the fetch failed, rather than
// that the record doesn't exist, or that it was only available from the
// distributed storage.
func isHardErr(err error) bool {
	return err != nil && !isAbsent(err) && !errors.Is(err, ErrOnlyCachedRecords)
}

func panicErr(r any) error {
	return fmt.Errorf("sturdyc: panic recovered: %v", r)
}

// fetchOne calls the fetch function, recovering from panics.
func fetchOne[T any](ctx context.Context, fn FetchFn[T]) (value T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicErr(r)
		}
	}()
	return fn(ctx)
}

// fetchMany calls the batch fetch function, recovering from panics.
func fetchMany[T any](ctx context.Context, fn BatchFetchFn[T], ids []string) (values map[string]T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicErr(r)
		}
	}()
	return fn(ctx, ids)
}

// runFetch is called by the leader of a single-key flight. It calls the fetch
// function, stores the result in the flight, commits it to the cache, and then
// releases any waiters. If deleteNotFound is true, an ErrNotFound from the
// fetch function removes the key from the cache.
func (c *Client[T]) runFetch(ctx context.Context, key string, f *flight[T], fn FetchFn[T], deleteNotFound bool) {
	shard := c.shardFor(key)
	// The waiters have to be released even if something panics after the
	// fetch function has returned, or they'd be waiting forever.
	defer func() {
		if r := recover(); r != nil {
			f.err = panicErr(r)
			shard.abandon(key, f)
		}
		close(f.call.done)
	}()

	value, err := fetchOne(withOwner(ctx, f.call), fn)
	f.val = value

	op := opNone
	switch {
	case err == nil:
		f.found = true
		op = opWrite
	case c.storeMissingRecords && errors.Is(err, ErrNotFound):
		err = ErrMissingRecord
		op = opWriteMissing
	case errors.Is(err, errOnlyDistributedRecords):
		// We were able to get a record from the distributed storage, but the call to
		// the underlying data source failed. We'll cache the distributed record.
		f.found = true
		err = onlyCachedRecords(err)
		op = opWrite
	case errors.Is(err, ErrNotFound) && deleteNotFound:
		op = opDelete
	}
	f.err = err
	f.call.leaderCancelled = err != nil && ctx.Err() != nil

	shard.commit(key, f, op, value)
}

// callAndCache fetches a single key, or waits for a fetch that is already in
// flight for it. That fetch could have been started by a single fetch, a batch
// fetch, or a refresh.
func callAndCache[T any](ctx context.Context, c *Client[T], key string, fn FetchFn[T], opts fetchOpts) (T, error) {
	ctx = orBackground(ctx)
	shard := c.shardFor(key)
	retriedFailure := false
	for {
		f, leader := shard.joinOrLead(key, nil, kindSingle, opts.reuseFresh)
		if !leader && ownedBy(ctx, f.call) {
			// We were called by the fetch function of the flight that we were about
			// to join, so waiting for it would deadlock. We'll fetch the key
			// ourselves, without registering another flight for it.
			f, leader = &flight[T]{call: newCall(kindSingle)}, true
		}
		if leader {
			c.runFetch(ctx, key, f, fn, opts.deleteNotFound)
			return f.val, f.err
		}

		retry, err := f.wait(ctx)
		if err != nil {
			var zero T
			return zero, err
		}
		if retry {
			continue
		}
		if !retriedFailure && f.failedElsewhere(kindSingle) {
			retriedFailure = true
			continue
		}
		return f.val, f.err
	}
}

// runBatchFetch is called by the leader of a batch of flights. The ids, keys
// and flights slices are aligned by index, and every flight belongs to bc.
func (c *Client[T]) runBatchFetch(ctx context.Context, ids, keys []string, flights []*flight[T], fn BatchFetchFn[T], keyFn KeyFn, deleteNotFound bool, bc *call) {
	// committed is the number of flights that have their result. If something
	// panics, the remaining flights fail with the panic. The waiters have to
	// be released either way, or they'd be waiting forever.
	committed := 0
	defer func() {
		if r := recover(); r != nil {
			err := panicErr(r)
			if committed == len(flights) {
				// Every caller got its result, so nobody would see the error.
				c.log.Error(err.Error())
			}
			for i := committed; i < len(flights); i++ {
				flights[i].err = err
				c.shardFor(keys[i]).abandon(keys[i], flights[i])
			}
		}
		close(bc.done)
	}()

	response, err := fetchMany(withOwner(ctx, bc), fn, ids)
	hardErr := err != nil && !errors.Is(err, errOnlyDistributedRecords)
	callErr := err
	if err != nil && !hardErr {
		callErr = onlyCachedRecords(err)
	}

	requested := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		requested[id] = struct{}{}
		f := flights[i]
		value, ok := response[id]

		op := opNone
		switch {
		case ok:
			// We never want to discard values from the fetch functions, even if they
			// return an error. Instead, we'll pass them to the user along with any
			// errors and let them decide what to do. However, we'll only write them
			// to the cache if the fetch function didn't fail.
			f.val, f.found, f.err = value, true, callErr
			if !hardErr {
				op = opWrite
			}
		case err != nil:
			// We don't know if the record exists, so we'll leave the cache as is.
			f.err = callErr
		case c.storeMissingRecords:
			f.err = ErrMissingRecord
			op = opWriteMissing
		default:
			f.err = ErrNotFound
			if deleteNotFound {
				op = opDelete
			}
		}
		c.shardFor(keys[i]).commit(keys[i], f, op, value)
		committed++
	}
	bc.leaderCancelled = err != nil && ctx.Err() != nil

	// Records that weren't requested aren't part of any flight, but we've
	// always cached them, and there's no reason to throw them away.
	if !hardErr {
		for id, value := range response {
			if _, ok := requested[id]; !ok {
				key := keyFn(id)
				c.shardFor(key).store(key, value)
			}
		}
	}
}

type callBatchOpts[T any] struct {
	fetchOpts
	ids   []string
	keyFn KeyFn
	fn    BatchFetchFn[T]
}

type pendingFlight[T any] struct {
	id string
	f  *flight[T]
}

// callAndCacheBatch fetches the IDs that aren't already in flight, and waits
// for the ones that are. Flights started by single fetches, batch fetches and
// refreshes are all shared.
func callAndCacheBatch[T any](ctx context.Context, c *Client[T], opts callBatchOpts[T]) (map[string]T, error) {
	ctx = orBackground(ctx)
	response := make(map[string]T, len(opts.ids))
	var softErr error
	// retriedFailures holds the IDs that we've already fetched ourselves after
	// the flight that we joined for them failed.
	var retriedFailures map[string]struct{}
	pending := opts.ids

	for len(pending) > 0 {
		// The keys are computed before any flights are registered, so that a key
		// function that panics can't leave flights behind that nobody completes.
		keys := make([]string, len(pending))
		for i, id := range pending {
			keys[i] = opts.keyFn(id)
		}

		bc := newCall(kindBatch)
		var leadIDs, leadKeys []string
		var leadFlights []*flight[T]
		flights := make([]pendingFlight[T], 0, len(pending))
		for i, id := range pending {
			f, leader := c.shardFor(keys[i]).joinOrLead(keys[i], bc, kindBatch, opts.reuseFresh)
			if !leader && ownedBy(ctx, f.call) {
				// We were called by the fetch function of the flight that we were
				// about to join, so waiting for it would deadlock. We'll fetch the ID
				// ourselves, without registering another flight for it.
				f, leader = &flight[T]{call: bc}, true
			}
			if leader {
				leadIDs = append(leadIDs, id)
				leadKeys = append(leadKeys, keys[i])
				leadFlights = append(leadFlights, f)
			}
			flights = append(flights, pendingFlight[T]{id: id, f: f})
		}

		// We'll fetch our own IDs first. The flights we've joined are
		// making progress in the meantime.
		if len(leadIDs) > 0 {
			c.runBatchFetch(ctx, leadIDs, leadKeys, leadFlights, opts.fn, opts.keyFn, opts.deleteNotFound, bc)
		}

		var hardErr error
		var retry, failedElsewhere []string
		for _, p := range flights {
			shouldRetry, err := p.f.wait(ctx)
			if err != nil {
				return response, err
			}
			if !shouldRetry && p.f.failedElsewhere(kindBatch) {
				if _, retried := retriedFailures[p.id]; !retried {
					failedElsewhere = append(failedElsewhere, p.id)
					shouldRetry = true
				}
			}
			if shouldRetry {
				retry = append(retry, p.id)
				continue
			}

			if p.f.found {
				response[p.id] = p.f.val
			}

			switch {
			case p.f.err == nil:
			case errors.Is(p.f.err, ErrOnlyCachedRecords):
				// This handles the scenario where we're using the distributed storage
				// option and are able to get some records while the request to the
				// underlying data source fails. We'll continue to accumulate partial
				// responses as long as the only issue is cached-only records.
				softErr = p.f.err
			case isAbsent(p.f.err):
				// Batch responses never contain records that don't exist.
			case hardErr == nil:
				hardErr = p.f.err
			}
		}

		if hardErr != nil {
			return response, hardErr
		}

		// The IDs are marked after the loop, as the same ID can appear more than
		// once, and every occurrence has to be retried.
		for _, id := range failedElsewhere {
			if retriedFailures == nil {
				retriedFailures = make(map[string]struct{}, len(failedElsewhere))
			}
			retriedFailures[id] = struct{}{}
		}
		pending = retry
	}

	return response, softErr
}
