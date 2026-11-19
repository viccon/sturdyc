package sturdyc

import (
	"context"
)

// scheduleRefresh runs a background refresh, unless the client has been
// closed, or the maximum number of concurrent background refreshes has been
// reached. A refresh that is skipped will be attempted again the next time
// the key is requested after the retry delay has passed.
func (c *Client[T]) scheduleRefresh(fn func(ctx context.Context)) {
	if c.refreshCtx.Err() != nil {
		return
	}

	if c.refreshSlots != nil {
		select {
		case c.refreshSlots <- struct{}{}:
		default:
			return
		}
	}

	c.safeGo(func() {
		if c.refreshSlots != nil {
			defer func() { <-c.refreshSlots }()
		}
		ctx, cancel := context.WithTimeout(c.refreshCtx, c.refreshTimeout)
		defer cancel()
		fn(ctx)
	})
}

// scheduleBatchRefresh schedules a background refresh for a batch of IDs.
func (c *Client[T]) scheduleBatchRefresh(ids []string, keyFn KeyFn, fetchFn BatchFetchFn[T]) {
	c.scheduleRefresh(func(ctx context.Context) {
		c.refreshBatch(ctx, ids, keyFn, fetchFn)
	})
}

// refresh fetches a key in the background, unless a fetch for it is already in flight.
func (c *Client[T]) refresh(ctx context.Context, key string, fetchFn FetchFn[T]) {
	f, leader := c.shardFor(key).joinOrLead(key, nil, refreshOpts.reuseFresh)
	if !leader {
		return
	}
	c.runFetch(ctx, key, f, fetchFn, refreshOpts.deleteNotFound)
}

// refreshBatch fetches the IDs in the background, skipping the ones that are already in flight.
func (c *Client[T]) refreshBatch(ctx context.Context, ids []string, keyFn KeyFn, fetchFn BatchFetchFn[T]) {
	bc := newCall()
	leadIDs := make([]string, 0, len(ids))
	leadKeys := make([]string, 0, len(ids))
	leadFlights := make([]*flight[T], 0, len(ids))
	for _, id := range ids {
		key := keyFn(id)
		if f, leader := c.shardFor(key).joinOrLead(key, bc, refreshOpts.reuseFresh); leader {
			leadIDs = append(leadIDs, id)
			leadKeys = append(leadKeys, key)
			leadFlights = append(leadFlights, f)
		}
	}

	if len(leadIDs) == 0 {
		return
	}

	c.reportBatchRefreshSize(len(leadIDs))
	c.runBatchFetch(ctx, leadIDs, leadKeys, leadFlights, fetchFn, keyFn, refreshOpts.deleteNotFound, bc)
}
