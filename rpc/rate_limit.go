package rpc

import (
	"context"
)

// The returned done function must be called to release resources.
func (c *RpcClient) applyRateLimit(ctx context.Context) (done func(), err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := c.advanceLimiter(ctx); err != nil {
		return nil, err
	}

	if err := c.acquireSemaphore(ctx, 1); err != nil {
		return nil, err
	}

	return func() {
		c.releaseSemaphore(1)
	}, nil
}

func (c *RpcClient) advanceLimiter(ctx context.Context) error {
	if c.lim == nil {
		return nil
	}

	if err := c.lim.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func (c *RpcClient) acquireSemaphore(ctx context.Context, weight int64) error {
	if c.sem == nil {
		return nil
	}

	return c.sem.Acquire(ctx, weight)
}

func (c *RpcClient) releaseSemaphore(weight int64) {
	if c.sem == nil {
		return
	}

	c.sem.Release(weight)
}
