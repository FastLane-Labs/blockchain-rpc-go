package rpc

import (
	"context"
	"errors"
)

const (
	defaultSemaphoreWeight = 1
)

var (
	ErrRateLimitExceeded      = errors.New("rate limit exceeded")
	ErrMaxConcurrencyExceeded = errors.New("max concurrency exceeded")
)

// Returns whether current rate limits allow a request to be made now.
func (c *RpcClient) canMakeRequestNow() bool {
	if c.lim != nil && c.lim.Tokens() < 1 {
		return false
	}

	if c.sem != nil {
		if !c.sem.TryAcquire(defaultSemaphoreWeight) {
			return false
		}

		c.sem.Release(defaultSemaphoreWeight)
	}

	return true
}

// The returned done function must be called to release resources.
func (c *RpcClient) applyRateLimit(ctx context.Context, nonBlocking bool) (done func(), err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := c.advanceLimiter(ctx, nonBlocking); err != nil {
		return nil, err
	}

	if err := c.acquireSemaphore(ctx, defaultSemaphoreWeight, nonBlocking); err != nil {
		return nil, err
	}

	return func() {
		c.releaseSemaphore(defaultSemaphoreWeight)
	}, nil
}

func (c *RpcClient) advanceLimiter(ctx context.Context, nonBlocking bool) error {
	if c.lim == nil {
		return nil
	}

	if nonBlocking {
		if c.lim.Allow() {
			return nil
		}

		return ErrRateLimitExceeded
	}

	if err := c.lim.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func (c *RpcClient) acquireSemaphore(ctx context.Context, weight int64, nonBlocking bool) error {
	if c.sem == nil {
		return nil
	}

	if nonBlocking {
		if c.sem.TryAcquire(weight) {
			return nil
		}

		return ErrMaxConcurrencyExceeded
	}

	return c.sem.Acquire(ctx, weight)
}

func (c *RpcClient) releaseSemaphore(weight int64) {
	if c.sem == nil {
		return
	}

	c.sem.Release(weight)
}
