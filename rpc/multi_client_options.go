package rpc

import (
	"context"
	"errors"
	"time"
)

const defaultParallelCallTimeout = 2 * time.Second

// MultiRpcClientOptions contains opt-in behavior without changing the layout of
// MultiRpcClientConfig or the signatures of existing constructors.
type MultiRpcClientOptions struct {
	// ParallelCalls sends calls and batches to all healthy, capable providers at
	// once, preferring responses from the highest configured weight. Equal weights
	// race each other. A lower weight is considered only after every attempt at a
	// higher weight times out. All other results and errors are returned unchanged.
	// All-timeout failures are aggregated. Results are decoded once.
	// Ordinary losing copies are canceled; eth_sendRawTransaction and batches
	// containing it continue under the caller's context. Like the default
	// broadcast, eth_sendRawTransaction calls return the first success and only
	// fail, with every provider's error, once all providers have failed.
	// Batches containing transactions select the first non-timeout batch, without
	// weight preference. HTTP preference does not restrict concurrent calls.
	// Provider limits still apply, and fan-out increases their load. Subscriptions
	// and notifications retain their existing behavior. The zero value preserves
	// default selection.
	ParallelCalls bool

	// ParallelCallTimeout limits each ordinary parallel provider attempt, including
	// rate/concurrency queueing. Zero defaults to 2 seconds; negative values are
	// invalid. All attempts share an absolute deadline but have independent contexts.
	// The caller's deadline always takes precedence: allow more time than this
	// timeout to select a buffered fallback after a preferred attempt expires.
	// Ignored when ParallelCalls is false, for transaction broadcasts, and for
	// batches containing transactions.
	ParallelCallTimeout time.Duration
}

// DialMultiWithOptions creates a client with explicit opt-in behavior.
func DialMultiWithOptions(cfg *MultiRpcClientConfig, opts MultiRpcClientOptions) (*MultiRpcClient, error) {
	return DialMultiContextWithOptions(context.Background(), cfg, opts)
}

// DialMultiContextWithOptions creates a client with explicit opt-in behavior.
func DialMultiContextWithOptions(ctx context.Context, cfg *MultiRpcClientConfig, opts MultiRpcClientOptions) (*MultiRpcClient, error) {
	if opts.ParallelCallTimeout < 0 {
		return nil, errors.New("parallel call timeout must not be negative")
	}
	c, err := DialMultiContext(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c.parallelCalls = opts.ParallelCalls
	c.parallelCallTimeout = opts.ParallelCallTimeout
	return c, nil
}

func (c *MultiRpcClient) parallelDeadline() time.Time {
	timeout := c.parallelCallTimeout
	if timeout == 0 {
		timeout = defaultParallelCallTimeout
	}
	return time.Now().Add(timeout)
}
