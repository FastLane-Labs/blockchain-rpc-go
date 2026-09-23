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
	// ParallelCalls sends allowlisted reads to all healthy read providers at once.
	// Prefer the highest weight; move lower only after all higher attempts time
	// out. Equal weights race for the first non-timeout result or error. A read
	// batch selects one complete response. Losing copies are canceled.
	// HTTP preference does not restrict dispatch; provider limits still apply.
	// Other methods and mixed batches keep their original routing. Single raw
	// transaction calls retain the existing first-success broadcast policy.
	// The zero value preserves default selection. See PARALLEL_CALLS.md.
	ParallelCalls bool

	// ParallelCallTimeout caps each parallel read attempt, including queueing,
	// even with one eligible provider. Zero defaults to 2 seconds; negatives are
	// invalid. Attempts share an absolute deadline and get no extra window on
	// fallback. The caller's deadline takes precedence and should exceed this
	// cap to leave time to select a buffered fallback. Ignored for other methods.
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
