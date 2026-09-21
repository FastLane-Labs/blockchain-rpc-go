package rpc

import "context"

// MultiRpcClientOptions contains opt-in behavior without changing the layout of
// MultiRpcClientConfig or the signatures of existing constructors.
type MultiRpcClientOptions struct {
	// ParallelCalls races calls and batches across all healthy, capable providers.
	// The first answer wins, whether a result or an error of any kind, except a
	// timeout (context.DeadlineExceeded), which waits for another copy. All-timeout
	// failures are aggregated. Results are decoded once.
	// Ordinary losing copies are canceled; eth_sendRawTransaction and batches
	// containing it continue under the caller's context. Like the default
	// broadcast, eth_sendRawTransaction calls return the first success and only
	// fail, with every provider's error, once all providers have failed.
	// Weights and HTTP preference do not restrict the race. Provider limits still
	// apply, and fan-out increases their load. Subscriptions and notifications
	// retain their existing behavior. The zero value preserves default selection.
	ParallelCalls bool
}

// DialMultiWithOptions creates a client with explicit opt-in behavior.
func DialMultiWithOptions(cfg *MultiRpcClientConfig, opts MultiRpcClientOptions) (*MultiRpcClient, error) {
	return DialMultiContextWithOptions(context.Background(), cfg, opts)
}

// DialMultiContextWithOptions creates a client with explicit opt-in behavior.
func DialMultiContextWithOptions(ctx context.Context, cfg *MultiRpcClientConfig, opts MultiRpcClientOptions) (*MultiRpcClient, error) {
	c, err := DialMultiContext(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c.parallelCalls = opts.ParallelCalls
	return c, nil
}
