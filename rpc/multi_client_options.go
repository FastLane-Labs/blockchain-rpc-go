package rpc

import "context"

// MultiRpcClientOptions contains opt-in behavior without changing the layout of
// MultiRpcClientConfig or the signatures of existing constructors.
type MultiRpcClientOptions struct {
	// ParallelCalls races calls and batches across all healthy, capable providers.
	// It returns the first result or non-transport error per request. RPC error
	// codes/messages do not affect selection. If all copies fail at the transport
	// level, their errors are aggregated. Results are decoded only once.
	// Provider weights and HTTP preference do not restrict the race. Subscriptions
	// and notifications retain their existing behavior. The zero value is false.
	// Pending copies are canceled on an accepted response except eth_sendRawTransaction calls
	// and batches containing them, which continue to every eligible provider.
	// Fan-out increases provider load; each provider's existing limits still apply.
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
