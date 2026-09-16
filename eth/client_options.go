package eth

import (
	"context"

	brpc "github.com/FastLane-Labs/blockchain-rpc-go/rpc"
)

// DialMultiWithOptions creates a client with explicit opt-in RPC behavior.
func DialMultiWithOptions(cfg *brpc.MultiRpcClientConfig, opts brpc.MultiRpcClientOptions) (*EthClient, error) {
	return DialMultiContextWithOptions(context.Background(), cfg, opts)
}

// DialMultiContextWithOptions creates a client with explicit opt-in RPC behavior.
func DialMultiContextWithOptions(ctx context.Context, cfg *brpc.MultiRpcClientConfig, opts brpc.MultiRpcClientOptions) (*EthClient, error) {
	c, err := brpc.DialMultiContextWithOptions(ctx, cfg, opts)
	if err != nil {
		return nil, err
	}
	return NewClient(c), nil
}
