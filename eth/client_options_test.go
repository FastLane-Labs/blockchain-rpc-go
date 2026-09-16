package eth

import (
	"context"

	brpc "github.com/FastLane-Labs/blockchain-rpc-go/rpc"
)

var (
	_ func(*brpc.MultiRpcClientConfig) (*EthClient, error)                  = DialMulti
	_ func(context.Context, *brpc.MultiRpcClientConfig) (*EthClient, error) = DialMultiContext
)
