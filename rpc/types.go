package rpc

import (
	"context"

	"github.com/ethereum/go-ethereum/rpc"
)

type RpcClientData struct {
	// Id is useful to identify the client in metrics
	Id string

	// Http or websocket endpoint
	Url string

	// RateLimit is the maximum number of requests per second to the RPC (0 = no limit)
	RateLimit uint64

	// MaxConcurrency is the maximum number of concurrent requests to the RPC (0 = no limit)
	MaxConcurrency uint64

	// Used in MultiRpcClient only. Higher weight means higher priority in the client selection
	Weight uint64
}

type IRpcClient interface {
	BatchCall(b []rpc.BatchElem) error
	BatchCallContext(ctx context.Context, b []rpc.BatchElem) error
	Call(result any, method string, args ...any) error
	CallContext(ctx context.Context, result any, method string, args ...any) error
	Close()
	EthSubscribe(ctx context.Context, channel any, args ...any) (*rpc.ClientSubscription, error)
	Notify(ctx context.Context, method string, args ...any) error
	RegisterName(name string, receiver any) error
	SetHeader(key string, value string)
	ShhSubscribe(ctx context.Context, channel any, args ...any) (*rpc.ClientSubscription, error)
	Subscribe(ctx context.Context, namespace string, channel any, args ...any) (*rpc.ClientSubscription, error)
	SupportedModules() (map[string]string, error)
	SupportsSubscriptions() bool
}
