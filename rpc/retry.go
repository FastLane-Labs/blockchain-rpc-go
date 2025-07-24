package rpc

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/rpc"
)

func isErrorRetryable(err error) bool {
	// No more time to retry
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}

	// RPC error, *should* be consistent across clients, not retryable
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		return false
	}

	// All other errors (http, network, etc.) are retryable
	return true
}
