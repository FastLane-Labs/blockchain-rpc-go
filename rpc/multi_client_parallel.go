package rpc

import (
	"context"
	"errors"
	"fmt"
)

type parallelResponse[T any] struct {
	client *internalRpcClient
	value  T
	err    error
}

// Dispatch each eligible provider once, then consume responses by weight.
// A single eligible provider uses the same deadline and error handling.
func parallelCall[T any](ctx context.Context, c *MultiRpcClient, method string, attempt func(context.Context, *RpcClient) (T, error)) (T, error) {
	var zero T
	clients := c.eligibleClients(false, false)
	if len(clients) == 0 {
		return zero, ErrNoAvailableClients
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errParallelSuperseded)
	deadline := c.parallelDeadline()
	groups := groupParallelClients[parallelResponse[T]](clients)
	for _, group := range groups {
		for _, client := range group.clients {
			go func() {
				attemptCtx, done := context.WithDeadline(ctx, deadline)
				defer done()
				res := parallelResponse[T]{client: client, err: attemptCtx.Err()}
				if res.err == nil {
					res.value, res.err = attempt(attemptCtx, client.rpcClient)
				}
				res.err = parallelAttemptError(attemptCtx, res.err)
				group.responses <- res
			}()
		}
	}
	var failures []error
	for _, group := range groups {
		for range group.clients {
			select {
			case res := <-group.responses:
				if err := ctx.Err(); err != nil {
					return zero, parallelErrors(method, failures, err)
				}
				if !isParallelTimeout(res.err) {
					return res.value, res.err
				}
				failures = append(failures, fmt.Errorf("%s: %w", res.client.rpcClient.id, res.err))
			case <-ctx.Done():
				return zero, parallelErrors(method, failures, ctx.Err())
			}
		}
	}
	return zero, parallelErrors(method, failures, nil)
}

func parallelErrors(method string, failures []error, cause error) error {
	if cause != nil {
		failures = append(failures, cause)
	}
	return fmt.Errorf("RPC %s failed: %w", method, errors.Join(failures...))
}
