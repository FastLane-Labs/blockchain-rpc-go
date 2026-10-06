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
nextGroup:
	for _, group := range groups {
		for range group.clients {
			var res parallelResponse[T]
			select {
			case res = <-group.responses:
			case <-ctx.Done():
				if ctx.Err() != context.DeadlineExceeded {
					return zero, parallelErrors(method, failures, ctx.Err())
				}
				// At the caller's deadline, consider only responses already
				// buffered, preserving weight order without waiting for workers.
				select {
				case res = <-group.responses:
				default:
					continue nextGroup
				}
			}
			if err := ctx.Err(); err != nil && err != context.DeadlineExceeded {
				return zero, parallelErrors(method, failures, err)
			}
			if !isParallelTimeout(res.err) {
				return res.value, res.err
			}
			failures = append(failures, fmt.Errorf("%s: %w", res.client.rpcClient.id, res.err))
		}
	}
	return zero, parallelErrors(method, failures, ctx.Err())
}

func parallelErrors(method string, failures []error, cause error) error {
	if cause != nil {
		failures = append(failures, cause)
	}
	return fmt.Errorf("RPC %s failed: %w", method, errors.Join(failures...))
}
