package rpc

import (
	"context"
	"encoding/json"

	"github.com/ethereum/go-ethereum/rpc"
)

type parallelBatchResponse struct {
	client *internalRpcClient
	batch  []rpc.BatchElem
	raw    []json.RawMessage
	err    error
}

func (c *MultiRpcClient) batchCallContextParallel(ctx context.Context, b []rpc.BatchElem, subscriptionRelated bool) error {
	if len(b) == 0 {
		return nil
	}
	var storage [8]*internalRpcClient
	clients := c.parallelClients(storage[:0], subscriptionRelated, false)
	if len(clients) == 0 {
		return ErrNoAvailableClients
	}
	broadcast := false
	for _, elem := range b {
		broadcast = broadcast || elem.Method == "eth_sendRawTransaction"
	}
	if len(clients) == 1 {
		if !broadcast {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, c.parallelDeadline())
			defer cancel()
		}
		return clients[0].rpcClient.BatchCallContext(ctx, b)
	}
	// Snapshot metadata and encode inputs once. Workers never access b or any
	// mutable arguments after ownership has returned to the caller.
	template := make([]rpc.BatchElem, len(b))
	for i, elem := range b {
		args, err := parallelArgs(elem.Args)
		if err != nil {
			return err
		}
		template[i] = rpc.BatchElem{Method: elem.Method, Args: args}
	}
	requestCtx := ctx
	if !broadcast {
		var cancel context.CancelFunc
		requestCtx, cancel = context.WithCancel(ctx)
		defer cancel()
	}
	groups := groupParallelClients[parallelBatchResponse](clients, !broadcast)
	done := requestCtx.Done()
	deadline := c.parallelDeadline()
	for _, group := range groups {
		for _, client := range group.clients {
			go func(ctx context.Context) {
				if !broadcast {
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, deadline)
					defer cancel()
				}
				res := parallelBatchResponse{client: client}
				if res.err = ctx.Err(); res.err == nil {
					res.batch = make([]rpc.BatchElem, len(template))
					res.raw = make([]json.RawMessage, len(template))
					copy(res.batch, template)
					for i := range res.batch {
						res.batch[i].Result = &res.raw[i]
					}
					res.err = client.rpcClient.BatchCallContext(ctx, res.batch)
				}
				group.responses <- res
			}(requestCtx)
		}
	}
	var failures []parallelResponse
	for _, group := range groups {
		for range group.clients {
			select {
			case res := <-group.responses:
				if err := requestCtx.Err(); err != nil {
					return parallelBatchErrors(b, failures, err)
				}
				if !isParallelTimeout(res.err) {
					if res.err != nil {
						return res.err
					}
					// Select one provider's complete batch, preserving element errors.
					for i := range res.batch {
						b[i].Error = res.batch[i].Error
						if b[i].Error == nil {
							b[i].Error = json.Unmarshal(res.raw[i], b[i].Result)
						}
					}
					return nil
				}
				failures = append(failures, parallelResponse{client: res.client, err: res.err})
			case <-done:
				return parallelBatchErrors(b, failures, requestCtx.Err())
			}
		}
	}
	return parallelBatchErrors(b, failures, nil)
}

func parallelBatchErrors(b []rpc.BatchElem, failures []parallelResponse, cause error) error {
	err := parallelErrors("batch", failures, cause)
	for i := range b {
		b[i].Error = err
	}
	return err
}
