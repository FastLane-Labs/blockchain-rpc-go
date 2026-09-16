package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
	if len(clients) == 1 {
		client := clients[0].rpcClient
		if err := client.BatchCallContext(ctx, b); err != nil {
			if isParallelTransportError(err) {
				return fmt.Errorf("%s: %w", client.id, err)
			}
			return err
		}
		for i := range b {
			if isParallelTransportError(b[i].Error) {
				b[i].Error = fmt.Errorf("%s: %w", client.id, b[i].Error)
			}
		}
		return nil
	}
	// Snapshot metadata and encode inputs once. Workers never access b or any
	// mutable arguments after ownership has returned to the caller.
	template := make([]rpc.BatchElem, len(b))
	broadcast := false
	for i, elem := range b {
		args, err := parallelArgs(elem.Args)
		if err != nil {
			return err
		}
		template[i] = rpc.BatchElem{Method: elem.Method, Args: args}
		broadcast = broadcast || elem.Method == "eth_sendRawTransaction"
	}
	requestCtx := ctx
	if !broadcast {
		var cancel context.CancelFunc
		requestCtx, cancel = context.WithCancel(ctx)
		defer cancel()
	}
	responses := make(chan parallelBatchResponse, len(clients))
	done := requestCtx.Done()
	for _, client := range clients {
		go func(ctx context.Context) {
			select {
			case <-done:
				responses <- parallelBatchResponse{client: client, err: ctx.Err()}
				return
			default:
			}
			res := parallelBatchResponse{client: client, batch: make([]rpc.BatchElem, len(template)), raw: make([]json.RawMessage, len(template))}
			copy(res.batch, template)
			for i := range res.batch {
				res.batch[i].Result = &res.raw[i]
			}
			res.err = client.rpcClient.BatchCallContext(ctx, res.batch)
			responses <- res
		}(requestCtx)
	}
	completed := make([]bool, len(b))
	remaining := len(b)
	responded := false
	var failures []parallelBatchResponse
	for range clients {
		select {
		case res := <-responses:
			select {
			case <-done:
				setParallelBatchErrors(b, completed, failures, requestCtx.Err())
				return requestCtx.Err()
			default:
			}
			if res.err != nil && !isParallelTransportError(res.err) {
				return res.err
			}
			if res.err == nil {
				responded = true
				for i := range res.batch {
					if completed[i] || isParallelTransportError(res.batch[i].Error) {
						continue
					}
					b[i].Error = res.batch[i].Error
					if b[i].Error == nil {
						b[i].Error = json.Unmarshal(res.raw[i], b[i].Result)
					}
					completed[i] = true
					remaining--
				}
				if remaining == 0 {
					return nil
				}
			}
			failures = append(failures, res)
		case <-done:
			setParallelBatchErrors(b, completed, failures, requestCtx.Err())
			return requestCtx.Err()
		}
	}
	setParallelBatchErrors(b, completed, failures, nil)
	// Match geth: per-request errors are in BatchElem.Error; an overall error
	// indicates that no provider could return a batch response.
	if !responded {
		errs := make([]error, len(failures))
		for i, res := range failures {
			errs[i] = fmt.Errorf("%s: %w", res.client.rpcClient.id, res.err)
		}
		return errors.Join(errs...)
	}
	return nil
}

func setParallelBatchErrors(b []rpc.BatchElem, completed []bool, failures []parallelBatchResponse, cause error) {
	for i := range b {
		if completed[i] {
			continue
		}
		errs := make([]error, 0, len(failures)+1)
		for _, res := range failures {
			err := res.err
			if err == nil {
				err = res.batch[i].Error
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", res.client.rpcClient.id, err))
			}
		}
		if cause != nil {
			errs = append(errs, cause)
		}
		b[i].Error = errors.Join(errs...)
	}
}
