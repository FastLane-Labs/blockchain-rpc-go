package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Append into caller-provided storage so small pools do not allocate a candidate
// slice. Snapshot health once: recounting it after launching calls could deadlock
// the receiver if a health check changes a provider's status in between.
func (c *MultiRpcClient) parallelClients(dst []*internalRpcClient, subscriptionRelated, broadcast bool) []*internalRpcClient {
	for _, client := range c.allClients {
		if client.rpcClient.sendOnly {
			if broadcast {
				dst = append(dst, client)
			}
			continue
		}
		if client.enabled.Load() && (!subscriptionRelated || client.rpcClient.SupportsSubscriptions()) {
			dst = append(dst, client)
		}
	}
	return dst
}

// Encode arguments once, before returning ownership to the caller. Queued or
// slow copies must not marshal caller-owned maps/slices after the race returns.
func parallelArgs(args []any) ([]any, error) {
	if len(args) == 0 {
		return args, nil
	}
	encoded := make([]any, len(args))
	for i, arg := range args {
		switch arg.(type) {
		case nil, bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr, float32, float64:
			// Plain scalar values cannot change after copying the interface.
			encoded[i] = arg
			continue
		}
		raw, err := json.Marshal(arg)
		if err != nil {
			return nil, err
		}
		encoded[i] = json.RawMessage(raw)
	}
	return encoded, nil
}

type parallelResponse struct {
	client *internalRpcClient
	raw    json.RawMessage
	err    error
}

func (c *MultiRpcClient) callContextFirstResponse(ctx context.Context, result any, method string, args ...any) error {
	var storage [8]*internalRpcClient
	broadcast := method == "eth_sendRawTransaction"
	clients := c.parallelClients(storage[:0], strings.HasSuffix(method, subscribeMethodSuffix), broadcast)
	if len(clients) == 0 {
		return ErrNoAvailableClients
	}
	if len(clients) == 1 {
		// No extra copying, serialization, goroutine or channel is needed.
		return clients[0].rpcClient.CallContext(ctx, result, method, args...)
	}
	if result != nil {
		v := reflect.ValueOf(result)
		if v.Kind() != reflect.Pointer || v.IsNil() {
			return &json.InvalidUnmarshalError{Type: v.Type()}
		}
	}
	encodedArgs, err := parallelArgs(args)
	if err != nil {
		return err
	}
	requestCtx := ctx
	if !broadcast {
		var cancel context.CancelFunc
		requestCtx, cancel = context.WithCancel(ctx)
		defer cancel()
	}
	responses := make(chan parallelResponse, len(clients))
	done := requestCtx.Done()
	needsResult := result != nil
	for _, client := range clients {
		go func(client *internalRpcClient, ctx context.Context) {
			res := parallelResponse{client: client}
			select {
			case <-done:
				return
			default:
			}
			if needsResult {
				var raw json.RawMessage
				res.err = client.rpcClient.CallContext(ctx, &raw, method, encodedArgs...)
				res.raw = raw
			} else {
				res.err = client.rpcClient.CallContext(ctx, nil, method, encodedArgs...)
			}
			responses <- res
		}(client, requestCtx)
	}
	var failures []parallelResponse
	for range clients {
		select {
		case res := <-responses:
			select {
			case <-done:
				return parallelErrors(method, failures, requestCtx.Err())
			default:
			}
			// Broadcasts keep the default first-success semantics: a provider
			// rejecting a transaction (already known, nonce too low) must not end
			// the race while another provider may still accept it.
			if res.err != nil && (broadcast || isParallelTimeout(res.err)) {
				failures = append(failures, res)
				continue
			}
			if res.err != nil || !needsResult {
				return res.err
			}
			// Choose the response before decoding, so only one provider can
			// ever write to the caller's receiver, even when decoding fails.
			return json.Unmarshal(res.raw, result)
		case <-done:
			return parallelErrors(method, failures, requestCtx.Err())
		}
	}
	return parallelErrors(method, failures, nil)
}

// Format exhausted failures: every copy timed out, or every provider rejected
// a broadcast. Accepted answers are returned unchanged, preserving their public
// interfaces and error data.
func parallelErrors(method string, failures []parallelResponse, cause error) error {
	errs := make([]error, 0, len(failures)+1)
	for _, res := range failures {
		errs = append(errs, fmt.Errorf("%s: %w", res.client.rpcClient.id, res.err))
	}
	if cause != nil {
		errs = append(errs, cause)
	}
	return fmt.Errorf("RPC %s failed: %w", method, errors.Join(errs...))
}
