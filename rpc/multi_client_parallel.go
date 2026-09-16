package rpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ethereum/go-ethereum/rpc"
)

func (c *MultiRpcClient) batchCallContextParallel(ctx context.Context, b []rpc.BatchElem, subscriptionRelated bool) error {
	if len(b) == 0 {
		return nil
	}
	providers := make([]*internalRpcClient, 0, len(c.allClients))
	for _, client := range c.allClients {
		if client.enabled.Load() && !client.rpcClient.sendOnly && (!subscriptionRelated || client.rpcClient.SupportsSubscriptions()) {
			providers = append(providers, client)
		}
	}
	if len(providers) == 0 {
		return ErrNoAvailableClients
	}
	type response struct {
		id    string
		batch []rpc.BatchElem
		err   error
	}
	responses := make(chan response, len(providers))
	for _, p := range providers {
		// Clone before starting the goroutine, so it never reads caller-owned
		// batch elements after this method returns.
		batch := make([]rpc.BatchElem, len(b))
		for i, elem := range b {
			batch[i] = rpc.BatchElem{Method: elem.Method, Args: elem.Args}
			v := reflect.ValueOf(elem.Result)
			if v.IsValid() && v.Kind() == reflect.Pointer && !v.IsNil() {
				batch[i].Result = reflect.New(v.Type().Elem()).Interface()
			}
		}
		go func() {
			err := p.rpcClient.BatchCallContext(ctx, batch)
			responses <- response{p.rpcClient.id, batch, err}
		}()
	}
	completed := make([]bool, len(b))
	errs := make([][]error, len(b))
	var transportErrors []error
	remaining := len(b)
	for range providers {
		select {
		case res := <-responses:
			if res.err != nil {
				transportErrors = append(transportErrors, fmt.Errorf("%s: %w", res.id, res.err))
			}
			for i, elem := range res.batch {
				if completed[i] {
					continue
				}
				err := res.err
				if err == nil {
					err = elem.Error
				}
				if err != nil {
					errs[i] = append(errs[i], fmt.Errorf("%s: %w", res.id, err))
					continue
				}
				reflect.ValueOf(b[i].Result).Elem().Set(reflect.ValueOf(elem.Result).Elem())
				b[i].Error = nil
				completed[i] = true
				remaining--
			}
			if remaining == 0 {
				return nil
			}
		case <-ctx.Done():
			return errors.Join(append(transportErrors, ctx.Err())...)
		}
	}
	for i := range b {
		if !completed[i] {
			b[i].Error = errors.Join(errs[i]...)
		}
	}
	// Match geth: RPC errors belong to individual elements; transport errors
	// are returned from the batch itself only if no provider could respond.
	if len(transportErrors) == len(providers) {
		return errors.Join(transportErrors...)
	}
	return nil
}
