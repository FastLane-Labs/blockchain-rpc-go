package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

type testRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params []string        `json:"params"`
}

type testResponse struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   any             `json:"error,omitempty"`
}

type testHandler func(context.Context, testRequest) (any, error)

func testServer(t *testing.T, handler testHandler) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
			return
		}
		var requests []testRequest
		batch := len(raw) > 0 && raw[0] == '['
		if batch {
			if err := json.Unmarshal(raw, &requests); err != nil {
				t.Error(err)
				return
			}
		} else {
			requests = make([]testRequest, 1)
			if err := json.Unmarshal(raw, &requests[0]); err != nil {
				t.Error(err)
				return
			}
		}
		responses := make([]testResponse, len(requests))
		for i, req := range requests {
			var value any = "0x1"
			var err error
			if req.Method != "eth_chainId" {
				value, err = handler(r.Context(), req)
			}
			responses[i] = testResponse{Version: "2.0", ID: req.ID, Result: value}
			if err != nil {
				responses[i].Result = nil
				responses[i].Error = map[string]any{"code": -32000, "message": err.Error()}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if batch {
			_ = json.NewEncoder(w).Encode(responses)
		} else {
			_ = json.NewEncoder(w).Encode(responses[0])
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func receive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("timed out waiting for RPC activity")
		var zero T
		return zero
	}
}

func testMultiClient(t *testing.T, handlers ...testHandler) *MultiRpcClient {
	t.Helper()
	c := &MultiRpcClient{parallelCalls: true, stopped: make(chan struct{})}
	for i, handler := range handlers {
		client, err := DialContext(testContext(t), &RpcClientConfig{RpcClientData: RpcClientData{
			Id: fmt.Sprintf("provider-%d", i), Url: testServer(t, handler),
		}})
		if err != nil {
			t.Fatal(err)
		}
		ic := &internalRpcClient{rpcClient: client}
		ic.setEnabled(true)
		c.allClients = append(c.allClients, ic)
	}
	t.Cleanup(c.Close)
	return c
}

func TestParallelCallFirstSuccess(t *testing.T) {
	for _, method := range []string{"eth_call", "eth_sendRawTransaction"} {
		t.Run(method, func(t *testing.T) {
			ctx := testContext(t)
			started := make(chan struct{}, 3)
			release := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
			slowDone := make(chan struct{})
			var handlers []testHandler
			for i := range release {
				handlers = append(handlers, func(_ context.Context, req testRequest) (any, error) {
					if req.Method != method || len(req.Params) != 1 || req.Params[0] != "payload" {
						t.Errorf("unexpected request: %+v", req)
					}
					started <- struct{}{}
					<-release[i]
					if i == 0 {
						return nil, errors.New("execution reverted")
					}
					if i == 2 {
						close(slowDone)
					}
					return fmt.Sprintf("result-%d", i), nil
				})
			}
			c := testMultiClient(t, handlers...)
			// Transaction broadcasting must remain enabled with the flag off.
			c.parallelCalls = method != "eth_sendRawTransaction"
			defer func() {
				for _, ch := range release {
					select {
					case <-ch:
					default:
						close(ch)
					}
				}
			}()
			var result string
			returned := make(chan error, 1)
			go func() { returned <- c.CallContext(ctx, &result, method, "payload") }()
			for range release {
				receive(t, ctx, started)
			}
			close(release[0])
			close(release[1])
			if err := receive(t, ctx, returned); err != nil {
				t.Fatal(err)
			}
			if result != "result-1" {
				t.Fatalf("got %q, want first successful result", result)
			}
			close(release[2])
			receive(t, ctx, slowDone)
			if result != "result-1" {
				t.Fatalf("late response overwrote result: %q", result)
			}
		})
	}
}

func TestParallelCallAllErrors(t *testing.T) {
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) { return nil, errors.New("execution reverted") },
		func(context.Context, testRequest) (any, error) { return nil, errors.New("upstream unavailable") },
	)
	result := "unchanged"
	err := c.Call(&result, "eth_call")
	for _, want := range []string{"provider-0: execution reverted", "provider-1: upstream unavailable"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("missing %q in %v", want, err)
		}
	}
	if result != "unchanged" {
		t.Fatalf("failed calls modified result: %q", result)
	}
}

func TestParallelCallCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext(t))
	started := make(chan struct{}, 2)
	handler := func(ctx context.Context, _ testRequest) (any, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c := testMultiClient(t, handler, handler)
	returned := make(chan error, 1)
	go func() { returned <- c.CallContext(ctx, nil, "eth_call") }()
	for range 2 {
		receive(t, ctx, started)
	}
	cancel()
	err := receive(t, testContext(t), returned)
	for _, id := range []string{"provider-0", "provider-1"} {
		if err == nil || !strings.Contains(err.Error(), id+": ") || strings.Count(err.Error(), "context canceled") != 2 {
			t.Fatalf("missing cancellation from %s in %v", id, err)
		}
	}
}

func TestParallelCallEligibleProviders(t *testing.T) {
	var calls atomic.Int32
	handler := func(context.Context, testRequest) (any, error) {
		calls.Add(1)
		return "ok", nil
	}
	c := testMultiClient(t, handler, handler, handler)
	c.allClients[1].setEnabled(false)
	c.allClients[2].rpcClient.sendOnly = true
	if err := c.Call(nil, "eth_call"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("called %d providers, want only the healthy read provider", calls.Load())
	}
	c.allClients[0].setEnabled(false)
	if err := c.Call(nil, "eth_call"); !errors.Is(err, ErrNoAvailableClients) {
		t.Fatalf("got %v, want no available clients", err)
	}
}

func TestParallelBatchFirstSuccessPerElement(t *testing.T) {
	ctx := testContext(t)
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	slowRelease := make(chan struct{})
	defer close(slowRelease)
	defer close(release)
	var handlers []testHandler
	for i := range 3 {
		handlers = append(handlers, func(_ context.Context, req testRequest) (any, error) {
			if req.Params[0] == "first" {
				started <- struct{}{}
				if i == 2 {
					<-slowRelease
				} else {
					<-release
				}
			}
			if (i == 0 && req.Params[0] == "first") || (i == 1 && req.Params[0] == "second") {
				return req.Params[0], nil
			}
			return nil, errors.New("execution reverted")
		})
	}
	c := testMultiClient(t, handlers...)
	var first, second string
	b := []gethrpc.BatchElem{
		{Method: "eth_call", Args: []any{"first"}, Result: &first},
		{Method: "eth_call", Args: []any{"second"}, Result: &second},
	}
	returned := make(chan error, 1)
	go func() { returned <- c.BatchCallContext(ctx, b) }()
	for range 3 {
		receive(t, ctx, started)
	}
	for range 2 {
		release <- struct{}{}
	}
	if err := receive(t, ctx, returned); err != nil {
		t.Fatal(err)
	}
	if first != "first" || second != "second" || b[0].Error != nil || b[1].Error != nil {
		t.Fatalf("unexpected batch results: %q %q, errors: %v %v", first, second, b[0].Error, b[1].Error)
	}
}

func TestParallelBatchAllErrors(t *testing.T) {
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) { return nil, errors.New("execution reverted") },
		func(context.Context, testRequest) (any, error) { return nil, errors.New("upstream unavailable") },
	)
	result := "unchanged"
	b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
	if err := c.BatchCall(b); err != nil {
		t.Fatalf("RPC errors must be returned per element: %v", err)
	}
	for _, want := range []string{"provider-0: execution reverted", "provider-1: upstream unavailable"} {
		if b[0].Error == nil || !strings.Contains(b[0].Error.Error(), want) {
			t.Fatalf("missing %q in %v", want, b[0].Error)
		}
	}
	if result != "unchanged" {
		t.Fatalf("failed batch modified result: %q", result)
	}
}

func TestParallelBatchTransportErrors(t *testing.T) {
	for _, allTransportErrors := range []bool{false, true} {
		t.Run(fmt.Sprintf("allTransportErrors=%v", allTransportErrors), func(t *testing.T) {
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return nil, errors.New("execution reverted") },
				func(context.Context, testRequest) (any, error) { return nil, errors.New("execution reverted") },
			)
			// Exercise both mixed RPC/transport errors and all-transport failures.
			for i, p := range c.allClients {
				if allTransportErrors || i == 0 {
					client, err := gethrpc.DialOptions(testContext(t), "http://unavailable", gethrpc.WithHTTPClient(&http.Client{
						Transport: failingTransport{},
					}))
					if err != nil {
						t.Fatal(err)
					}
					p.rpcClient.c.Close()
					p.rpcClient.c = client
				}
			}
			var result string
			b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
			err := c.BatchCall(b)
			if (err != nil) != allTransportErrors {
				t.Fatalf("unexpected batch transport error: %v", err)
			}
			for _, id := range []string{"provider-0", "provider-1"} {
				if b[0].Error == nil || !strings.Contains(b[0].Error.Error(), id+": ") {
					t.Fatalf("missing %s in %v", id, b[0].Error)
				}
				if allTransportErrors && !strings.Contains(err.Error(), id+": ") {
					t.Fatalf("missing %s in transport error %v", id, err)
				}
			}
		})
	}
}

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("connection refused")
}

func TestParallelSupportedModules(t *testing.T) {
	ctx := testContext(t)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	handler := func(_ context.Context, req testRequest) (any, error) {
		if req.Method != "rpc_modules" {
			t.Errorf("unexpected method: %s", req.Method)
		}
		started <- struct{}{}
		<-release
		return map[string]string{"eth": "1.0"}, nil
	}
	c := testMultiClient(t, handler, handler)
	returned := make(chan error, 1)
	go func() {
		modules, err := c.SupportedModules()
		if err == nil && modules["eth"] != "1.0" {
			err = fmt.Errorf("unexpected modules: %v", modules)
		}
		returned <- err
	}()
	for range 2 {
		receive(t, ctx, started)
	}
	release <- struct{}{}
	if err := receive(t, ctx, returned); err != nil {
		t.Fatal(err)
	}
}

func TestParallelCallsOptIn(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", parallel), func(t *testing.T) {
			var calls [2]atomic.Int32
			var data []*RpcClientData
			for i := range calls {
				url := testServer(t, func(context.Context, testRequest) (any, error) {
					calls[i].Add(1)
					return nil, fmt.Errorf("provider-%d failed", i)
				})
				data = append(data, &RpcClientData{Id: fmt.Sprintf("provider-%d", i), Url: url})
			}
			cfg := &MultiRpcClientConfig{RpcData: data, HealthCheckInterval: time.Hour}
			if parallel {
				cfg.ParallelCalls = true
			}
			c, err := DialMultiContext(testContext(t), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)
			if err := c.Call(nil, "eth_call"); err == nil {
				t.Fatal("expected call failure")
			}
			var result string
			b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
			if err := c.BatchCall(b); err != nil || b[0].Error == nil {
				t.Fatalf("unexpected batch errors: %v %v", err, b[0].Error)
			}
			wantSecond := int32(0)
			if parallel {
				wantSecond = 2
			}
			if calls[0].Load() != 2 || calls[1].Load() != wantSecond {
				t.Fatalf("provider calls: %d %d, want 2 %d", calls[0].Load(), calls[1].Load(), wantSecond)
			}
		})
	}
}
