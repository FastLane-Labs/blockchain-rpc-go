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
				rpcError := map[string]any{"code": -32000, "message": err.Error()}
				if coded, ok := err.(gethrpc.Error); ok {
					rpcError["code"] = coded.ErrorCode()
				}
				if dataErr, ok := err.(gethrpc.DataError); ok {
					rpcError["data"] = dataErr.ErrorData()
				}
				responses[i].Error = rpcError
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
	for _, tc := range []struct {
		name, method string
		parallel     bool
	}{
		{"ordinary", "eth_call", true},
		{"default_broadcast", "eth_sendRawTransaction", false},
		{"parallel_broadcast", "eth_sendRawTransaction", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			method := tc.method
			ctx := testContext(t)
			started := make(chan struct{}, 3)
			release := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
			slowDone := make(chan struct{})
			var handlers []testHandler
			for i := range release {
				handlers = append(handlers, func(ctx context.Context, req testRequest) (any, error) {
					if req.Method != method || len(req.Params) != 1 || req.Params[0] != "payload" {
						t.Errorf("unexpected request: %+v", req)
					}
					started <- struct{}{}
					<-release[i]
					if i == 0 {
						return nil, errors.New("execution reverted")
					}
					if i == 2 {
						if method == "eth_sendRawTransaction" && ctx.Err() != nil {
							t.Error("losing broadcast was canceled")
						}
						close(slowDone)
					}
					return fmt.Sprintf("result-%d", i), nil
				})
			}
			c := testMultiClient(t, handlers...)
			// Transaction broadcasting must remain enabled with the flag off.
			c.parallelCalls = tc.parallel
			if tc.parallel && method == "eth_sendRawTransaction" {
				// Send-only providers participate even without read health checks.
				c.allClients[2].rpcClient.sendOnly = true
				c.allClients[2].setEnabled(false)
			}
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
			if method == "eth_sendRawTransaction" {
				// Broadcasts ignore RPC errors until success in both modes.
				close(release[0])
			}
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

func TestParallelCallAllTimeouts(t *testing.T) {
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) { return nil, errors.New("execution reverted") },
		func(context.Context, testRequest) (any, error) { return nil, errors.New("upstream unavailable") },
	)
	for i := range c.allClients {
		setTestTransport(t, c, i, failingTransport{})
	}
	result := "unchanged"
	err := c.Call(&result, "eth_call")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("aggregated error lost its cause: %v", err)
	}
	for _, want := range []string{"provider-0: ", "provider-1: "} {
		if !strings.Contains(err.Error(), want) {
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
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("missing context cancellation in %v", err)
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

func TestParallelBatchFirstResponse(t *testing.T) {
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
	release <- struct{}{}
	if err := receive(t, ctx, returned); err != nil {
		t.Fatal(err)
	}
	// Either released provider can finish first, but its RPC error is final too.
	if !((first == "first" && b[0].Error == nil && b[1].Error != nil) ||
		(second == "second" && b[1].Error == nil && b[0].Error != nil)) {
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
	if _, ok := b[0].Error.(gethrpc.Error); !ok {
		t.Fatalf("expected the first RPC error unchanged, got %T: %v", b[0].Error, b[0].Error)
	}
	if result != "unchanged" {
		t.Fatalf("failed batch modified result: %q", result)
	}
}

func TestParallelBatchTimeouts(t *testing.T) {
	for _, allTimeouts := range []bool{false, true} {
		t.Run(fmt.Sprintf("allTimeouts=%v", allTimeouts), func(t *testing.T) {
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return nil, errors.New("execution reverted") },
				func(context.Context, testRequest) (any, error) { return nil, errors.New("execution reverted") },
			)
			// Exercise both a mixed timeout/RPC error race and all-timeout failures.
			for i := range c.allClients {
				if allTimeouts || i == 0 {
					setTestTransport(t, c, i, failingTransport{})
				}
			}
			var result string
			b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
			err := c.BatchCall(b)
			if (err != nil) != allTimeouts {
				t.Fatalf("unexpected batch timeout error: %v", err)
			}
			if !allTimeouts {
				if _, ok := b[0].Error.(gethrpc.Error); !ok || b[0].Error.Error() != "execution reverted" {
					t.Fatalf("RPC error should finish the element unchanged: %v", b[0].Error)
				}
				return
			}
			for _, id := range []string{"provider-0", "provider-1"} {
				if b[0].Error == nil || !strings.Contains(b[0].Error.Error(), id+": ") {
					t.Fatalf("missing %s in %v", id, b[0].Error)
				}
				if !strings.Contains(err.Error(), id+": ") {
					t.Fatalf("missing %s in timeout error %v", id, err)
				}
			}
		})
	}
}

// A provider copy that times out on its own, before the caller's deadline.
type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, context.DeadlineExceeded
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
	for _, tc := range []struct {
		name              string
		options, parallel bool
	}{
		{"default", false, false},
		{"zero_options", true, false},
		{"parallel", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls [2]atomic.Int32
			var arrivals [2]atomic.Int32
			ready := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			var data []*RpcClientData
			for i := range calls {
				url := testServer(t, func(ctx context.Context, _ testRequest) (any, error) {
					count := calls[i].Add(1)
					if tc.parallel {
						// Both copies must start before either can cancel the other.
						if arrivals[count-1].Add(1) == 2 {
							close(ready[count-1])
						}
						select {
						case <-ready[count-1]:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return nil, fmt.Errorf("provider-%d failed", i)
				})
				data = append(data, &RpcClientData{Id: fmt.Sprintf("provider-%d", i), Url: url})
			}
			cfg := &MultiRpcClientConfig{RpcData: data, HealthCheckInterval: time.Hour}
			var c *MultiRpcClient
			var err error
			if tc.options {
				c, err = DialMultiContextWithOptions(testContext(t), cfg, MultiRpcClientOptions{ParallelCalls: tc.parallel})
			} else {
				c, err = DialMultiContext(testContext(t), cfg)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)
			if err := c.CallContext(testContext(t), nil, "eth_call"); err == nil {
				t.Fatal("expected call failure")
			}
			var result string
			b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
			if err := c.BatchCallContext(testContext(t), b); err != nil || b[0].Error == nil {
				t.Fatalf("unexpected batch errors: %v %v", err, b[0].Error)
			}
			wantSecond := int32(0)
			if tc.parallel {
				wantSecond = 2
			}
			if calls[0].Load() != 2 || calls[1].Load() != wantSecond {
				t.Fatalf("provider calls: %d %d, want 2 %d", calls[0].Load(), calls[1].Load(), wantSecond)
			}
		})
	}
}
