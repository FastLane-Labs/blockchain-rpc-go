package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

// These deliberately use the original positional literal and function types.
// Adding config fields or making existing constructors variadic breaks clients.
var (
	_                                                                       = MultiRpcClientConfig{nil, false, 0, 0, nil}
	_ func(*MultiRpcClientConfig) (*MultiRpcClient, error)                  = DialMulti
	_ func(context.Context, *MultiRpcClientConfig) (*MultiRpcClient, error) = DialMultiContext
)

func waitUntil(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("condition did not become true before deadline")
		}
	}
}

func TestParallelCallCancelsLoser(t *testing.T) {
	ctx := testContext(t)
	started := make(chan struct{})
	canceled := make(chan struct{})
	c := testMultiClient(t,
		func(ctx context.Context, _ testRequest) (any, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		},
		func(context.Context, testRequest) (any, error) {
			<-started
			return "winner", nil
		},
	)
	c.allClients[0].rpcClient.sem = semaphore.NewWeighted(1)
	var result string
	if err := c.CallContext(ctx, &result, "eth_call"); err != nil {
		t.Fatal(err)
	}
	receive(t, ctx, canceled)
	waitUntil(t, ctx, func() bool { return c.allClients[0].rpcClient.queued.Load() == 0 })
	if !c.allClients[0].rpcClient.sem.TryAcquire(1) {
		t.Fatal("losing call retained its concurrency slot")
	}
	c.allClients[0].rpcClient.sem.Release(1)
}

func TestParallelCallCancelsQueuedCopy(t *testing.T) {
	ctx := testContext(t)
	allowWinner := make(chan struct{})
	defer close(allowWinner)
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) {
			t.Error("queued copy should have been canceled before sending")
			return "unexpected", nil
		},
		func(context.Context, testRequest) (any, error) {
			<-allowWinner
			return "winner", nil
		},
	)
	slow := c.allClients[0].rpcClient
	slow.sem = semaphore.NewWeighted(1)
	if err := slow.sem.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	defer slow.sem.Release(1)
	returned := make(chan error, 1)
	go func() { returned <- c.CallContext(ctx, nil, "eth_call") }()
	waitUntil(t, ctx, func() bool { return slow.queued.Load() == 1 })
	allowWinner <- struct{}{}
	if err := receive(t, ctx, returned); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, ctx, func() bool { return slow.queued.Load() == 0 })
}

func TestParallelBatchCancellation(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(fmt.Sprintf("success=%v", success), func(t *testing.T) {
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			started := make(chan struct{})
			canceled := make(chan struct{})
			c := testMultiClient(t,
				func(ctx context.Context, _ testRequest) (any, error) {
					close(started)
					<-ctx.Done()
					close(canceled)
					return nil, ctx.Err()
				},
				func(context.Context, testRequest) (any, error) {
					<-started
					if !success {
						cancel()
						return nil, errors.New("failed")
					}
					return "winner", nil
				},
			)
			var result string
			b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
			err := c.BatchCallContext(ctx, b)
			if success {
				if err != nil || b[0].Error != nil || result != "winner" {
					t.Fatalf("unexpected result: %q, errors: %v %v", result, err, b[0].Error)
				}
			} else if !errors.Is(err, context.Canceled) || !errors.Is(b[0].Error, context.Canceled) {
				t.Fatalf("cancellation missing from batch/element errors: %v %v", err, b[0].Error)
			}
			receive(t, testContext(t), canceled)
		})
	}
}

func TestRateLimitCancellationDoesNotLeakQueueCount(t *testing.T) {
	for _, limited := range []string{"rate", "concurrency"} {
		t.Run(limited, func(t *testing.T) {
			c := &RpcClient{}
			if limited == "rate" {
				c.lim = rate.NewLimiter(1, 1)
				c.lim.Allow()
			} else {
				c.sem = semaphore.NewWeighted(1)
				if err := c.sem.Acquire(context.Background(), 1); err != nil {
					t.Fatal(err)
				}
				defer c.sem.Release(1)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := c.applyRateLimit(ctx, false); err == nil {
				t.Fatal("expected cancellation")
			}
			if count := c.queued.Load(); count != 0 {
				t.Fatalf("leaked %d queued requests", count)
			}
		})
	}
}

type initializedResult struct {
	prefix string
	value  string
}

func (r *initializedResult) UnmarshalJSON(data []byte) error {
	if r.prefix == "" {
		return errors.New("decoder state was lost")
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	r.value = r.prefix + value
	return nil
}

func TestParallelInitializedResults(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			handler := func(context.Context, testRequest) (any, error) { return "value", nil }
			c := testMultiClient(t, handler, handler)
			result := initializedResult{prefix: "existing-"}
			if batch {
				b := []gethrpc.BatchElem{{Method: "test_value", Result: &result}}
				if err := c.BatchCall(b); err != nil || b[0].Error != nil {
					t.Fatalf("unexpected errors: %v %v", err, b[0].Error)
				}
			} else if err := c.Call(&result, "test_value"); err != nil {
				t.Fatal(err)
			}
			if result.value != "existing-value" {
				t.Fatalf("initialized receiver lost: %+v", result)
			}
		})
	}
	handler := func(context.Context, testRequest) (any, error) { return map[string]int{"new": 2}, nil }
	c := testMultiClient(t, handler, handler)
	result := map[string]int{"kept": 1}
	if err := c.Call(&result, "test_value"); err != nil || result["kept"] != 1 || result["new"] != 2 {
		t.Fatalf("map decode semantics changed: %v, error: %v", result, err)
	}
}

func TestParallelMalformedResultDoesNotWin(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return 42, nil },
				func(context.Context, testRequest) (any, error) { return "valid", nil },
			)
			var result string
			if batch {
				b := []gethrpc.BatchElem{{Method: "test_value", Result: &result}}
				if err := c.BatchCall(b); err != nil || b[0].Error != nil {
					t.Fatalf("unexpected errors: %v %v", err, b[0].Error)
				}
			} else if err := c.Call(&result, "test_value"); err != nil {
				t.Fatal(err)
			}
			if result != "valid" {
				t.Fatalf("invalid response won: %q", result)
			}
		})
	}
}

func TestParallelInvalidResultDoesNotPanic(t *testing.T) {
	handler := func(context.Context, testRequest) (any, error) { return "value", nil }
	c := testMultiClient(t, handler, handler)
	var typedNil *string
	for _, result := range []any{typedNil, "not a pointer"} {
		if err := c.Call(result, "test_value"); err == nil {
			t.Fatalf("expected error for result %T", result)
		}
	}
}

func TestParallelErrorsPreserveTypes(t *testing.T) {
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) { return nil, errors.New("execution reverted") },
		func(context.Context, testRequest) (any, error) { return nil, errors.New("upstream failed") },
	)
	err := c.Call(nil, "eth_call")
	var rpcErr gethrpc.Error
	if !errors.As(err, &rpcErr) || rpcErr.ErrorCode() != -32000 {
		t.Fatalf("RPC error type was lost: %v", err)
	}
	for _, id := range []string{"provider-0", "provider-1"} {
		if !strings.Contains(err.Error(), id) {
			t.Fatalf("missing provider %s: %v", id, err)
		}
	}
}

type revertError struct{}

func (revertError) Error() string  { return "execution reverted" }
func (revertError) ErrorData() any { return "0x12345678" }

func TestParallelErrorsPreserveRevertData(t *testing.T) {
	handler := func(context.Context, testRequest) (any, error) { return nil, revertError{} }
	c := testMultiClient(t, handler, handler)
	var dataErr gethrpc.DataError
	if err := c.Call(nil, "eth_call"); !errors.As(err, &dataErr) || dataErr.ErrorData() != "0x12345678" {
		t.Fatalf("revert data was lost: %v", err)
	}
}

type mutableArgument struct {
	value string
	calls int
}

func (a *mutableArgument) MarshalJSON() ([]byte, error) {
	a.calls++
	return json.Marshal(a.value)
}

func TestParallelBroadcastSnapshotsArguments(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			ctx := testContext(t)
			sent := make(chan string, 1)
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return "ok", nil },
				func(_ context.Context, req testRequest) (any, error) {
					sent <- req.Params[0]
					return "ok", nil
				},
			)
			blocked := semaphore.NewWeighted(1)
			if err := blocked.Acquire(ctx, 1); err != nil {
				t.Fatal(err)
			}
			held := true
			defer func() {
				if held {
					blocked.Release(1)
				}
			}()
			c.allClients[1].rpcClient.sem = blocked
			arg := &mutableArgument{value: "original"}
			if batch {
				var result string
				b := []gethrpc.BatchElem{{Method: "eth_sendRawTransaction", Args: []any{arg}, Result: &result}}
				if err := c.BatchCallContext(ctx, b); err != nil || b[0].Error != nil {
					t.Fatalf("unexpected errors: %v %v", err, b[0].Error)
				}
			} else if err := c.CallContext(ctx, nil, "eth_sendRawTransaction", arg); err != nil {
				t.Fatal(err)
			}
			arg.value = "changed after return"
			if arg.calls != 1 {
				t.Fatalf("user marshaller called %d times, want 1", arg.calls)
			}
			blocked.Release(1)
			held = false
			if got := receive(t, ctx, sent); got != "original" {
				t.Fatalf("late provider read caller-owned input: %q", got)
			}
			// Ensure the worker has released its slot before cleanup.
			if err := blocked.Acquire(ctx, 1); err != nil {
				t.Fatal(err)
			}
			held = true
		})
	}
}
