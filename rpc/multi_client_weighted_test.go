package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/semaphore"
)

type weightedTestResult struct {
	value string
	err   error
}

func callWeightedTest(ctx context.Context, c *MultiRpcClient, batch bool) weightedTestResult {
	var result weightedTestResult
	if batch {
		b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result.value}}
		result.err = c.BatchCallContext(ctx, b)
		if result.err == nil {
			result.err = b[0].Error
		}
	} else {
		result.err = c.CallContext(ctx, &result.value, "eth_call")
	}
	return result
}

func TestParallelPrefersWeightOverArrival(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, preferredError := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%v/error=%v", batch, preferredError), func(t *testing.T) {
				ctx := testContext(t)
				release := make(chan struct{})
				defer close(release)
				started := make(chan struct{}, 3)
				c := testMultiClient(t,
					func(ctx context.Context, _ testRequest) (any, error) {
						started <- struct{}{}
						select {
						case <-release:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
						if preferredError {
							return nil, rpcOutcomeError{3, "execution reverted"}
						}
						return "preferred", nil
					},
					func(context.Context, testRequest) (any, error) { started <- struct{}{}; return "second", nil },
					func(context.Context, testRequest) (any, error) { started <- struct{}{}; return "third", nil },
				)
				for i, weight := range []uint64{100, 50, 10} {
					c.allClients[i].rpcClient.weight = weight
				}
				returned := make(chan weightedTestResult, 1)
				go func() { returned <- callWeightedTest(ctx, c, batch) }()
				for range 3 {
					receive(t, ctx, started)
				}
				select {
				case got := <-returned:
					t.Fatalf("lower weight answered while preferred provider was pending: %+v", got)
				case <-time.After(50 * time.Millisecond):
				}
				release <- struct{}{}
				got := receive(t, ctx, returned)
				if preferredError {
					coded, ok := got.err.(gethrpc.Error)
					data, dataOK := got.err.(gethrpc.DataError)
					if !ok || !dataOK || coded.ErrorCode() != 3 || data.ErrorData() != "0x12345678" || got.value != "" {
						t.Fatalf("preferred RPC error/data lost: %+v", got)
					}
				} else if got.err != nil || got.value != "preferred" {
					t.Fatalf("wrong provider selected: %+v", got)
				}
			})
		}
	}
}

func TestParallelBufferedFallbacksFollowWeights(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, timeouts := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("batch=%v/timeouts=%d", batch, timeouts), func(t *testing.T) {
				ctx := testContext(t)
				var handlers []testHandler
				var dispatches [3]atomic.Int32
				for i := range 3 {
					handlers = append(handlers, func(ctx context.Context, _ testRequest) (any, error) {
						dispatches[i].Add(1)
						if i < timeouts {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						// The lowest weight answers first, but must not beat a higher
						// weight's response buffered before the preferred timeout.
						if i == 1 {
							time.Sleep(10 * time.Millisecond)
						}
						return fmt.Sprintf("provider-%d", i), nil
					})
				}
				c := testMultiClient(t, handlers...)
				c.parallelCallTimeout = 500 * time.Millisecond
				for i, weight := range []uint64{100, 50, 10} {
					c.allClients[i].rpcClient.weight = weight
				}
				got := callWeightedTest(ctx, c, batch)
				if ctx.Err() != nil {
					t.Fatal("provider timeout exhausted the caller context")
				}
				if timeouts < 3 {
					if got.err != nil || got.value != fmt.Sprintf("provider-%d", timeouts) {
						t.Fatalf("buffered fallback lost or selected out of order: %+v", got)
					}
				} else {
					if !errors.Is(got.err, context.DeadlineExceeded) || got.value != "" {
						t.Fatalf("expected aggregated timeout: %+v", got)
					}
					for i := range 3 {
						if !strings.Contains(got.err.Error(), fmt.Sprintf("provider-%d:", i)) {
							t.Fatalf("provider missing from error: %v", got.err)
						}
					}
				}
				for i, client := range c.allClients {
					if n := dispatches[i].Load(); n != 1 {
						t.Fatalf("provider %d dispatched %d times", i, n)
					}
					waitUntil(t, ctx, func() bool { return client.rpcClient.queued.Load() == 0 })
				}
			})
		}
	}
}

func TestParallelWaitsForEntireHigherWeightGroup(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			ctx := testContext(t)
			release := make(chan struct{})
			defer close(release)
			started := make(chan struct{}, 2)
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return nil, nil },
				func(ctx context.Context, _ testRequest) (any, error) {
					started <- struct{}{}
					select {
					case <-release:
						return "equal-high-weight", nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
				func(context.Context, testRequest) (any, error) { started <- struct{}{}; return "low-weight", nil },
			)
			for i, weight := range []uint64{100, 100, 10} {
				c.allClients[i].rpcClient.weight = weight
			}
			setTestTransport(t, c, 0, failingTransport{})
			returned := make(chan weightedTestResult, 1)
			go func() { returned <- callWeightedTest(ctx, c, batch) }()
			for range 2 {
				receive(t, ctx, started)
			}
			select {
			case got := <-returned:
				t.Fatalf("selected lower weight while an equal-high-weight attempt was pending: %+v", got)
			case <-time.After(50 * time.Millisecond):
			}
			release <- struct{}{}
			if got := receive(t, ctx, returned); got.err != nil || got.value != "equal-high-weight" {
				t.Fatalf("wrong result: %+v", got)
			}
		})
	}
}

func TestParallelCallerDeadlineDiscardsBufferedFallback(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			c := testMultiClient(t,
				func(ctx context.Context, _ testRequest) (any, error) { <-ctx.Done(); return nil, ctx.Err() },
				func(context.Context, testRequest) (any, error) { return "buffered", nil },
			)
			c.allClients[0].rpcClient.weight = 100
			c.parallelCallTimeout = time.Second
			ctx, cancel := context.WithTimeout(testContext(t), 100*time.Millisecond)
			defer cancel()
			if got := callWeightedTest(ctx, c, batch); !errors.Is(got.err, context.DeadlineExceeded) || got.value != "" {
				t.Fatalf("caller deadline must end selection: %+v", got)
			}
		})
	}
}

func TestParallelPreferredQueueTimeout(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			ctx := testContext(t)
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) {
					t.Error("queued request reached provider")
					return nil, nil
				},
				func(context.Context, testRequest) (any, error) { return "fallback", nil },
			)
			preferred := c.allClients[0].rpcClient
			preferred.weight = 100
			preferred.sem = semaphore.NewWeighted(1)
			if err := preferred.sem.Acquire(ctx, 1); err != nil {
				t.Fatal(err)
			}
			defer preferred.sem.Release(1)
			c.parallelCallTimeout = 500 * time.Millisecond
			if got := callWeightedTest(ctx, c, batch); got.err != nil || got.value != "fallback" {
				t.Fatalf("queue timeout did not allow fallback: %+v", got)
			}
			waitUntil(t, ctx, func() bool { return preferred.queued.Load() == 0 })
		})
	}
}

func TestParallelAttemptDeadlines(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, timeout := range []time.Duration{0, 500 * time.Millisecond} {
			for _, count := range []int{1, 3} {
				t.Run(fmt.Sprintf("batch=%v/timeout=%v/providers=%d", batch, timeout, count), func(t *testing.T) {
					var handlers []testHandler
					for range count {
						handlers = append(handlers, func(context.Context, testRequest) (any, error) { return nil, nil })
					}
					c := testMultiClient(t, handlers...)
					c.parallelCallTimeout = timeout
					deadlines := make(chan time.Time, count)
					for i := range count {
						setTestTransport(t, c, i, transportFunc(func(r *http.Request) (*http.Response, error) {
							deadline, _ := r.Context().Deadline()
							deadlines <- deadline
							return nil, context.DeadlineExceeded
						}))
					}
					before := time.Now()
					got := callWeightedTest(testContext(t), c, batch)
					after := time.Now()
					if !errors.Is(got.err, context.DeadlineExceeded) {
						t.Fatalf("unexpected error: %v", got.err)
					}
					want := timeout
					if want == 0 {
						want = 2 * time.Second
					}
					var first time.Time
					for range count {
						deadline := receive(t, testContext(t), deadlines)
						if deadline.Before(before.Add(want)) || deadline.After(after.Add(want)) {
							t.Fatalf("attempt deadline %v is not dispatch time + %v", deadline, want)
						}
						if !first.IsZero() && !first.Equal(deadline) {
							t.Fatal("attempts received different timeout windows")
						}
						first = deadline
					}
				})
			}
		}
	}
}

func TestParallelWeightedOptionsAndFiltering(t *testing.T) {
	ctx := testContext(t)
	if _, err := DialMultiContextWithOptions(ctx, nil, MultiRpcClientOptions{ParallelCalls: true, ParallelCallTimeout: -1}); err == nil {
		t.Fatal("negative timeout accepted")
	}
	var data []*RpcClientData
	for _, weight := range []uint64{10, 200, 100, 50} {
		data = append(data, &RpcClientData{Id: fmt.Sprint(weight), Weight: weight, Url: testServer(t,
			func(context.Context, testRequest) (any, error) { return fmt.Sprint(weight), nil }), SendOnly: weight == 200})
	}
	c, err := DialMultiContextWithOptions(ctx, &MultiRpcClientConfig{RpcData: data, HealthCheckInterval: time.Hour},
		MultiRpcClientOptions{ParallelCalls: true, ParallelCallTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	for _, batch := range []bool{false, true} {
		if got := callWeightedTest(ctx, c, batch); got.err != nil || got.value != "100" {
			t.Fatalf("configured weights/send-only filtering ignored: %+v", got)
		}
	}
	// Check health filtering without a background health loop re-enabling the
	// provider while this test deliberately marks it unavailable.
	unhealthy := testMultiClient(t,
		func(context.Context, testRequest) (any, error) {
			t.Error("unhealthy provider called")
			return "100", nil
		},
		func(context.Context, testRequest) (any, error) { return "50", nil },
	)
	unhealthy.allClients[0].rpcClient.weight = 100
	unhealthy.allClients[1].rpcClient.weight = 50
	unhealthy.allClients[0].setEnabled(false)
	if got := callWeightedTest(ctx, unhealthy, false); got.err != nil || got.value != "50" {
		t.Fatalf("unhealthy provider was selected: %+v", got)
	}
}

func TestParallelWeightedNonTimeoutFailure(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			ready := make(chan struct{})
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return nil, nil },
				func(context.Context, testRequest) (any, error) { close(ready); return "lower-success", nil },
			)
			c.allClients[0].rpcClient.weight = 100
			setTestTransport(t, c, 0, transportFunc(func(r *http.Request) (*http.Response, error) {
				select {
				case <-ready:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
				return nil, io.EOF
			}))
			if got := callWeightedTest(testContext(t), c, batch); !errors.Is(got.err, io.EOF) || got.value != "" {
				t.Fatalf("non-timeout failure was overridden: %+v", got)
			}
		})
	}
}

func TestParallelFallbackErrorOverridesLowerSuccess(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return nil, nil },
				func(context.Context, testRequest) (any, error) { return nil, rpcOutcomeError{3, "execution reverted"} },
				func(context.Context, testRequest) (any, error) { return "lower-success", nil },
			)
			for i, weight := range []uint64{100, 50, 10} {
				c.allClients[i].rpcClient.weight = weight
			}
			setTestTransport(t, c, 0, failingTransport{})
			got := callWeightedTest(testContext(t), c, batch)
			coded, ok := got.err.(gethrpc.Error)
			if !ok || coded.ErrorCode() != 3 || got.value != "" {
				t.Fatalf("fallback error was replaced by a lower-weight success: %+v", got)
			}
		})
	}
}
