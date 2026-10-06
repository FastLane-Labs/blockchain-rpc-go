package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/time/rate"
)

func TestParallelReadAllowlistCoversEthClient(t *testing.T) {
	files, err := filepath.Glob("../eth/*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("find eth source files: %v", err)
	}
	for _, filename := range files {
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			method, _ := strconv.Unquote(literal.Value)
			isMethod := strings.HasPrefix(method, "eth_") || strings.HasPrefix(method, "net_") ||
				strings.HasPrefix(method, "web3_") || strings.HasPrefix(method, "debug_")
			// Exempt writes/node-local methods here instead of allowlisting them.
			if isMethod && method != "eth_sendRawTransaction" && !parallelReadMethod(method) {
				t.Errorf("%s: %s is missing from the parallel read allowlist", filename, method)
			}
			return true
		})
	}
}

func TestEmptyBatchChecksProviderAvailability(t *testing.T) {
	c := testMultiClient(t, func(context.Context, testRequest) (any, error) {
		t.Error("unhealthy provider called")
		return nil, nil
	})
	c.allClients[0].setEnabled(false)
	for _, parallel := range []bool{false, true} {
		c.parallelCalls = parallel
		if err := c.BatchCall(nil); !errors.Is(err, ErrNoAvailableClients) {
			t.Fatalf("parallel=%v: empty batch reported success without a provider: %v", parallel, err)
		}
	}
}

func TestParallelStatefulMethodsKeepOriginalRouting(t *testing.T) {
	for _, method := range []string{"eth_newFilter", "eth_getFilterChanges", "eth_uninstallFilter", "custom_submitBundle", "eth_sendRawTransaction"} {
		for _, batch := range []bool{false, true} {
			if !batch && method == "eth_sendRawTransaction" {
				continue // Single transaction calls deliberately broadcast.
			}
			t.Run(fmt.Sprintf("%s/batch=%v", method, batch), func(t *testing.T) {
				var calls [3]atomic.Int32
				lowerStarted := make(chan struct{}, 4)
				var handlers []testHandler
				for i := range calls {
					handlers = append(handlers, func(_ context.Context, req testRequest) (any, error) {
						calls[i].Add(1)
						if i > 0 {
							lowerStarted <- struct{}{}
						} else {
							select {
							case <-lowerStarted:
							case <-time.After(50 * time.Millisecond):
							}
						}
						if req.Method == "eth_sendRawTransaction" {
							return nil, rpcOutcomeError{-32000, "nonce too low"}
						}
						return "value", nil
					})
				}
				c := testMultiClient(t, handlers...)
				c.allClients[2].rpcClient.sendOnly = true
				c.parallelCallTimeout = time.Nanosecond // Must not cap legacy routing.
				var result, read string
				wantCalls := int32(1)
				if batch {
					b := []gethrpc.BatchElem{{Method: "eth_call", Result: &read}, {Method: method, Result: &result}}
					if err := c.BatchCallContext(testContext(t), b); err != nil || b[0].Error != nil || read != "value" {
						t.Fatalf("batch routing failed: %q, %v, %v", read, err, b[0].Error)
					}
					if method == "eth_sendRawTransaction" && (b[1].Error == nil || b[1].Error.Error() != "nonce too low") {
						t.Fatalf("lost transaction rejection: %v", b[1].Error)
					}
					wantCalls = 2
				} else if err := c.CallContext(testContext(t), &result, method); err != nil {
					t.Fatal(err)
				}
				// All calls use the same provider. No background copies may submit
				// after the caller receives a rejection, including send-only copies.
				c.Close()
				if calls[0].Load() != wantCalls || calls[1].Load() != 0 || calls[2].Load() != 0 {
					t.Fatalf("unexpected dispatch counts: %d %d %d", calls[0].Load(), calls[1].Load(), calls[2].Load())
				}
			})
		}
	}
}

func TestParallelNilContextAndPoolSize(t *testing.T) {
	for _, count := range []int{1, 2} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("providers=%d/batch=%v", count, batch), func(t *testing.T) {
				var handlers []testHandler
				for range count {
					handlers = append(handlers, func(context.Context, testRequest) (any, error) { return "value", nil })
				}
				c := testMultiClient(t, handlers...)
				got := callWeightedTest(nil, c, batch)
				if got.err != nil || got.value != "value" {
					t.Fatalf("nil context: %+v", got)
				}
			})
		}
	}
}

func TestParallelInvalidReceiverNeverDispatches(t *testing.T) {
	for _, count := range []int{1, 2} {
		for _, method := range []string{"eth_call", "eth_sendRawTransaction"} {
			for _, parallel := range []bool{false, true} {
				if !parallel && method == "eth_call" {
					continue
				}
				t.Run(fmt.Sprintf("providers=%d/%s/parallel=%v", count, method, parallel), func(t *testing.T) {
					var handlers []testHandler
					for range count {
						handlers = append(handlers, func(context.Context, testRequest) (any, error) {
							t.Error("invalid receiver must be rejected before submitting")
							return "value", nil
						})
					}
					c := testMultiClient(t, handlers...)
					c.parallelCalls = parallel
					results := []any{"not a pointer"}
					if method == "eth_sendRawTransaction" {
						results = append(results, (*string)(nil))
					}
					for _, result := range results {
						var invalid *json.InvalidUnmarshalError
						if err := c.CallContext(testContext(t), result, method); !errors.As(err, &invalid) {
							t.Fatalf("want invalid receiver error, got %v", err)
						}
					}
				})
			}
		}
	}
}

func TestParallelSocketTimeoutAtEveryPoolSize(t *testing.T) {
	for _, count := range []int{1, 2} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("providers=%d/batch=%v", count, batch), func(t *testing.T) {
				var handlers []testHandler
				for range count {
					handlers = append(handlers, func(context.Context, testRequest) (any, error) { return nil, nil })
				}
				c := testMultiClient(t, handlers...)
				c.parallelCallTimeout = 100 * time.Millisecond
				cause := &net.DNSError{Err: "i/o timeout", IsTimeout: true}
				for i := range count {
					setTestTransport(t, c, i, transportFunc(func(r *http.Request) (*http.Response, error) {
						<-r.Context().Done()
						return nil, cause
					}))
				}
				got := callWeightedTest(testContext(t), c, batch)
				if !errors.Is(got.err, context.DeadlineExceeded) || !errors.Is(got.err, cause) {
					t.Fatalf("lost timeout classification or transport cause: %v", got.err)
				}
			})
		}
	}
}

// Each response selects its own hook, so private zero-valued broadcast receivers
// can exercise cancellation after successful transport but before worker return.
var decodeHooks sync.Map

type hookedResult string

func (r *hookedResult) UnmarshalJSON(data []byte) error {
	var key string
	if err := json.Unmarshal(data, &key); err != nil {
		return err
	}
	if hook, ok := decodeHooks.Load(key); ok {
		hook.(func())()
	}
	*r = hookedResult(key)
	return nil
}

func TestParallelBroadcastKeepsAcceptedResponseAfterCancel(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, count := range []int{1, 2} {
			t.Run(fmt.Sprintf("parallel=%v/providers=%d", parallel, count), func(t *testing.T) {
				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()
				decodeHooks.Store(t.Name(), func() { cancel() })
				defer decodeHooks.Delete(t.Name())
				handlers := []testHandler{func(context.Context, testRequest) (any, error) { return t.Name(), nil }}
				if count == 2 {
					handlers = append(handlers, func(ctx context.Context, _ testRequest) (any, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					})
				}
				c := testMultiClient(t, handlers...)
				c.parallelCalls = parallel
				var result hookedResult
				err := c.CallContext(ctx, &result, "eth_sendRawTransaction", "0xsigned")
				if ctx.Err() != context.Canceled || err != nil || string(result) != t.Name() {
					t.Fatalf("accepted response discarded: ctx=%v result=%q err=%v", ctx.Err(), result, err)
				}
			})
		}
	}
}

func TestParallelCancelsBeforeDecoding(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			started, canceled := make(chan struct{}), make(chan struct{})
			c := testMultiClient(t,
				func(ctx context.Context, _ testRequest) (any, error) {
					close(started)
					<-ctx.Done()
					close(canceled)
					return nil, ctx.Err()
				},
				func(context.Context, testRequest) (any, error) { <-started; return t.Name(), nil },
			)
			c.parallelCallTimeout = time.Hour
			decodeHooks.Store(t.Name(), func() {
				select {
				case <-canceled:
				case <-time.After(time.Second):
					t.Error("decoder started before loser cancellation")
				}
			})
			defer decodeHooks.Delete(t.Name())
			var result hookedResult
			if batch {
				b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
				if err := c.BatchCallContext(ctx, b); err != nil || b[0].Error != nil {
					t.Fatalf("call failed: %v %v", err, b[0].Error)
				}
			} else if err := c.CallContext(ctx, &result, "eth_call"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRateLimitDeadlineBoundaryAlwaysClassified(t *testing.T) {
	c := &RpcClient{lim: rate.NewLimiter(rate.Every(time.Hour), 1)}
	c.lim.Allow()
	for i := range 20000 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%50)*time.Microsecond)
		err := c.advanceLimiter(ctx, false)
		cancel()
		if err == nil || !isParallelTimeout(err) {
			t.Fatalf("unclassified limiter deadline error on iteration %d: %T %v", i, err, err)
		}
	}
}

func TestParallelCancellationMetrics(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, callerCancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%v/callerCancel=%v", batch, callerCancel), func(t *testing.T) {
				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()
				started := make(chan struct{})
				c := testMultiClient(t,
					func(ctx context.Context, _ testRequest) (any, error) {
						close(started)
						<-ctx.Done()
						return nil, ctx.Err()
					},
					func(context.Context, testRequest) (any, error) {
						<-started
						if callerCancel {
							cancel()
						}
						return "winner", nil
					},
				)
				c.parallelCallTimeout = time.Hour
				loser := c.allClients[0].rpcClient
				loser.metrics = newMetrics(prometheus.NewRegistry())
				got := callWeightedTest(ctx, c, batch)
				if !callerCancel && (got.err != nil || got.value != "winner") {
					t.Fatalf("call failed: %+v", got)
				}
				waitUntil(t, testContext(t), func() bool { return loser.queued.Load() == 0 })
				want := float64(0)
				if callerCancel {
					want = 1
				}
				metric := &dto.Metric{}
				_ = loser.metrics.Errors.WithLabelValues(loser.id, "eth_call").Write(metric)
				wantErrors := want
				if batch {
					// Existing batch metrics count element errors; geth reports
					// caller cancellation only as the overall batch error.
					wantErrors = 0
				}
				if got := metric.GetCounter().GetValue(); got != wantErrors {
					t.Fatalf("errors=%v, want %v", got, wantErrors)
				}
				_ = loser.metrics.RpcMethodsCalls.WithLabelValues(loser.id, "eth_call").Write(metric)
				if got := metric.GetCounter().GetValue(); got != 1 {
					t.Fatalf("actual attempt count=%v, want 1", got)
				}
				method := "eth_call"
				if batch {
					method = "BatchCall"
				}
				histogram, _ := loser.metrics.RpcCallsDuration.GetMetricWithLabelValues(loser.id, method)
				_ = histogram.(prometheus.Metric).Write(metric)
				if got := metric.GetHistogram().GetSampleCount(); got != uint64(want) {
					t.Fatalf("duration samples=%d, want %v", got, want)
				}
			})
		}
	}
}

func TestEligibilityAndHTTPPreference(t *testing.T) {
	handler := func(context.Context, testRequest) (any, error) { return "http", nil }
	c := testMultiClient(t, handler, handler, handler, handler)
	ws := c.allClients[0]
	ws.rpcClient.c.Close()
	ws.rpcClient.c = gethrpc.DialInProc(gethrpc.NewServer())
	c.allClients[2].setEnabled(false)
	c.allClients[3].rpcClient.sendOnly = true
	c.allClients[3].setEnabled(false)
	c.preferHttpForNonSubscriptionRelated = true
	if got, err := c.getRpcClient(false); err != nil || got != c.allClients[1] {
		t.Fatalf("default HTTP preference lost: %v %v", got, err)
	}
	if got, err := c.getRpcClient(true); err != nil || got != ws {
		t.Fatalf("subscription capability filter lost: %v %v", got, err)
	}
	for _, tc := range []struct {
		subscribe, broadcast bool
		want                 []*internalRpcClient
	}{
		{false, false, c.allClients[:2]},
		{true, false, []*internalRpcClient{ws}},
		{false, true, []*internalRpcClient{ws, c.allClients[1], c.allClients[3]}},
	} {
		got := c.eligibleClients(tc.subscribe, tc.broadcast)
		if len(got) != len(tc.want) {
			t.Fatalf("eligible providers=%d, want %d", len(got), len(tc.want))
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("wrong eligible provider at %d", i)
			}
		}
	}
	// Read dispatch deliberately ignores HTTP preference, preserving weight.
	ws.rpcClient.weight = 100
	var result string
	err := c.CallContext(testContext(t), &result, "eth_call")
	var rpcErr gethrpc.Error
	if !errors.As(err, &rpcErr) || rpcErr.ErrorCode() != -32601 {
		t.Fatalf("preferred subscription-capable provider was skipped: %v", err)
	}
}

func TestParallelProviderFailuresRemainInMetrics(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, timeout := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%v/timeout=%v", batch, timeout), func(t *testing.T) {
				c := testMultiClient(t, func(ctx context.Context, _ testRequest) (any, error) {
					if timeout {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return nil, rpcOutcomeError{-32000, "execution reverted"}
				})
				c.parallelCallTimeout = 500 * time.Millisecond
				provider := c.allClients[0].rpcClient
				provider.metrics = newMetrics(prometheus.NewRegistry())
				if got := callWeightedTest(testContext(t), c, batch); got.err == nil {
					t.Fatal("expected provider failure")
				}
				waitUntil(t, testContext(t), func() bool { return provider.queued.Load() == 0 })
				wantErrors := float64(1)
				if batch && timeout {
					wantErrors = 0 // Existing counters only count batch element errors.
				}
				metric := &dto.Metric{}
				_ = provider.metrics.Errors.WithLabelValues(provider.id, "eth_call").Write(metric)
				if got := metric.GetCounter().GetValue(); got != wantErrors {
					t.Fatalf("provider error metrics suppressed: %v, want %v", got, wantErrors)
				}
				method := "eth_call"
				if batch {
					method = "BatchCall"
				}
				histogram, _ := provider.metrics.RpcCallsDuration.GetMetricWithLabelValues(provider.id, method)
				_ = histogram.(prometheus.Metric).Write(metric)
				if metric.GetHistogram().GetSampleCount() != 1 {
					t.Fatal("provider failure duration suppressed")
				}
			})
		}
	}
}
