package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/time/rate"
)

type rpcOutcomeError struct {
	code    int
	message string
}

func (e rpcOutcomeError) Error() string  { return e.message }
func (e rpcOutcomeError) ErrorCode() int { return e.code }
func (e rpcOutcomeError) ErrorData() any { return "0x12345678" }

func TestParallelTimeoutClassification(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		timeout bool
	}{
		{"success", nil, false},
		{"deadline", context.DeadlineExceeded, true},
		{"HTTP deadline", &url.Error{Op: "Post", URL: "http://provider", Err: context.DeadlineExceeded}, true},
		{"limiter deadline", rateLimitWaitError{errors.New("rate: Wait(n=1) would exceed context deadline")}, true},
		{"caller cancellation", context.Canceled, false},
		{"connection reset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, false},
		{"EOF", io.EOF, false},
		{"HTTP failure", gethrpc.HTTPError{StatusCode: 503, Status: "unavailable"}, false},
		{"malformed JSON", &json.SyntaxError{Offset: 12}, false},
		{"closed client", gethrpc.ErrClientQuit, false},
		{"deadline text only", errors.New("context deadline exceeded"), false},
		{"RPC timeout", rpcOutcomeError{-32002, "request timed out"}, false},
		{"RPC deadline message", rpcOutcomeError{-32000, "context deadline exceeded"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isParallelTimeout(tc.err); got != tc.timeout {
				t.Fatalf("timeout=%v, want %v", got, tc.timeout)
			}
			if tc.err != nil && isParallelTimeout(fmt.Errorf("wrapped: %w", tc.err)) != tc.timeout {
				t.Fatal("wrapping changed classification")
			}
		})
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func setTestTransport(t *testing.T, c *MultiRpcClient, index int, transport http.RoundTripper) {
	t.Helper()
	client, err := gethrpc.DialOptions(testContext(t), "http://test", gethrpc.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	c.allClients[index].rpcClient.c.Close()
	c.allClients[index].rpcClient.c = client
}

func TestParallelRPCErrorWinsUnchanged(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, providers := range []int{1, 2} {
			for _, code := range []int{3, -32002, -32603, 123456} {
				t.Run(fmt.Sprintf("batch=%v/providers=%d/code=%d", batch, providers, code), func(t *testing.T) {
					ctx := testContext(t)
					started := make(chan struct{})
					handlers := []testHandler{func(context.Context, testRequest) (any, error) {
						if providers > 1 {
							<-started
						}
						return nil, rpcOutcomeError{code, "context deadline exceeded"}
					}}
					canceled := make(chan struct{})
					if providers > 1 {
						handlers = append(handlers, func(ctx context.Context, _ testRequest) (any, error) {
							close(started)
							<-ctx.Done()
							close(canceled)
							return "late success", nil
						})
					}
					c := testMultiClient(t, handlers...)
					result := "unchanged"
					var err error
					if batch {
						b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
						if err = c.BatchCallContext(ctx, b); err != nil {
							t.Fatal(err)
						}
						err = b[0].Error
					} else {
						err = c.CallContext(ctx, &result, "eth_call")
					}
					coded, codeOK := err.(gethrpc.Error)
					data, dataOK := err.(gethrpc.DataError)
					if !codeOK || !dataOK || coded.ErrorCode() != code || data.ErrorData() != "0x12345678" || result != "unchanged" {
						t.Fatalf("RPC outcome changed: result=%q err=%T %v", result, err, err)
					}
					if providers > 1 {
						receive(t, ctx, canceled)
					}
				})
			}
		}
	}
}

func TestParallelWaitsAfterTimeout(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			timedOut := make(chan struct{})
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return nil, nil },
				func(context.Context, testRequest) (any, error) { <-timedOut; return "winner", nil },
			)
			setTestTransport(t, c, 0, transportFunc(func(*http.Request) (*http.Response, error) {
				close(timedOut)
				return nil, context.DeadlineExceeded
			}))
			var result string
			if batch {
				b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
				if err := c.BatchCallContext(testContext(t), b); err != nil || b[0].Error != nil {
					t.Fatalf("failed to wait for working provider: %v %v", err, b[0].Error)
				}
			} else if err := c.CallContext(testContext(t), &result, "eth_call"); err != nil {
				t.Fatal(err)
			}
			if result != "winner" {
				t.Fatalf("got %q", result)
			}
		})
	}
}

// Any answer other than a timeout ends the race, including transport failures
// and malformed responses, and cancels the other copies.
func TestParallelNonTimeoutFailureIsAnAnswer(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for name, cause := range map[string]error{
			"connection": io.ErrUnexpectedEOF,
			"HTTP":       gethrpc.HTTPError{StatusCode: 503, Status: "503 unavailable"},
		} {
			t.Run(fmt.Sprintf("batch=%v/%s", batch, name), func(t *testing.T) {
				started, canceled := make(chan struct{}), make(chan struct{})
				c := testMultiClient(t,
					func(context.Context, testRequest) (any, error) { return nil, nil },
					func(ctx context.Context, _ testRequest) (any, error) {
						close(started)
						<-ctx.Done()
						close(canceled)
						return nil, ctx.Err()
					},
				)
				setTestTransport(t, c, 0, transportFunc(func(r *http.Request) (*http.Response, error) {
					select {
					case <-started:
					case <-r.Context().Done():
					}
					if httpErr, ok := cause.(gethrpc.HTTPError); ok {
						return &http.Response{StatusCode: httpErr.StatusCode, Status: httpErr.Status, Body: io.NopCloser(strings.NewReader(""))}, nil
					}
					return nil, cause
				}))
				ctx := testContext(t)
				result := "unchanged"
				var err error
				if batch {
					err = c.BatchCallContext(ctx, []gethrpc.BatchElem{{Method: "eth_call", Result: &result}})
				} else {
					err = c.CallContext(ctx, &result, "eth_call")
				}
				var httpErr gethrpc.HTTPError
				if err == nil || result != "unchanged" || (!errors.Is(err, cause) && !errors.As(err, &httpErr)) {
					t.Fatalf("expected the failure to end the race: %q, %v", result, err)
				}
				receive(t, ctx, canceled)
			})
		}
	}
}

func TestParallelBroadcastWaitsForSuccess(t *testing.T) {
	for _, allFail := range []bool{false, true} {
		t.Run(fmt.Sprintf("allFail=%v", allFail), func(t *testing.T) {
			ctx := testContext(t)
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { <-started; return nil, revertError{} },
				func(ctx context.Context, _ testRequest) (any, error) {
					close(started)
					<-release
					if ctx.Err() != nil {
						t.Error("slow broadcast was canceled")
					}
					if allFail {
						return nil, rpcOutcomeError{-32000, "nonce too low"}
					}
					return "0xhash", nil
				},
			)
			var result string
			returned := make(chan error, 1)
			go func() { returned <- c.CallContext(ctx, &result, "eth_sendRawTransaction") }()
			receive(t, ctx, started)
			// The fast rejection is delivered while the slow provider is still held.
			select {
			case err := <-returned:
				t.Fatalf("broadcast ended on first RPC error: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			releaseOnce.Do(func() { close(release) })
			err := receive(t, ctx, returned)
			if !allFail {
				if err != nil || result != "0xhash" {
					t.Fatalf("expected the later acceptance, got %q, %v", result, err)
				}
				return
			}
			var rpcErr gethrpc.Error
			if !errors.As(err, &rpcErr) || result != "" {
				t.Fatalf("expected aggregated RPC errors, got %q, %v", result, err)
			}
			for _, want := range []string{"provider-0: execution reverted", "provider-1: nonce too low"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("missing %q in %v", want, err)
				}
			}
		})
	}
}

// A batch containing a transaction still selects the first RPC outcome. Its
// losing copies must not be canceled.
func TestParallelBroadcastBatchContinuesAfterRPCError(t *testing.T) {
	ctx := testContext(t)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	finished := make(chan error, 1)
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) { <-started; return nil, revertError{} },
		func(context.Context, testRequest) (any, error) { return nil, nil },
	)
	setTestTransport(t, c, 1, transportFunc(func(r *http.Request) (*http.Response, error) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		finished <- r.Context().Err()
		return nil, io.EOF
	}))
	var result string
	b := []gethrpc.BatchElem{{Method: "eth_sendRawTransaction", Result: &result}}
	if err := c.BatchCallContext(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, ok := b[0].Error.(gethrpc.Error); !ok {
		t.Fatalf("expected first RPC error, got %v", b[0].Error)
	}
	select {
	case release <- struct{}{}:
	case err := <-finished:
		t.Fatalf("broadcast canceled before release: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := receive(t, ctx, finished); err != nil {
		t.Fatalf("losing broadcast was canceled: %v", err)
	}
}

func TestParallelNullIsAnAcceptedResult(t *testing.T) {
	started := make(chan struct{})
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) { <-started; return json.RawMessage("null"), nil },
		func(ctx context.Context, _ testRequest) (any, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)
	var result json.RawMessage
	if err := c.CallContext(testContext(t), &result, "eth_getTransactionReceipt"); err != nil || strings.TrimSpace(string(result)) != "null" {
		t.Fatalf("null should finish the race: %s %v", result, err)
	}
}

func TestParallelBatchAcceptsElementOutcomes(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) { return nil, nil },
		func(ctx context.Context, req testRequest) (any, error) {
			if req.Method == "test_null" {
				close(started)
				<-ctx.Done()
				close(canceled)
			}
			return nil, ctx.Err()
		},
	)
	setTestTransport(t, c, 0, transportFunc(func(req *http.Request) (*http.Response, error) {
		var requests []testRequest
		if err := json.NewDecoder(req.Body).Decode(&requests); err != nil {
			return nil, err
		}
		select {
		case <-started:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		responses := []testResponse{
			{Version: "2.0", ID: requests[0].ID, Result: json.RawMessage("null")},
			{Version: "2.0", ID: requests[1].ID}, // Missing result field.
			// No response at all for requests[2].
			{Version: "2.0", ID: requests[3].ID, Error: map[string]any{"code": -32002, "message": "request timed out", "data": "0x1234"}},
			{Version: "2.0", ID: requests[4].ID, Result: "winner"},
		}
		body, err := json.Marshal(responses)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	}))
	var null json.RawMessage
	noResult, missing, reverted, success := "kept", "kept", "kept", ""
	b := []gethrpc.BatchElem{
		{Method: "test_null", Result: &null},
		{Method: "test_no_result", Result: &noResult},
		{Method: "test_missing", Result: &missing},
		{Method: "test_reverted", Result: &reverted},
		{Method: "test_success", Result: &success},
	}
	ctx := testContext(t)
	if err := c.BatchCallContext(ctx, b); err != nil {
		t.Fatal(err)
	}
	if string(null) != "null" || b[0].Error != nil || b[1].Error != gethrpc.ErrNoResult || b[2].Error != gethrpc.ErrMissingBatchResponse {
		t.Fatalf("batch element errors changed: %+v", b)
	}
	rpcErr, ok := b[3].Error.(gethrpc.Error)
	dataErr, dataOK := b[3].Error.(gethrpc.DataError)
	if !ok || !dataOK || rpcErr.ErrorCode() != -32002 || dataErr.ErrorData() != "0x1234" {
		t.Fatalf("RPC error changed: %v", b[3].Error)
	}
	if noResult != "kept" || missing != "kept" || reverted != "kept" || success != "winner" || b[4].Error != nil {
		t.Fatalf("incorrect results: %q %q %q %q", noResult, missing, reverted, success)
	}
	receive(t, ctx, canceled)
}

func TestParallelSkipsExhaustedRateLimit(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) {
					t.Error("rate-limited request reached the provider")
					return "unexpected", nil
				},
				func(context.Context, testRequest) (any, error) {
					time.Sleep(10 * time.Millisecond)
					return "winner", nil
				},
			)
			lim := rate.NewLimiter(1, 1)
			lim.Allow()
			c.allClients[0].rpcClient.lim = lim
			ctx, cancel := context.WithTimeout(testContext(t), 500*time.Millisecond)
			defer cancel()
			var result string
			var err error
			if batch {
				b := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
				if err = c.BatchCallContext(ctx, b); err == nil {
					err = b[0].Error
				}
			} else {
				err = c.CallContext(ctx, &result, "eth_call")
			}
			if err != nil || result != "winner" {
				t.Fatalf("local limiter failure won: %q, %v", result, err)
			}
		})
	}
}
