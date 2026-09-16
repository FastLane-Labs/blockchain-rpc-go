package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/gorilla/websocket"
)

type rpcOutcomeError struct {
	code    int
	message string
}

func (e rpcOutcomeError) Error() string  { return e.message }
func (e rpcOutcomeError) ErrorCode() int { return e.code }
func (e rpcOutcomeError) ErrorData() any { return "0x12345678" }

func TestParallelTransportErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		transport bool
	}{
		{"success", nil, false},
		{"deadline", context.DeadlineExceeded, true},
		{"canceled provider", context.Canceled, true},
		{"connection reset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, true},
		{"EOF", io.EOF, true},
		{"truncated stream", io.ErrUnexpectedEOF, true},
		{"closed pipe", io.ErrClosedPipe, true},
		{"closed connection", net.ErrClosed, true},
		{"closed client", gethrpc.ErrClientQuit, true},
		{"HTTP failure", gethrpc.HTTPError{StatusCode: 503, Status: "unavailable"}, true},
		{"websocket close", &websocket.CloseError{Code: websocket.CloseAbnormalClosure}, true},
		{"websocket write after close", websocket.ErrCloseSent, true},
		{"ordinary error", errors.New("application error"), false},
		{"timeout text only", errors.New("context deadline exceeded"), false},
		{"RPC revert", rpcOutcomeError{3, "execution reverted"}, false},
		{"RPC timeout", rpcOutcomeError{-32002, "request timed out"}, false},
		{"RPC internal error", rpcOutcomeError{-32603, "internal error"}, false},
		{"unknown RPC code", rpcOutcomeError{123456, "context deadline exceeded"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isParallelTransportError(tc.err); got != tc.transport {
				t.Fatalf("transport=%v, want %v", got, tc.transport)
			}
			if tc.err != nil && isParallelTransportError(fmt.Errorf("wrapped: %w", tc.err)) != tc.transport {
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

func TestParallelWaitsAfterTransportFailure(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, cause := range []error{context.DeadlineExceeded, io.EOF, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}} {
			t.Run(fmt.Sprintf("batch=%v/error=%T", batch, cause), func(t *testing.T) {
				failed := make(chan struct{})
				c := testMultiClient(t,
					func(context.Context, testRequest) (any, error) { return nil, nil },
					func(context.Context, testRequest) (any, error) { <-failed; return "winner", nil },
				)
				setTestTransport(t, c, 0, transportFunc(func(*http.Request) (*http.Response, error) {
					close(failed)
					return nil, cause
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
}

func TestParallelBroadcastContinuesAfterRPCError(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
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
			var err error
			if batch {
				var result string
				b := []gethrpc.BatchElem{{Method: "eth_sendRawTransaction", Result: &result}}
				if err = c.BatchCallContext(ctx, b); err != nil {
					t.Fatal(err)
				}
				err = b[0].Error
			} else {
				err = c.CallContext(ctx, nil, "eth_sendRawTransaction")
			}
			if _, ok := err.(gethrpc.Error); !ok {
				t.Fatalf("expected first RPC error, got %v", err)
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
		})
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
