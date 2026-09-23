package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/gorilla/websocket"
)

func TestParallelWebsocketReconnectTimeout(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			var connects atomic.Int32
			release := make(chan struct{})
			reconnecting := make(chan struct{}, 1)
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if connects.Add(1) == 1 {
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					// Drop an in-flight request so geth observes the disconnection
					// before the next call, without relying on a scheduling delay.
					_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
					_, _, _ = conn.ReadMessage()
					return
				}
				select {
				case reconnecting <- struct{}{}:
				default:
				}
				// Accept TCP but stall the WebSocket handshake. Its socket read
				// deadline produces an I/O timeout, not context.DeadlineExceeded.
				select {
				case <-release:
				case <-r.Context().Done():
				}
			}))
			defer server.Close()
			defer close(release)
			ctx := testContext(t)
			ws, err := gethrpc.DialOptions(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
			if err != nil {
				t.Fatal(err)
			}
			defer ws.Close()
			if err := ws.CallContext(ctx, nil, "test_disconnect"); err == nil {
				t.Fatal("expected the initial connection to be closed")
			}
			fallbackAnswered := make(chan struct{})
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { return "unused", nil },
				func(context.Context, testRequest) (any, error) {
					close(fallbackAnswered)
					return "fallback", nil
				},
			)
			c.allClients[0].rpcClient.c.Close()
			c.allClients[0].rpcClient.c = ws
			c.allClients[0].rpcClient.weight = 100
			c.allClients[1].rpcClient.weight = 10
			c.parallelCallTimeout = 250 * time.Millisecond
			got := callWeightedTest(ctx, c, batch)
			select {
			case <-reconnecting:
			default:
				t.Fatal("test did not trigger a reconnect")
			}
			select {
			case <-fallbackAnswered:
			default:
				t.Fatal("fallback did not answer")
			}
			if ctx.Err() != nil {
				t.Fatalf("caller context unexpectedly expired: %v", ctx.Err())
			}
			if got.err != nil || got.value != "fallback" {
				t.Fatalf("attempt deadline must enable buffered fallback: result=%q err=%T %v, matches DeadlineExceeded=%v", got.value, got.err, got.err, errors.Is(got.err, context.DeadlineExceeded))
			}
		})
	}
}
