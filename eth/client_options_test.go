package eth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	brpc "github.com/FastLane-Labs/blockchain-rpc-go/rpc"
)

var (
	_ func(*brpc.MultiRpcClientConfig) (*EthClient, error)                  = DialMulti
	_ func(context.Context, *brpc.MultiRpcClientConfig) (*EthClient, error) = DialMultiContext
)

func TestDialMultiContextWithOptionsRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := DialMultiContextWithOptions(ctx, nil, brpc.MultiRpcClientOptions{ParallelCallTimeout: -1}); err == nil {
		t.Fatal("invalid options were not forwarded")
	}
	var calls [2]atomic.Int32
	var data []*brpc.RpcClientData
	for i, weight := range []uint64{10, 100} { // Exercise constructor sorting too.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				return
			}
			result := "0x1"
			if req.Method == "eth_blockNumber" {
				calls[i].Add(1)
				if weight == 100 {
					<-r.Context().Done()
					return
				}
				result = "0x2a"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
		}))
		t.Cleanup(server.Close)
		data = append(data, &brpc.RpcClientData{Url: server.URL, Weight: weight})
	}
	client, err := DialMultiContextWithOptions(ctx, &brpc.MultiRpcClientConfig{RpcData: data, HealthCheckInterval: time.Hour},
		brpc.MultiRpcClientOptions{ParallelCalls: true, ParallelCallTimeout: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	before := time.Now()
	block, err := client.BlockNumber(ctx)
	if err != nil || block != 42 || calls[0].Load() != 1 || calls[1].Load() != 1 {
		t.Fatalf("options not applied: block=%d err=%v calls=%d,%d", block, err, calls[0].Load(), calls[1].Load())
	}
	if elapsed := time.Since(before); elapsed < 300*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Fatalf("configured attempt timeout not used: %v", elapsed)
	}
}
