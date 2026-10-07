package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/prometheus/client_golang/prometheus"
)

// The transport keeps network jitter out of the overhead comparison. The delay
// cases add a fixed provider latency; they are not production latency estimates.
type benchmarkTransport struct{ delay time.Duration }

func (tr benchmarkTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if tr.delay != 0 {
		timer := time.NewTimer(tr.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	var msg struct{ ID json.RawMessage }
	if err := json.NewDecoder(req.Body).Decode(&msg); err != nil {
		return nil, err
	}
	body := `{"jsonrpc":"2.0","id":` + string(msg.ID) + `,"result":"0x1234"}`
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
}

// Report latency percentiles as well as aggregate throughput under concurrent
// load. Run with -cpu=4 to keep the number of in-flight application calls fixed.
func BenchmarkRPCConcurrent(b *testing.B) {
	for _, delay := range []time.Duration{0, time.Millisecond} {
		for _, count := range []int{1, 2, 4} {
			for _, parallel := range []bool{false, true} {
				name := fmt.Sprintf("delay=%s/providers=%d/parallel=%v", delay, count, parallel)
				b.Run(name, func(b *testing.B) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					c := &MultiRpcClient{parallelCalls: parallel, stopped: make(chan struct{})}
					metrics := newMetrics(prometheus.NewRegistry())
					for i := 0; i < count; i++ {
						client, err := gethrpc.DialOptions(ctx, "http://benchmark", gethrpc.WithHTTPClient(&http.Client{Transport: benchmarkTransport{delay: delay}}))
						if err != nil {
							b.Fatal(err)
						}
						ic := &internalRpcClient{rpcClient: &RpcClient{id: fmt.Sprintf("provider-%d", i), c: client, metrics: metrics}}
						ic.setEnabled(true)
						c.allClients = append(c.allClients, ic)
					}
					defer c.Close()
					parts := make(chan []time.Duration, runtime.GOMAXPROCS(0))
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						samples := make([]time.Duration, 0, b.N/runtime.GOMAXPROCS(0)+1)
						for pb.Next() {
							started := time.Now()
							var result string
							if err := c.CallContext(ctx, &result, "eth_call", "0x1234", "latest"); err != nil {
								b.Error(err)
							}
							samples = append(samples, time.Since(started))
						}
						parts <- samples
					})
					b.StopTimer()
					close(parts)
					var samples []time.Duration
					for part := range parts {
						samples = append(samples, part...)
					}
					slices.Sort(samples)
					for _, percentile := range []int{50, 95, 99} {
						index := (len(samples) - 1) * percentile / 100
						b.ReportMetric(float64(samples[index].Nanoseconds())/1000, fmt.Sprintf("p%d-us", percentile))
					}
				})
			}
		}
	}
}

func BenchmarkRPCCall(b *testing.B) {
	for _, delay := range []time.Duration{0, time.Millisecond} {
		for _, metricsEnabled := range []bool{false, true} {
			for _, count := range []int{1, 2, 4} {
				for _, parallel := range []bool{false, true} {
					name := fmt.Sprintf("delay=%s/metrics=%v/providers=%d/parallel=%v", delay, metricsEnabled, count, parallel)
					b.Run(name, func(b *testing.B) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						c := &MultiRpcClient{parallelCalls: parallel, stopped: make(chan struct{})}
						var metrics *Metrics
						if metricsEnabled {
							metrics = newMetrics(prometheus.NewRegistry())
						}
						for i := 0; i < count; i++ {
							client, err := gethrpc.DialOptions(ctx, "http://benchmark", gethrpc.WithHTTPClient(&http.Client{Transport: benchmarkTransport{delay: delay}}))
							if err != nil {
								b.Fatal(err)
							}
							ic := &internalRpcClient{rpcClient: &RpcClient{id: fmt.Sprintf("provider-%d", i), c: client, metrics: metrics}}
							ic.setEnabled(true)
							c.allClients = append(c.allClients, ic)
						}
						defer c.Close()
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							var result string
							if err := c.CallContext(ctx, &result, "eth_call", "0x1234", "latest"); err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
					})
				}
			}
		}
	}
}
