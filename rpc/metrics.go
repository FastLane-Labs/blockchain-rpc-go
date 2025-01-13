package rpc

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

var (
	histogramBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.8, 0.9, 1.0, 1.5, 2.0, 3.0}

	instance *Metrics
	once     sync.Once
)

type Metrics struct {
	ClientFunctionsCalls *prometheus.CounterVec
	RpcMethodsCalls      *prometheus.CounterVec
	Errors               *prometheus.CounterVec
	RpcCallsDuration     *prometheus.HistogramVec
}

func getMetrics(reg prometheus.Registerer) *Metrics {
	once.Do(func() {
		instance = newMetrics(reg)
	})
	return instance
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ClientFunctionsCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockchain_rpc_go_client_functions_calls",
			Help: "Number of times a client function is called",
		}, []string{"id", "function"}),

		RpcMethodsCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockchain_rpc_go_rpc_methods_calls",
			Help: "Number of times an RPC method is called",
		}, []string{"id", "method"}),

		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockchain_rpc_go_rpc_errors",
			Help: "Number of times an RPC method returns an error",
		}, []string{"id", "method", "error"}),

		RpcCallsDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "blockchain_rpc_go_rpc_calls_duration",
			Help:    "Duration of RPC calls",
			Buckets: histogramBuckets,
		}, []string{"id", "method"}),
	}

	reg.MustRegister(
		m.ClientFunctionsCalls,
		m.RpcMethodsCalls,
		m.Errors,
		m.RpcCallsDuration,
	)

	return m
}
