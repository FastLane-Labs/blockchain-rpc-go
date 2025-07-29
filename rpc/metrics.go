package rpc

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	MetricKeyClientEnabled        = "ClientEnabled"
	MetricKeyClientFunctionsCalls = "ClientFunctionsCalls"
	MetricKeyRpcMethodsCalls      = "RpcMethodsCalls"
	MetricKeyErrors               = "Errors"
	MetricKeyRpcCallsDuration     = "RpcCallsDuration"

	DefaultQueueSize = 10000
)

var (
	instance *Metrics
	once     sync.Once
)

// MetricEventType represents the type of metric event
type MetricEventType int

const (
	CounterIncEvent MetricEventType = iota
	HistogramObserveEvent
	GaugeSetEvent
)

// MetricEvent represents a metric operation to be processed asynchronously
type MetricEvent struct {
	Type      MetricEventType
	MetricKey string
	Labels    []string
	Value     float64
}

// AsyncMetricsRecorder handles asynchronous metrics processing
type AsyncMetricsRecorder struct {
	metrics    *Metrics
	eventQueue chan MetricEvent
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	queueSize  int
	closed     bool
	mu         sync.RWMutex
}

// NewAsyncMetricsRecorder creates a new async metrics recorder
func NewAsyncMetricsRecorder(metrics *Metrics, queueSize int) *AsyncMetricsRecorder {
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	recorder := &AsyncMetricsRecorder{
		metrics:    metrics,
		eventQueue: make(chan MetricEvent, queueSize),
		ctx:        ctx,
		cancel:     cancel,
		queueSize:  queueSize,
	}

	// Start background worker
	recorder.wg.Add(1)
	go recorder.processEvents()

	return recorder
}

// processEvents runs in background to process metric events
func (r *AsyncMetricsRecorder) processEvents() {
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			// Process remaining events before shutdown
			r.drainEventQueue()
			return
		case event := <-r.eventQueue:
			r.processEvent(event)
		}
	}
}

// drainEventQueue processes all remaining events in the queue
func (r *AsyncMetricsRecorder) drainEventQueue() {
	for {
		select {
		case event := <-r.eventQueue:
			r.processEvent(event)
		default:
			return
		}
	}
}

// processEvent handles a single metric event
func (r *AsyncMetricsRecorder) processEvent(event MetricEvent) {
	if r.metrics == nil {
		return
	}

	switch event.Type {
	case CounterIncEvent:
		switch event.MetricKey {
		case MetricKeyClientFunctionsCalls:
			r.metrics.ClientFunctionsCalls.WithLabelValues(event.Labels...).Inc()
		case MetricKeyRpcMethodsCalls:
			r.metrics.RpcMethodsCalls.WithLabelValues(event.Labels...).Inc()
		case MetricKeyErrors:
			r.metrics.Errors.WithLabelValues(event.Labels...).Inc()
		}

	case HistogramObserveEvent:
		switch event.MetricKey {
		case MetricKeyRpcCallsDuration:
			r.metrics.RpcCallsDuration.WithLabelValues(event.Labels...).Observe(event.Value)
		}

	case GaugeSetEvent:
		switch event.MetricKey {
		case MetricKeyClientEnabled:
			r.metrics.ClientEnabled.WithLabelValues(event.Labels...).Set(event.Value)
		}
	}
}

// RecordCounterInc records a counter increment asynchronously
func (r *AsyncMetricsRecorder) RecordCounterInc(metricKey string, labels ...string) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return
	}
	r.mu.RUnlock()

	event := MetricEvent{
		Type:      CounterIncEvent,
		MetricKey: metricKey,
		Labels:    labels,
	}

	select {
	case r.eventQueue <- event:
		// Event queued successfully
	default:
		// Queue is full, drop the metric to avoid blocking
	}
}

// RecordHistogramObserve records a histogram observation asynchronously
func (r *AsyncMetricsRecorder) RecordHistogramObserve(metricKey string, value float64, labels ...string) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return
	}
	r.mu.RUnlock()

	event := MetricEvent{
		Type:      HistogramObserveEvent,
		MetricKey: metricKey,
		Labels:    labels,
		Value:     value,
	}

	select {
	case r.eventQueue <- event:
		// Event queued successfully
	default:
		// Queue is full, drop the metric to avoid blocking
	}
}

// RecordGaugeSet records a gauge set operation asynchronously
func (r *AsyncMetricsRecorder) RecordGaugeSet(metricKey string, value float64, labels ...string) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return
	}
	r.mu.RUnlock()

	event := MetricEvent{
		Type:      GaugeSetEvent,
		MetricKey: metricKey,
		Labels:    labels,
		Value:     value,
	}

	select {
	case r.eventQueue <- event:
		// Event queued successfully
	default:
		// Queue is full, drop the metric to avoid blocking
	}
}

// Close gracefully shuts down the async metrics recorder
func (r *AsyncMetricsRecorder) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()

	r.cancel()
	r.wg.Wait()
	close(r.eventQueue)
}

// GetQueueLength returns current queue length for monitoring
func (r *AsyncMetricsRecorder) GetQueueLength() int {
	return len(r.eventQueue)
}

// IsQueueFull returns true if the queue is at capacity
func (r *AsyncMetricsRecorder) IsQueueFull() bool {
	return len(r.eventQueue) >= r.queueSize
}

type Metrics struct {
	ClientEnabled        *prometheus.GaugeVec
	ClientFunctionsCalls *prometheus.CounterVec
	RpcMethodsCalls      *prometheus.CounterVec
	Errors               *prometheus.CounterVec
	RpcCallsDuration     *prometheus.HistogramVec

	// Async recorder for non-blocking metrics
	AsyncRecorder *AsyncMetricsRecorder
}

func getMetrics(reg prometheus.Registerer) *Metrics {
	once.Do(func() {
		instance = newMetrics(reg)
	})
	return instance
}

// ShutdownMetrics gracefully shuts down the global metrics instance
// This ensures all queued metric events are processed before shutdown
func ShutdownMetrics() {
	if instance != nil && instance.AsyncRecorder != nil {
		instance.AsyncRecorder.Close()
	}
}

// GetMetricsQueueStatus returns information about the async metrics queue
// Returns queue length, queue capacity, and whether the queue is full
func GetMetricsQueueStatus() (queueLength int, queueCapacity int, isFull bool) {
	if instance != nil && instance.AsyncRecorder != nil {
		return instance.AsyncRecorder.GetQueueLength(),
			instance.AsyncRecorder.queueSize,
			instance.AsyncRecorder.IsQueueFull()
	}
	return 0, 0, false
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ClientEnabled: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "blockchain_rpc_go_client_enabled",
			Help: "Whether a client is enabled",
		}, []string{"id"}),

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
		}, []string{"id", "method"}),

		RpcCallsDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "blockchain_rpc_go_rpc_calls_duration",
			Help:    "Duration of RPC calls",
			Buckets: prometheus.DefBuckets,
		}, []string{"id", "method"}),
	}

	reg.MustRegister(
		m.ClientEnabled,
		m.ClientFunctionsCalls,
		m.RpcMethodsCalls,
		m.Errors,
		m.RpcCallsDuration,
	)

	// Initialize async recorder with default queue size of 1000
	m.AsyncRecorder = NewAsyncMetricsRecorder(m, DefaultQueueSize)

	return m
}
