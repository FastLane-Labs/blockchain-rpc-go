package rpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

type RpcClientConfig struct {
	RpcClientData

	// PrometheusRegisterer is used to register metrics with Prometheus
	PrometheusRegisterer prometheus.Registerer
}

type RpcClient struct {
	id      string
	c       *rpc.Client
	lim     *rate.Limiter
	sem     *semaphore.Weighted
	maxConc uint64
	queued  atomic.Uint64
	weight  uint64
	metrics *Metrics
}

func Dial(cfg *RpcClientConfig) (*RpcClient, error) {
	return DialContext(context.Background(), cfg)
}

func DialContext(ctx context.Context, cfg *RpcClientConfig) (*RpcClient, error) {
	c, err := rpc.DialContext(ctx, cfg.Url)
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	if cfg != nil && cfg.Id != "" {
		id = cfg.Id
	}

	var lim *rate.Limiter
	if cfg != nil && cfg.RateLimit > 0 {
		lim = rate.NewLimiter(rate.Limit(cfg.RateLimit), int(cfg.RateLimit))
	}

	var sem *semaphore.Weighted
	if cfg != nil && cfg.MaxConcurrency > 0 {
		sem = semaphore.NewWeighted(int64(cfg.MaxConcurrency))
	}

	var metrics *Metrics
	if cfg != nil && cfg.PrometheusRegisterer != nil {
		metrics = getMetrics(cfg.PrometheusRegisterer)
	}

	return &RpcClient{
		id:      id,
		c:       c,
		lim:     lim,
		sem:     sem,
		maxConc: cfg.MaxConcurrency,
		weight:  cfg.Weight,
		metrics: metrics,
	}, nil
}

func (c *RpcClient) BatchCall(b []rpc.BatchElem) error {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "BatchCall")
	}
	return c._batchCallContext(context.Background(), b)
}

func (c *RpcClient) BatchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "BatchCallContext")
	}
	return c._batchCallContext(ctx, b)
}

func (c *RpcClient) _batchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	done, err := c.applyRateLimit(ctx, false)
	if err != nil {
		return err
	}
	defer done()

	start := time.Now()
	err = c.c.BatchCallContext(ctx, b)

	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		duration := time.Since(start).Seconds()
		c.metrics.AsyncRecorder.RecordHistogramObserve(MetricKeyRpcCallsDuration, duration, c.id, "BatchCall")

		for _, elem := range b {
			c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyRpcMethodsCalls, c.id, elem.Method)
			if elem.Error != nil {
				c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyErrors, c.id, elem.Method)
			}
		}
	}

	return err
}

func (c *RpcClient) Call(result any, method string, args ...any) error {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "Call")
	}
	return c._callContext(context.Background(), result, method, args...)
}

func (c *RpcClient) CallContext(ctx context.Context, result any, method string, args ...any) error {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "CallContext")
	}
	return c._callContext(ctx, result, method, args...)
}

func (c *RpcClient) _callContext(ctx context.Context, result any, method string, args ...any) error {
	done, err := c.applyRateLimit(ctx, false)
	if err != nil {
		return err
	}
	defer done()

	start := time.Now()
	err = c.c.CallContext(ctx, result, method, args...)

	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		duration := time.Since(start).Seconds()
		c.metrics.AsyncRecorder.RecordHistogramObserve(MetricKeyRpcCallsDuration, duration, c.id, method)
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyRpcMethodsCalls, c.id, method)
		if err != nil {
			c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyErrors, c.id, method)
		}
	}

	return err
}

func (c *RpcClient) Close() {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "Close")
	}
	c.c.Close()
}

func (c *RpcClient) EthSubscribe(ctx context.Context, channel any, args ...any) (*rpc.ClientSubscription, error) {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "EthSubscribe")
	}
	return c._subscribe(ctx, "eth", channel, args...)
}

func (c *RpcClient) ShhSubscribe(ctx context.Context, channel any, args ...any) (*rpc.ClientSubscription, error) {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "ShhSubscribe")
	}
	return c._subscribe(ctx, "shh", channel, args...)
}

func (c *RpcClient) Subscribe(ctx context.Context, namespace string, channel any, args ...any) (*rpc.ClientSubscription, error) {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "Subscribe")
	}
	return c._subscribe(ctx, namespace, channel, args...)
}

func (c *RpcClient) _subscribe(ctx context.Context, namespace string, channel any, args ...any) (*rpc.ClientSubscription, error) {
	done, err := c.applyRateLimit(ctx, false)
	if err != nil {
		return nil, err
	}
	defer done()

	start := time.Now()
	sub, err := c.c.Subscribe(ctx, namespace, channel, args...)

	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		method := fmt.Sprintf("%s_subscribe", namespace)
		duration := time.Since(start).Seconds()
		c.metrics.AsyncRecorder.RecordHistogramObserve(MetricKeyRpcCallsDuration, duration, c.id, method)
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyRpcMethodsCalls, c.id, method)
		if err != nil {
			c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyErrors, c.id, method)
		}
	}

	return sub, err
}

func (c *RpcClient) Notify(ctx context.Context, method string, args ...any) error {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "Notify")
	}

	done, err := c.applyRateLimit(ctx, false)
	if err != nil {
		return err
	}
	defer done()

	start := time.Now()
	err = c.c.Notify(ctx, method, args...)

	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		duration := time.Since(start).Seconds()
		c.metrics.AsyncRecorder.RecordHistogramObserve(MetricKeyRpcCallsDuration, duration, c.id, method)
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyRpcMethodsCalls, c.id, method)
		if err != nil {
			c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyErrors, c.id, method)
		}
	}

	return err
}

func (c *RpcClient) RegisterName(name string, receiver any) error {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "RegisterName")
	}

	return c.c.RegisterName(name, receiver)
}

func (c *RpcClient) SetHeader(key string, value string) {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "SetHeader")
	}

	c.c.SetHeader(key, value)
}

func (c *RpcClient) SupportedModules() (map[string]string, error) {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "SupportedModules")
	}

	done, err := c.applyRateLimit(context.Background(), false)
	if err != nil {
		return nil, err
	}
	defer done()

	start := time.Now()
	modules, err := c.c.SupportedModules()

	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		duration := time.Since(start).Seconds()
		c.metrics.AsyncRecorder.RecordHistogramObserve(MetricKeyRpcCallsDuration, duration, c.id, "rpc_modules")
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyRpcMethodsCalls, c.id, "rpc_modules")
		if err != nil {
			c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyErrors, c.id, "rpc_modules")
		}
	}

	return modules, err
}

func (c *RpcClient) SupportsSubscriptions() bool {
	if c.metrics != nil && c.metrics.AsyncRecorder != nil {
		c.metrics.AsyncRecorder.RecordCounterInc(MetricKeyClientFunctionsCalls, c.id, "SupportsSubscriptions")
	}

	return c.c.SupportsSubscriptions()
}
