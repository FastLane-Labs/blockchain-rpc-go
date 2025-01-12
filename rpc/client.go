package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

type IRpcClient interface {
	BatchCall(b []rpc.BatchElem) error
	BatchCallContext(ctx context.Context, b []rpc.BatchElem) error
	Call(result interface{}, method string, args ...interface{}) error
	CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error
	Close()
	EthSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (*rpc.ClientSubscription, error)
	Notify(ctx context.Context, method string, args ...interface{}) error
	RegisterName(name string, receiver interface{}) error
	SetHeader(key string, value string)
	ShhSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (*rpc.ClientSubscription, error)
	Subscribe(ctx context.Context, namespace string, channel interface{}, args ...interface{}) (*rpc.ClientSubscription, error)
	SupportedModules() (map[string]string, error)
	SupportsSubscriptions() bool
}

type RpcClientConfig struct {
	// Id is useful to identify the client in metrics
	Id string

	// MaxConcurrency is the maximum number of concurrent requests to the RPC server (0 = no limit)
	MaxConcurrency uint64

	// PrometheusRegisterer is used to register metrics with Prometheus
	PrometheusRegisterer prometheus.Registerer
}

type RpcClient struct {
	id      string
	c       *rpc.Client
	sem     *semaphore.Weighted
	metrics *Metrics
}

func Dial(url string, cfg *RpcClientConfig) (*RpcClient, error) {
	return DialContext(context.Background(), url, cfg)
}

func DialContext(ctx context.Context, url string, cfg *RpcClientConfig) (*RpcClient, error) {
	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, err
	}

	var sem *semaphore.Weighted
	if cfg != nil && cfg.MaxConcurrency > 0 {
		sem = semaphore.NewWeighted(int64(cfg.MaxConcurrency))
	}

	var metrics *Metrics
	if cfg != nil && cfg.PrometheusRegisterer != nil {
		metrics = NewMetrics(cfg.PrometheusRegisterer)
	}

	id := uuid.New().String()
	if cfg != nil && cfg.Id != "" {
		id = cfg.Id
	}

	return &RpcClient{
		id:      id,
		c:       c,
		sem:     sem,
		metrics: metrics,
	}, nil
}

func (c *RpcClient) acquireSemaphore(ctx context.Context, weight int64) error {
	if c.sem == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return c.sem.Acquire(ctx, weight)
}

func (c *RpcClient) releaseSemaphore(weight int64) {
	if c.sem == nil {
		return
	}
	c.sem.Release(weight)
}

func (c *RpcClient) BatchCall(b []rpc.BatchElem) error {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "BatchCall").Inc()
	}
	return c._batchCallContext(context.Background(), b)
}

func (c *RpcClient) BatchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "BatchCallContext").Inc()
	}
	return c._batchCallContext(ctx, b)
}

func (c *RpcClient) _batchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	if err := c.acquireSemaphore(ctx, 1); err != nil {
		return err
	}
	defer c.releaseSemaphore(1)

	start := time.Now()
	err := c.c.BatchCallContext(ctx, b)

	if c.metrics != nil {
		c.metrics.RpcCallsDuration.WithLabelValues(c.id, "BatchCall").Observe(time.Since(start).Seconds())
		for _, elem := range b {
			c.metrics.RpcMethodsCalls.WithLabelValues(c.id, elem.Method).Inc()
			if elem.Error != nil {
				c.metrics.Errors.WithLabelValues(c.id, elem.Method, elem.Error.Error()).Inc()
			}
		}
	}

	return err
}

func (c *RpcClient) Call(result interface{}, method string, args ...interface{}) error {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "Call").Inc()
	}
	return c._callContext(context.Background(), result, method, args...)
}

func (c *RpcClient) CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "CallContext").Inc()
	}
	return c._callContext(ctx, result, method, args...)
}

func (c *RpcClient) _callContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	if err := c.acquireSemaphore(ctx, 1); err != nil {
		return err
	}
	defer c.releaseSemaphore(1)

	start := time.Now()
	err := c.c.CallContext(ctx, result, method, args...)

	if c.metrics != nil {
		c.metrics.RpcCallsDuration.WithLabelValues(c.id, method).Observe(time.Since(start).Seconds())
		c.metrics.RpcMethodsCalls.WithLabelValues(c.id, method).Inc()
		if err != nil {
			c.metrics.Errors.WithLabelValues(c.id, method, err.Error()).Inc()
		}
	}

	return err
}

func (c *RpcClient) Close() {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "Close").Inc()
	}
	c.c.Close()
}

func (c *RpcClient) EthSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (*rpc.ClientSubscription, error) {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "EthSubscribe").Inc()
	}
	return c._subscribe(ctx, "eth", channel, args...)
}

func (c *RpcClient) ShhSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (*rpc.ClientSubscription, error) {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "ShhSubscribe").Inc()
	}
	return c._subscribe(ctx, "shh", channel, args...)
}

func (c *RpcClient) Subscribe(ctx context.Context, namespace string, channel interface{}, args ...interface{}) (*rpc.ClientSubscription, error) {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "Subscribe").Inc()
	}
	return c._subscribe(ctx, namespace, channel, args...)
}

func (c *RpcClient) _subscribe(ctx context.Context, namespace string, channel interface{}, args ...interface{}) (*rpc.ClientSubscription, error) {
	if err := c.acquireSemaphore(ctx, 1); err != nil {
		return nil, err
	}
	defer c.releaseSemaphore(1)

	start := time.Now()
	sub, err := c.c.Subscribe(ctx, namespace, channel, args...)

	if c.metrics != nil {
		method := fmt.Sprintf("%s_subscribe", namespace)
		c.metrics.RpcCallsDuration.WithLabelValues(c.id, method).Observe(time.Since(start).Seconds())
		c.metrics.RpcMethodsCalls.WithLabelValues(c.id, method).Inc()
		if err != nil {
			c.metrics.Errors.WithLabelValues(c.id, method, err.Error()).Inc()
		}
	}

	return sub, err
}

func (c *RpcClient) Notify(ctx context.Context, method string, args ...interface{}) error {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "Notify").Inc()
	}

	if err := c.acquireSemaphore(ctx, 1); err != nil {
		return err
	}
	defer c.releaseSemaphore(1)

	start := time.Now()
	err := c.c.Notify(ctx, method, args...)

	if c.metrics != nil {
		c.metrics.RpcCallsDuration.WithLabelValues(c.id, method).Observe(time.Since(start).Seconds())
		c.metrics.RpcMethodsCalls.WithLabelValues(c.id, method).Inc()
		if err != nil {
			c.metrics.Errors.WithLabelValues(c.id, method, err.Error()).Inc()
		}
	}

	return err
}

func (c *RpcClient) RegisterName(name string, receiver interface{}) error {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "RegisterName").Inc()
	}

	return c.c.RegisterName(name, receiver)
}

func (c *RpcClient) SetHeader(key string, value string) {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "SetHeader").Inc()
	}

	c.c.SetHeader(key, value)
}

func (c *RpcClient) SupportedModules() (map[string]string, error) {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "SupportedModules").Inc()
	}

	if err := c.acquireSemaphore(context.Background(), 1); err != nil {
		return nil, err
	}
	defer c.releaseSemaphore(1)

	start := time.Now()
	modules, err := c.c.SupportedModules()

	if c.metrics != nil {
		c.metrics.RpcCallsDuration.WithLabelValues(c.id, "rpc_modules").Observe(time.Since(start).Seconds())
		c.metrics.RpcMethodsCalls.WithLabelValues(c.id, "rpc_modules").Inc()
		if err != nil {
			c.metrics.Errors.WithLabelValues(c.id, "rpc_modules", err.Error()).Inc()
		}
	}

	return modules, err
}

func (c *RpcClient) SupportsSubscriptions() bool {
	if c.metrics != nil {
		c.metrics.ClientFunctionsCalls.WithLabelValues(c.id, "SupportsSubscriptions").Inc()
	}

	return c.c.SupportsSubscriptions()
}
