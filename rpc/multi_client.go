package rpc

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	subscribeMethodSuffix = "_subscribe"
)

var (
	ErrNoAvailableClients = errors.New("no available clients")
)

type MultiRpcClientConfig struct {
	RpcData []*RpcClientData

	// Prioritize HTTP RPCs for non-subscription related requests (recommended in high throughput environments)
	PreferHttpForNonSubscriptionRelated bool

	// HealthCheckInterval is the interval at which the health check loop will run for all clients (defaults to 1 second)
	HealthCheckInterval time.Duration

	// HealthCheckTimeout is the timeout for each health check (defaults to 1 seconds)
	HealthCheckTimeout time.Duration

	// PrometheusRegisterer is used to register metrics with Prometheus
	PrometheusRegisterer prometheus.Registerer
}

type internalRpcClient struct {
	rpcClient *RpcClient
	enabled   atomic.Bool
}

func (c *internalRpcClient) setEnabled(enabled bool) {
	c.enabled.Store(enabled)

	if c.rpcClient.metrics != nil && c.rpcClient.metrics.AsyncRecorder != nil {
		if enabled {
			c.rpcClient.metrics.AsyncRecorder.RecordGaugeSet(MetricKeyClientEnabled, 1, c.rpcClient.id)
		} else {
			c.rpcClient.metrics.AsyncRecorder.RecordGaugeSet(MetricKeyClientEnabled, 0, c.rpcClient.id)
		}
	}
}

type MultiRpcClient struct {
	preferHttpForNonSubscriptionRelated bool
	healthCheckInterval                 time.Duration
	healthCheckTimeout                  time.Duration
	allClients                          []*internalRpcClient // Sorted by weight
	stopped                             chan struct{}
}

func DialMulti(cfg *MultiRpcClientConfig) (*MultiRpcClient, error) {
	return DialMultiContext(context.Background(), cfg)
}

func DialMultiContext(ctx context.Context, cfg *MultiRpcClientConfig) (*MultiRpcClient, error) {
	allClients := make([]*internalRpcClient, 0)

	for _, rpcData := range cfg.RpcData {
		rpcClient, err := DialContext(ctx, &RpcClientConfig{
			RpcClientData:        *rpcData,
			PrometheusRegisterer: cfg.PrometheusRegisterer,
		})
		if err != nil {
			return nil, err
		}

		allClients = append(allClients, &internalRpcClient{
			rpcClient: rpcClient,
		})
	}

	slices.SortFunc(allClients, func(a, b *internalRpcClient) int {
		if a.rpcClient.weight > b.rpcClient.weight {
			return -1
		}
		if a.rpcClient.weight < b.rpcClient.weight {
			return 1
		}
		return 0
	})

	var (
		retries = 5
		wg      sync.WaitGroup
		errored uint64
	)

	for _, client := range allClients {
		wg.Add(1)
		go func(client *internalRpcClient) {
			defer wg.Done()

			for range retries {
				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				err := client.rpcClient.CallContext(ctx, nil, "eth_chainId")
				cancel()

				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}

				client.setEnabled(true)
				return
			}

			atomic.AddUint64(&errored, 1)
		}(client)
	}

	wg.Wait()

	if errored == uint64(len(allClients)) {
		return nil, ErrNoAvailableClients
	}

	c := &MultiRpcClient{
		preferHttpForNonSubscriptionRelated: cfg.PreferHttpForNonSubscriptionRelated,
		healthCheckInterval:                 cfg.HealthCheckInterval,
		healthCheckTimeout:                  cfg.HealthCheckTimeout,
		stopped:                             make(chan struct{}),
		allClients:                          allClients,
	}

	go c.healthCheckLoop()
	return c, nil
}

func (c *MultiRpcClient) getRpcClient(subscriptionRelated bool) (*internalRpcClient, error) {
	candidates := make([]*internalRpcClient, 0)

	// Filter out clients that are not capable of handling the request
	for _, client := range c.allClients {
		if !client.enabled.Load() {
			continue
		}

		if subscriptionRelated && !client.rpcClient.SupportsSubscriptions() {
			continue
		}

		candidates = append(candidates, client)
	}

	var (
		candidatesOverLimits = make([]*internalRpcClient, 0)
		nonPreferredClients  = make([]*internalRpcClient, 0)
	)

	// Check for rate limits and user preferences
	for _, client := range candidates {
		if !client.rpcClient.canMakeRequestNow() {
			candidatesOverLimits = append(candidatesOverLimits, client)
			continue
		}

		if c.preferHttpForNonSubscriptionRelated && !subscriptionRelated && client.rpcClient.SupportsSubscriptions() {
			nonPreferredClients = append(nonPreferredClients, client)
			continue
		}

		return client, nil
	}

	// At that point, no ideal client is found

	// Select first non-preferred client in priority because it can process the request immediately
	if len(nonPreferredClients) > 0 {
		return nonPreferredClients[0], nil
	}

	// Last choice: select client with best rate limit score, the request will be queued
	if len(candidatesOverLimits) > 0 {
		slices.SortFunc(candidatesOverLimits, func(a, b *internalRpcClient) int {
			return int(clientRateLimitScore(b.rpcClient) - clientRateLimitScore(a.rpcClient))
		})

		return candidatesOverLimits[0], nil
	}

	return nil, ErrNoAvailableClients
}

func (c *MultiRpcClient) BatchCall(b []rpc.BatchElem) error {
	return c.BatchCallContext(context.Background(), b)
}

func (c *MultiRpcClient) BatchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	var subscriptionRelated bool

	for _, elem := range b {
		if strings.HasSuffix(elem.Method, subscribeMethodSuffix) {
			subscriptionRelated = true
			break
		}
	}

	ic, err := c.getRpcClient(subscriptionRelated)
	if err != nil {
		return err
	}

	if err := ic.rpcClient.BatchCallContext(ctx, b); err != nil {
		if !isErrorRetryable(err) {
			return err
		}

		// Deactivate the client and retry with another
		ic.setEnabled(false)
		return c.BatchCallContext(ctx, b)
	}

	return nil
}

func (c *MultiRpcClient) Call(result any, method string, args ...any) error {
	return c.CallContext(context.Background(), result, method, args...)
}

func (c *MultiRpcClient) CallContext(ctx context.Context, result any, method string, args ...any) error {
	ic, err := c.getRpcClient(strings.HasSuffix(method, subscribeMethodSuffix))
	if err != nil {
		return err
	}

	if err := ic.rpcClient.CallContext(ctx, result, method, args...); err != nil {
		if !isErrorRetryable(err) {
			return err
		}

		// Deactivate the client and retry with another
		ic.setEnabled(false)
		return c.CallContext(ctx, result, method, args...)
	}

	return nil
}

func (c *MultiRpcClient) Close() {
	select {
	case <-c.stopped:
		return
	default:
		close(c.stopped)
	}

	for _, client := range c.allClients {
		client.rpcClient.Close()
	}
}

func (c *MultiRpcClient) EthSubscribe(ctx context.Context, channel any, args ...any) (*rpc.ClientSubscription, error) {
	return c.Subscribe(ctx, "eth", channel, args...)
}

func (c *MultiRpcClient) ShhSubscribe(ctx context.Context, channel any, args ...any) (*rpc.ClientSubscription, error) {
	return c.Subscribe(ctx, "shh", channel, args...)
}

func (c *MultiRpcClient) Subscribe(ctx context.Context, namespace string, channel any, args ...any) (*rpc.ClientSubscription, error) {
	ic, err := c.getRpcClient(true)
	if err != nil {
		return nil, err
	}

	s, err := ic.rpcClient.Subscribe(ctx, namespace, channel, args...)
	if err != nil {
		if !isErrorRetryable(err) {
			return nil, err
		}

		// Deactivate the client and retry with another
		ic.setEnabled(false)
		return c.Subscribe(ctx, namespace, channel, args...)
	}

	return s, nil
}

func (c *MultiRpcClient) Notify(ctx context.Context, method string, args ...any) error {
	ic, err := c.getRpcClient(strings.HasSuffix(method, subscribeMethodSuffix))
	if err != nil {
		return err
	}

	if err := ic.rpcClient.Notify(ctx, method, args...); err != nil {
		if !isErrorRetryable(err) {
			return err
		}

		// Deactivate the client and retry with another
		ic.setEnabled(false)
		return c.Notify(ctx, method, args...)
	}

	return nil
}

func (c *MultiRpcClient) RegisterName(name string, receiver any) error {
	for _, client := range c.allClients {
		if err := client.rpcClient.RegisterName(name, receiver); err != nil {
			return err
		}
	}

	return nil
}

func (c *MultiRpcClient) SetHeader(key string, value string) {
	for _, client := range c.allClients {
		client.rpcClient.SetHeader(key, value)
	}
}

func (c *MultiRpcClient) SupportedModules() (map[string]string, error) {
	// Simply aggregates all results, not reliable
	supportedModules := make(map[string]string)

	for _, client := range c.allClients {
		modules, err := client.rpcClient.SupportedModules()
		if err != nil {
			return nil, err
		}

		for module, version := range modules {
			supportedModules[module] = version
		}
	}

	return supportedModules, nil
}

func (c *MultiRpcClient) SupportsSubscriptions() bool {
	for _, client := range c.allClients {
		if client.rpcClient.SupportsSubscriptions() {
			return true
		}
	}

	return false
}
