package rpc

import (
	"context"
	"time"
)

const (
	defaultHealthCheckInterval = 1 * time.Second
	defaultHealthCheckTimeout  = 1 * time.Second
)

func (c *MultiRpcClient) healthCheckLoop() {
	// Running each client in a separate goroutine so they don't block each other
	for _, client := range c.allClients {
		go c.healthCheckLoopSingle(client)
	}
}

func (c *MultiRpcClient) healthCheckLoopSingle(client *internalRpcClient) {
	interval := defaultHealthCheckInterval
	if c.healthCheckInterval > 0 {
		interval = c.healthCheckInterval
	}

	timeout := defaultHealthCheckTimeout
	if c.healthCheckTimeout > 0 {
		timeout = c.healthCheckTimeout
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := client.rpcClient.CallContext(ctx, nil, "eth_chainId")
		cancel()

		client.enabled.Store(err == nil)

		select {
		case <-c.stopped:
			return
		case <-ticker.C:
		}
	}
}
