package rpc

import (
	"context"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

func TestApplyRateLimit(t *testing.T) {
	tests := []struct {
		name        string
		client      *RpcClient
		ctx         context.Context
		expectError bool
		expectDone  bool
	}{
		{
			name: "no rate limit or semaphore",
			client: &RpcClient{
				lim: nil,
				sem: nil,
			},
			ctx:         context.Background(),
			expectError: false,
			expectDone:  true,
		},
		{
			name: "with rate limit only",
			client: &RpcClient{
				lim: rate.NewLimiter(rate.Limit(10), 1),
				sem: nil,
			},
			ctx:         context.Background(),
			expectError: false,
			expectDone:  true,
		},
		{
			name: "with semaphore only",
			client: &RpcClient{
				lim: nil,
				sem: semaphore.NewWeighted(5),
			},
			ctx:         context.Background(),
			expectError: false,
			expectDone:  true,
		},
		{
			name: "with both rate limit and semaphore",
			client: &RpcClient{
				lim: rate.NewLimiter(rate.Limit(10), 1),
				sem: semaphore.NewWeighted(5),
			},
			ctx:         context.Background(),
			expectError: false,
			expectDone:  true,
		},
		{
			name: "with nil context",
			client: &RpcClient{
				lim: rate.NewLimiter(rate.Limit(10), 1),
				sem: semaphore.NewWeighted(5),
			},
			ctx:         nil,
			expectError: false,
			expectDone:  true,
		},
		{
			name: "with cancelled context",
			client: &RpcClient{
				lim: rate.NewLimiter(rate.Limit(10), 1),
				sem: semaphore.NewWeighted(5),
			},
			ctx:         createCancelledContext(),
			expectError: true,
			expectDone:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done, err := tt.client.applyRateLimit(tt.ctx)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tt.expectDone && done == nil {
				t.Errorf("expected done function but got nil")
			}
			if !tt.expectDone && done != nil {
				t.Errorf("expected nil done function but got one")
			}

			// If we got a done function, call it to release resources
			if done != nil {
				done()
			}
		})
	}
}

func TestAdvanceLimiter(t *testing.T) {
	tests := []struct {
		name        string
		client      *RpcClient
		ctx         context.Context
		expectError bool
	}{
		{
			name: "no rate limiter",
			client: &RpcClient{
				lim: nil,
			},
			ctx:         context.Background(),
			expectError: false,
		},
		{
			name: "with rate limiter - should pass",
			client: &RpcClient{
				lim: rate.NewLimiter(rate.Limit(100), 10), // High rate limit
			},
			ctx:         context.Background(),
			expectError: false,
		},
		{
			name: "with cancelled context",
			client: &RpcClient{
				lim: rate.NewLimiter(rate.Limit(1), 1),
			},
			ctx:         createCancelledContext(),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.client.advanceLimiter(tt.ctx)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestAcquireSemaphore(t *testing.T) {
	tests := []struct {
		name        string
		client      *RpcClient
		ctx         context.Context
		weight      int64
		expectError bool
	}{
		{
			name: "no semaphore",
			client: &RpcClient{
				sem: nil,
			},
			ctx:         context.Background(),
			weight:      1,
			expectError: false,
		},
		{
			name: "acquire within capacity",
			client: &RpcClient{
				sem: semaphore.NewWeighted(10),
			},
			ctx:         context.Background(),
			weight:      5,
			expectError: false,
		},
		{
			name: "acquire with zero weight",
			client: &RpcClient{
				sem: semaphore.NewWeighted(10),
			},
			ctx:         context.Background(),
			weight:      0,
			expectError: false,
		},
		{
			name: "acquire with cancelled context",
			client: &RpcClient{
				sem: semaphore.NewWeighted(10),
			},
			ctx:         createCancelledContext(),
			weight:      1,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.client.acquireSemaphore(tt.ctx, tt.weight)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// If acquisition was successful and we have a semaphore, release it
			if err == nil && tt.client.sem != nil {
				tt.client.releaseSemaphore(tt.weight)
			}
		})
	}
}

func TestReleaseSemaphore(t *testing.T) {
	tests := []struct {
		name   string
		client *RpcClient
		weight int64
	}{
		{
			name: "no semaphore",
			client: &RpcClient{
				sem: nil,
			},
			weight: 1,
		},
		{
			name: "release with semaphore",
			client: &RpcClient{
				sem: semaphore.NewWeighted(10),
			},
			weight: 1,
		},
		{
			name: "release zero weight",
			client: &RpcClient{
				sem: semaphore.NewWeighted(10),
			},
			weight: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First acquire if we have a semaphore to make release meaningful
			if tt.client.sem != nil && tt.weight > 0 {
				err := tt.client.acquireSemaphore(context.Background(), tt.weight)
				if err != nil {
					t.Fatalf("failed to acquire semaphore for test setup: %v", err)
				}
			}

			// This should not panic or cause issues
			tt.client.releaseSemaphore(tt.weight)
		})
	}
}

func TestApplyRateLimitIntegration(t *testing.T) {
	t.Run("rate limiting works", func(t *testing.T) {
		// Create a client with very low rate limit
		client := &RpcClient{
			lim: rate.NewLimiter(rate.Limit(1), 1), // 1 request per second, burst of 1
			sem: nil,
		}

		ctx := context.Background()

		// First call should succeed immediately
		done1, err := client.applyRateLimit(ctx)
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}
		if done1 == nil {
			t.Fatal("expected done function")
		}
		done1()

		// Second call should be blocked/delayed due to rate limit
		start := time.Now()
		done2, err := client.applyRateLimit(ctx)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("second call failed: %v", err)
		}
		if done2 == nil {
			t.Fatal("expected done function")
		}
		done2()

		// Should have been delayed for close to 1 second
		if elapsed < 500*time.Millisecond {
			t.Errorf("expected delay due to rate limiting, but call completed in %v", elapsed)
		}
	})

	t.Run("semaphore limiting works", func(t *testing.T) {
		// Create a client with semaphore limit of 1
		client := &RpcClient{
			lim: nil,
			sem: semaphore.NewWeighted(1),
		}

		ctx := context.Background()

		// First call should succeed
		done1, err := client.applyRateLimit(ctx)
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}
		if done1 == nil {
			t.Fatal("expected done function")
		}

		// Second call should block until first is done
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		_, err = client.applyRateLimit(ctxWithTimeout)
		if err == nil {
			t.Error("expected timeout error due to semaphore limit")
		}

		// Release first call
		done1()

		// Now second call should succeed
		done2, err := client.applyRateLimit(ctx)
		if err != nil {
			t.Fatalf("second call after release failed: %v", err)
		}
		if done2 == nil {
			t.Fatal("expected done function")
		}
		done2()
	})
}

// Helper function to create a cancelled context
func createCancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// Benchmark tests
func BenchmarkApplyRateLimit(b *testing.B) {
	client := &RpcClient{
		lim: rate.NewLimiter(rate.Limit(1000), 100),
		sem: semaphore.NewWeighted(100),
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		done, err := client.applyRateLimit(ctx)
		if err != nil {
			b.Fatalf("applyRateLimit failed: %v", err)
		}
		done()
	}
}

func BenchmarkApplyRateLimitNoLimits(b *testing.B) {
	client := &RpcClient{
		lim: nil,
		sem: nil,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		done, err := client.applyRateLimit(ctx)
		if err != nil {
			b.Fatalf("applyRateLimit failed: %v", err)
		}
		done()
	}
}
