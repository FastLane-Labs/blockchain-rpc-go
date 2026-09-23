package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestParallelBroadcastDecodeFailure(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprintf("parallel=%v", parallel), func(t *testing.T) {
			ctx := testContext(t)
			started, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			want := common.HexToHash("0x1234")
			c := testMultiClient(t,
				func(context.Context, testRequest) (any, error) { <-started; return 42, nil },
				func(context.Context, testRequest) (any, error) {
					close(started)
					<-release
					return want.Hex(), nil
				},
			)
			c.parallelCalls = parallel
			var result common.Hash
			returned := make(chan error, 1)
			go func() { returned <- c.CallContext(ctx, &result, "eth_sendRawTransaction", "0xsigned") }()
			receive(t, ctx, started)
			select {
			case err := <-returned:
				t.Fatalf("malformed hash ended the broadcast before the valid response: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			unblock()
			if err := receive(t, ctx, returned); err != nil || result != want {
				t.Fatalf("expected valid acceptance, got hash=%s err=%v", result, err)
			}
		})
	}
}

// A decoder may modify its receiver even when decoding fails.
type broadcastHash struct {
	hash    common.Hash
	decoded bool
}

func (r *broadcastHash) UnmarshalJSON(data []byte) error {
	r.decoded = true
	return json.Unmarshal(data, &r.hash)
}

func TestParallelBroadcastDecodeErrorsAggregated(t *testing.T) {
	c := testMultiClient(t,
		func(context.Context, testRequest) (any, error) { return 42, nil },
		func(context.Context, testRequest) (any, error) { return "0xinvalid", nil },
	)
	result := broadcastHash{hash: common.HexToHash("0x5678")}
	before := result
	err := c.CallContext(testContext(t), &result, "eth_sendRawTransaction", "0xsigned")
	var decodeErr *json.UnmarshalTypeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("missing original decode error: %v", err)
	}
	for _, id := range []string{"provider-0:", "provider-1:"} {
		if !strings.Contains(err.Error(), id) {
			t.Fatalf("missing provider error %q: %v", id, err)
		}
	}
	if result != before {
		t.Fatalf("failed decoders modified the caller's result: %+v", result)
	}
}
