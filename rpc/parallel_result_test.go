package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

type isolatedValue struct{ Value int }

type isolatedResult struct {
	Count   int
	Marker  string
	Map     map[string]int
	Slice   []isolatedValue
	Pointer *isolatedValue
	decoded chan struct{}
}

func (r *isolatedResult) UnmarshalJSON(data []byte) error {
	type plain isolatedResult
	err := json.Unmarshal(data, (*plain)(r))
	if err != nil {
		r.decoded <- struct{}{}
	}
	return err
}

func TestParallelDecodeIsolation(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, allFail := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%v/allFail=%v", batch, allFail), func(t *testing.T) {
				ctx := testContext(t)
				decoded := make(chan struct{}, 2)
				bad := json.RawMessage(`{"Count":"invalid","Marker":"rejected","Map":{"bad":99},"Slice":[{"Value":99}],"Pointer":{"Value":99}}`)
				c := testMultiClient(t,
					func(context.Context, testRequest) (any, error) { return bad, nil },
					func(ctx context.Context, _ testRequest) (any, error) {
						select {
						case <-decoded:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
						if allFail {
							return bad, nil
						}
						return json.RawMessage(`{"Count":7}`), nil
					},
				)
				originalMap := map[string]int{"kept": 1}
				originalSlice := []isolatedValue{{Value: 1}}
				originalPointer := &isolatedValue{Value: 1}
				result := isolatedResult{Marker: "original", Map: originalMap, Slice: originalSlice, Pointer: originalPointer, decoded: decoded}
				var err error
				if batch {
					b := []gethrpc.BatchElem{{Method: "test_value", Result: &result}}
					if err = c.BatchCallContext(ctx, b); err == nil {
						err = b[0].Error
					}
				} else {
					err = c.CallContext(ctx, &result, "test_value")
				}
				if (err != nil) != allFail {
					t.Fatalf("unexpected error: %v", err)
				}
				wantCount := 7
				if allFail {
					wantCount = 0
					for _, id := range []string{"provider-0", "provider-1"} {
						if !strings.Contains(err.Error(), id) {
							t.Fatalf("missing error from %s: %v", id, err)
						}
					}
				}
				if result.Count != wantCount || result.Marker != "original" || len(result.Map) != 1 || result.Map["kept"] != 1 || result.Slice[0].Value != 1 || result.Pointer.Value != 1 {
					t.Fatalf("rejected response polluted result: %+v", result)
				}
				if len(originalMap) != 1 || originalSlice[0].Value != 1 || originalPointer.Value != 1 {
					t.Fatal("rejected response mutated an aliased map, slice, or pointer")
				}
			})
		}
	}
}

func TestParallelResultPreservesInitializedGraph(t *testing.T) {
	type node struct {
		Value int
		Next  *node
	}
	n := &node{Value: 1}
	n.Next = n
	result := struct {
		First, Second *node
		Any           any
	}{n, n, map[string]int{"kept": 2}}
	if err := decodeParallelResult([]byte(`{"First":{"Value":3},"Any":{"new":4}}`), &result); err != nil {
		t.Fatal(err)
	}
	if result.First != result.Second || result.First.Next != result.First || result.First.Value != 3 || n.Value != 1 {
		t.Fatalf("initialized graph was not isolated: %+v", result)
	}
}

type privateResult struct{ state *int }

func (r *privateResult) UnmarshalJSON([]byte) error {
	*r.state = 99
	return errors.New("rejected")
}

func TestParallelResultRejectsUnsafePrivateState(t *testing.T) {
	state := 1
	result := privateResult{state: &state}
	err := decodeParallelResult([]byte(`{}`), &result)
	if err == nil || !strings.Contains(err.Error(), "private reference state") || state != 1 {
		t.Fatalf("unsafe custom decoder executed: state=%d err=%v", state, err)
	}
}
