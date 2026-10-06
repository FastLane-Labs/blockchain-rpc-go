package rpc

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/ethereum/go-ethereum/rpc"
)

// Unknown methods keep the original routing. In particular, node-local filters
// and methods with side effects must not be duplicated across providers.
func parallelReadMethod(method string) bool {
	switch method {
	case "eth_call", "eth_estimateGas", "eth_createAccessList", "debug_traceCall",
		"eth_chainId", "eth_blockNumber", "eth_syncing", "eth_gasPrice", "eth_maxPriorityFeePerGas", "eth_feeHistory",
		"eth_getBalance", "eth_getCode", "eth_getStorageAt", "eth_getProof", "eth_getTransactionCount",
		"eth_getBlockByHash", "eth_getBlockByNumber", "eth_getBlockReceipts",
		"eth_getBlockTransactionCountByHash", "eth_getBlockTransactionCountByNumber",
		"eth_getUncleByBlockHashAndIndex", "eth_getUncleByBlockNumberAndIndex",
		"eth_getUncleCountByBlockHash", "eth_getUncleCountByBlockNumber",
		"eth_getTransactionByHash", "eth_getTransactionByBlockHashAndIndex", "eth_getTransactionByBlockNumberAndIndex",
		"eth_getTransactionReceipt", "eth_getLogs", "net_version", "net_peerCount", "net_listening",
		"rpc_modules", "web3_clientVersion", "web3_sha3":
		return true
	}
	return false
}

// Encode arguments before returning ownership to the caller. Queued or slow
// copies must not marshal caller-owned maps/slices after the call returns.
func parallelArgs(args []any) ([]any, error) {
	if len(args) == 0 {
		return args, nil
	}
	encoded := make([]any, len(args))
	for i, arg := range args {
		switch arg.(type) {
		case nil, bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr, float32, float64:
			encoded[i] = arg
			continue
		}
		raw, err := json.Marshal(arg)
		if err != nil {
			return nil, err
		}
		encoded[i] = json.RawMessage(raw)
	}
	return encoded, nil
}

func validateParallelResult(result any) error {
	if result != nil {
		v := reflect.ValueOf(result)
		if v.Kind() != reflect.Pointer || v.IsNil() {
			return &json.InvalidUnmarshalError{Type: v.Type()}
		}
	}
	return nil
}

func (c *MultiRpcClient) callContextConcurrent(ctx context.Context, result any, method string, args ...any) error {
	if result != nil && reflect.TypeOf(result).Kind() != reflect.Pointer {
		return &json.InvalidUnmarshalError{Type: reflect.TypeOf(result)}
	}
	encodedArgs, err := parallelArgs(args)
	if err != nil {
		return err
	}
	needsResult := result != nil
	raw, err := parallelCall(ctx, c, method, func(ctx context.Context, client *RpcClient) (json.RawMessage, error) {
		var raw json.RawMessage
		var receiver any
		if needsResult {
			receiver = &raw
		}
		err := client.CallContext(ctx, receiver, method, encodedArgs...)
		return raw, err
	})
	if err != nil || !needsResult {
		return err
	}
	// The driver has already cancelled losing copies. Only the caller decodes.
	return json.Unmarshal(raw, result)
}

func (c *MultiRpcClient) batchCallContextParallel(ctx context.Context, b []rpc.BatchElem) error {
	// Workers only access this immutable snapshot, never the caller's batch.
	template := make([]rpc.BatchElem, len(b))
	for i, elem := range b {
		args, err := parallelArgs(elem.Args)
		if err != nil {
			return err
		}
		template[i] = rpc.BatchElem{Method: elem.Method, Args: args}
	}
	selected, err := parallelCall(ctx, c, "batch", func(ctx context.Context, client *RpcClient) ([]rpc.BatchElem, error) {
		batch := append([]rpc.BatchElem(nil), template...)
		raw := make([]json.RawMessage, len(batch))
		for i := range batch {
			batch[i].Result = &raw[i]
		}
		err := client.BatchCallContext(ctx, batch)
		return batch, err
	})
	if err != nil {
		return err
	}
	for i := range b {
		b[i].Error = selected[i].Error
		if b[i].Error == nil {
			b[i].Error = json.Unmarshal(*selected[i].Result.(*json.RawMessage), b[i].Result)
		}
	}
	return nil
}
