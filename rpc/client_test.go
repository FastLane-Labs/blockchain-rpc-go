package rpc

import (
	"testing"

	"github.com/ethereum/go-ethereum/rpc"
)

func TestEthClientCompatibility(t *testing.T) {
	// Ensure the rpc client is compatible with the go-ethereum rpc client
	gethRpcClient, err := rpc.Dial("https://eth.llamarpc.com")
	if err != nil {
		t.Fatalf("Failed to dial eth client: %v", err)
	}
	anyFunc(gethRpcClient)
}

func anyFunc(_ IRpcClient) {}
