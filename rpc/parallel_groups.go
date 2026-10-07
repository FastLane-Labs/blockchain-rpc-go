package rpc

// Each weight has its own buffered channel. All groups run concurrently, but
// reading groups in descending weight order prevents an early lower-weight
// response from overriding a pending higher-weight attempt. Within a group,
// the first non-timeout response wins. Workers never wait for the receiver.
type parallelGroup[T any] struct {
	clients   []*internalRpcClient
	responses chan T
}

// clients is a snapshot of allClients, which is already sorted by weight.
func groupParallelClients[T any](clients []*internalRpcClient) []parallelGroup[T] {
	groups := make([]parallelGroup[T], 0, 1)
	for start := 0; start < len(clients); {
		end := start + 1
		for end < len(clients) && clients[end].rpcClient.weight == clients[start].rpcClient.weight {
			end++
		}
		groups = append(groups, parallelGroup[T]{
			clients:   clients[start:end],
			responses: make(chan T, end-start),
		})
		start = end
	}
	return groups
}
