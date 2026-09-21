# Optional parallel RPC calls

Existing applications need no code changes: the original constructors, config
layout (including positional literals), and default provider selection remain
unchanged. Opt in through the additional constructors in `rpc` or `eth`:

```go
client, err := eth.DialMultiContextWithOptions(ctx, cfg,
    rpc.MultiRpcClientOptions{ParallelCalls: true})
```

Zero-valued options preserve the default behavior.

## Response selection

- Race all healthy, capable providers, regardless of weights or HTTP preference.
  Existing health checks and per-provider rate/concurrency limits still apply.
- Return the first result or non-transport error. All JSON-RPC errors, including
  reverts, are accepted unchanged; RPC codes and messages never affect selection.
  Direct `rpc.Error` / `rpc.DataError` assertions and revert data are preserved.
- Wait for another copy after a timeout, network/HTTP/WebSocket/TLS failure, closed
  connection/client, or rejected local rate-limit wait. Geth's two private,
  untyped connection errors require exact local-message checks, performed only
  after excluding RPC errors. An RPC error saying "request timed out" still wins.
- Batches select the first response without an overall transport failure, then
  decode its elements once. Element errors stay in `BatchElem.Error`. Geth reports
  transport failures for the whole batch, not for individual elements.
- Cancel ordinary losing copies, including queued copies that may never reach
  their provider. `eth_sendRawTransaction` copies and batches containing one
  continue under the caller's context. Single transaction calls include send-only
  providers; batches use read-capable providers.
- Keep the default broadcast semantics for `eth_sendRawTransaction` calls: return
  the first success, and only fail once every provider has failed, aggregating
  their errors with provider IDs. A fast `already known` or `nonce too low`
  rejection never hides a later acceptance. Batches containing a transaction
  still select the first response without an overall transport failure.
- Aggregate all transport failures with provider IDs when no provider answers.
  `errors.Is` / `errors.As` preserve their causes. Caller cancellation/deadline
  stops waiting promptly, with errors collected so far. With one eligible
  provider, delegate directly and return its error unchanged.
- Subscription and notification APIs retain their existing behavior.

Mutable arguments are encoded before starting workers. Workers never write to
caller-owned results; only the chosen response is decoded, on the calling
goroutine. Decode errors end the call without trying another provider, preserving
normal JSON decoder behavior for initialized receivers, maps, and custom codecs.
Valid JSON `null` is accepted; later method-specific checks in `eth` are unchanged.

## Latency and resource use

One eligible provider adds no goroutine, channel, or argument copying. Multiple
providers use one worker each and a buffered response channel. There is no new
explicit mutex, wait group, or wait for losing workers. Error aggregation happens
only on failure. The existing transports, metrics, limiters, and cancellation
still use synchronization.

Fan-out adds scheduling, serialization, traffic, and quota consumption; identical
latency at every load cannot be guaranteed. Cancellation releases local resources
but cannot undo remote work or restore consumed rate-limit tokens. Provider
metrics count individual copies, including canceled copies as errors.

Run the concurrent latency benchmark with `-cpu=4` to compare four application
callers, with metrics enabled and zero or 1 ms simulated provider delay:

```sh
go test ./rpc -run '^$' -bench '^BenchmarkRPCConcurrent$' -benchmem -cpu=4 -benchtime=500ms -count=3
```

These synthetic measurements isolate overhead; they do not predict live provider
latency. Also validate using the consuming application's Go/geth versions.

On 2026-09-16, with the auctioneer's Go 1.24.3/geth 1.15.3 on an Apple M2 Pro,
four-provider zero-delay runs measured median p50 of 5.88 us (default) versus
12.25 us (parallel), and median p99 of 122.5 versus 223.1 us. With 1 ms simulated
provider delay, p50 was 1.282 ms in both modes. Tails varied substantially across
the three runs: p99 ranged from 2.13–3.66 ms (default) and 1.57–4.89 ms (parallel).
These results do not establish identical tail latency under load.
