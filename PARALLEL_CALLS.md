# Optional parallel RPC calls with weight preference

Existing applications need no code changes: the original constructors, config
layout (including positional literals), and default provider selection remain
unchanged. Opt in through the additional constructors in `rpc` or `eth`:

```go
client, err := eth.DialMultiContextWithOptions(ctx, cfg,
    rpc.MultiRpcClientOptions{
        ParallelCalls:       true,
        ParallelCallTimeout: 2 * time.Second,
    })
```

Zero-valued options preserve the default behavior.

## Response selection

- Start attempts on all healthy, capable providers concurrently. Use the existing
  configured weights: prefer the highest weight, advancing to lower weights only
  after every attempt at a higher weight times out. Equal-weight providers race
  each other; the first non-timeout response at that weight wins. HTTP preference
  does not restrict this mode. Existing health checks and per-provider
  rate/concurrency limits still apply.
- Return the selected provider's result or error unchanged. Every
  JSON-RPC error, including reverts, is accepted unchanged; RPC codes and
  messages never affect selection. Direct `rpc.Error` / `rpc.DataError`
  assertions and revert data are preserved. Transport failures such as a closed
  connection, an HTTP status error, or a malformed response are answers too.
- Discard only timeouts. A copy that failed with Go's `context.DeadlineExceeded`
  ("context deadline exceeded"), or whose local rate-limit wait would exceed the
  deadline, waits for another copy. A network timeout returned at or after the
  attempt's deadline also qualifies, including WebSocket handshake I/O timeouts.
  These errors retain their transport cause and match `context.DeadlineExceeded`
  through `errors.Is`. Earlier network timeouts and RPC errors saying "request
  timed out" remain answers; error text alone never enables fallback.
- Read batches select one provider's complete response using the same weight
  ordering, then decode its elements once. Element errors stay in
  `BatchElem.Error`; results from different providers are never combined.
- Cancel ordinary losing copies, including queued copies that may never reach
  their provider. `eth_sendRawTransaction` copies and batches containing one
  continue under the caller's context. Single transaction calls include send-only
  providers; batches use read-capable providers.
- Keep the default broadcast semantics for `eth_sendRawTransaction` calls: return
  the first success, and only fail once every provider has failed, aggregating
  their errors with provider IDs. A fast `already known` or `nonce too low`
  rejection or malformed transaction hash never hides a later acceptance.
  Batches containing a transaction still select the first non-timeout batch,
  regardless of weights. Both broadcast paths use only the caller's deadline.
- Aggregate all timeouts with provider IDs when no provider answers.
  `errors.Is` / `errors.As` preserve their causes. Caller cancellation/deadline
  stops waiting promptly, with errors collected so far. With one eligible
  provider, apply its attempt timeout, delegate directly and return its error
  unchanged.
- Subscription and notification APIs retain their existing behavior.

Mutable arguments are encoded before starting workers. Workers never write to
caller-owned results. Ordinary calls and batches decode only the chosen response,
on the calling goroutine. Their decode errors end selection without trying
another provider, preserving normal JSON decoder behavior for initialized
receivers, maps, and custom codecs.
Valid JSON `null` is accepted; later method-specific checks in `eth` are unchanged.

Like the default transaction broadcast, multi-provider `eth_sendRawTransaction`
calls decode each response into a private, zero-valued receiver of the caller's
result type. A decode failure counts as that provider's failure; only a successfully
decoded result is copied to the caller. Custom broadcast decoders must work on
zero-valued receivers and may run concurrently. Batches containing transactions
continue to select a whole batch before decoding its elements.

## Deadlines

`ParallelCallTimeout` defaults to 2 seconds when zero; negative values are
rejected. Each ordinary provider attempt has an independent child context with
the same absolute deadline, set when concurrent dispatch begins. The timeout
includes rate/concurrency queueing. Moving to a lower weight does not launch a
retry or reset its timeout window.

The coordinator retains the caller's context. For example, a 3-second caller
budget with a 2-second attempt timeout lets the highest-weight attempt expire
while preserving a lower-weight response that completed earlier. With weights
100, 50 and 10, if weights 50 and 10 finish at 200 ms and 100 ms respectively,
the call still waits for weight 100. If that attempt times out at 2 seconds,
weight 50's buffered response is selected.

Caller cancellation or deadline expiry always ends the entire operation, even
when a fallback response is buffered. Give the caller more time than the attempt
timeout to permit fallback after an attempt expires. A shorter caller deadline
caps every attempt. Transaction broadcasts and batches containing transactions
are exempt from `ParallelCallTimeout` and keep using the caller's context.

## Latency and resource use

One eligible provider adds a deadline context but no goroutine, channel, or
argument copying. Multiple providers use one worker per provider and a buffered
response channel per weight group. Broadcasts use one response channel. There is
no new explicit mutex, wait group, or wait for losing workers. Error aggregation happens
only on failure. The existing transports, metrics, limiters, and cancellation
still use synchronization.

With N eligible providers, each logical call can make N provider requests.
Fan-out adds scheduling, serialization, traffic, and quota consumption. A fast
lower-weight provider does not reduce latency while a higher-weight attempt is
pending. Cancellation releases local resources but cannot undo remote work or
restore consumed rate-limit tokens. Provider
metrics count individual copies, including canceled copies as errors.

Run the concurrent latency benchmark with `-cpu=4` to compare four application
callers, with metrics enabled and zero or 1 ms simulated provider delay:

```sh
go test ./rpc -run '^$' -bench '^BenchmarkRPCConcurrent$' -benchmem -cpu=4 -benchtime=500ms -count=3
```

These synthetic measurements isolate overhead; they do not predict live provider
latency. Also validate using the consuming application's Go/geth versions.

Use different provider weights when measuring preference and timeout behavior;
equal-weight benchmarks measure the race within one weight group.
