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

Zero-valued options preserve default provider selection. The existing transaction
broadcast implementation is shared by both modes, with argument snapshots,
receiver validation, and error-cause preservation applied consistently.

## Covered methods

Parallel dispatch applies only to the read/simulation allowlist in
`rpc/multi_client_calls.go`: standard Ethereum block, transaction, receipt, log,
state and fee reads; `eth_call`, `eth_estimateGas`, `eth_createAccessList`,
`debug_traceCall`; and the listed `net_*`, `web3_*` and `rpc_modules` reads.
Unknown methods, node-local filters and raw subscriptions retain original routing.
A nonempty batch is parallel only when **every** element is allowlisted. A mixed batch,
including any `eth_sendRawTransaction` element, keeps the original single-provider
selection and sequential retry behavior. It is never concurrently broadcast.
Subscription and notification APIs are unchanged.

`SupportedModules()` retains its original aggregation across providers and fails
if any provider fails. It does not use parallel dispatch or its attempt timeout;
direct `Call` requests for `rpc_modules` do.

## Response selection

- Start attempts on all healthy read providers concurrently. Use the existing
  configured weights: prefer the highest weight, advancing to lower weights only
  after every attempt at a higher weight times out. Equal-weight providers race
  each other; the first non-timeout response at that weight wins. HTTP preference
  does not restrict parallel reads. Existing health checks and per-provider
  rate/concurrency limits still apply; queueing does not reduce a provider's
  priority. Send-only providers are excluded from reads and batches.
- Return the selected provider's result or error unchanged. Every
  JSON-RPC error, including reverts, is accepted unchanged; RPC codes and
  messages never affect selection. Direct `rpc.Error` / `rpc.DataError`
  assertions and revert data are preserved. Transport failures such as a closed
  connection, an HTTP status error, or a malformed response are answers too.
- Discard only timeouts. A copy that failed with Go's `context.DeadlineExceeded`
  ("context deadline exceeded"), or whose local rate-limit wait would exceed the
  deadline, waits for another copy. A network timeout returned at or after the
  attempt's deadline also qualifies, including WebSocket handshake I/O timeouts.
  Errors wrapping an expired context's custom deadline cause also qualify.
  These errors retain their transport cause and match `context.DeadlineExceeded`
  through `errors.Is`. Earlier network timeouts and RPC errors saying "request
  timed out" remain answers; error text alone never enables fallback.
- Read batches select one provider's complete response using the same weight
  ordering, then decode its elements once. Element errors stay in
  `BatchElem.Error`; results from different providers are never combined.
  Batch-level errors are returned without modifying element errors or results.
- Cancel losing read copies as soon as a response is selected, before decoding,
  including queued copies that may never reach their provider.
- Keep the default broadcast semantics for `eth_sendRawTransaction` calls: return
  the first success, and only fail once every provider has failed, aggregating
  their errors with provider IDs. A fast `already known` or `nonce too low`
  rejection or malformed transaction hash never hides a later acceptance.
  Include send-only providers and let all copies continue under the caller's
  context. Broadcasts ignore weights and `ParallelCallTimeout`. A successfully
  decoded acceptance is returned even if the caller has just canceled; cancellation
  cannot undo a transaction already accepted by a provider.
- Aggregate all timeouts with provider IDs when no provider answers.
  `errors.Is` / `errors.As` preserve their causes. Caller deadline expiry selects
  the highest-weight buffered non-timeout response, or returns the deadline error
  with errors collected so far. Explicit caller cancellation ends selection
  immediately, even with buffered responses. The same driver,
  receiver validation, argument snapshot and timeout normalization apply with one
  or many eligible providers. Nil contexts are treated as `context.Background()`.

Mutable arguments are encoded before starting workers. Workers never write to
caller-owned results. Ordinary calls and batches decode only the chosen response,
on the calling goroutine. Their decode errors end selection without trying
another provider, preserving normal JSON decoder behavior for initialized
receivers, maps, and custom codecs.
For read calls, a typed-nil receiver fails only when decoding a successful response;
RPC errors are returned unchanged. Non-pointer receivers are rejected before dispatch.
Valid JSON `null` is accepted; later method-specific checks in `eth` are unchanged.

Single `eth_sendRawTransaction` calls in either mode decode every response into a
private, zero-valued receiver of the caller's result type. A decode failure counts as that provider's failure; only a successfully
decoded result is copied to the caller. Custom broadcast decoders must work on
zero-valued receivers and may run concurrently, even after the call returns.

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

For parallel reads, caller deadline expiry stops waiting and selects the
highest-weight non-timeout response already buffered. If none is available, the
deadline error is returned. Explicit caller cancellation ends selection even
when a fallback response is buffered. A shorter caller deadline caps every
attempt. The attempt cap applies even with one eligible provider and to calls
made through `Call()` using a background context. Long
reads may require a larger cap or a client without parallel reads enabled; the
option is per client, not per call. Methods outside the allowlist and mixed
batches use the caller's context without this cap.

## Latency and resource use

Parallel reads use one worker per eligible provider and one buffered response
channel per weight group, including a group of one for a single provider. The
caller does not wait for losing read workers. Transaction broadcasting retains
its existing workers, response channel and wait group.

With N eligible providers, each covered logical call can make N provider requests.
Fan-out adds scheduling, serialization, traffic, and quota consumption. A fast
lower-weight provider does not reduce latency while a higher-weight attempt is
pending: the benefit is a buffered answer after a timeout. Limiter queueing counts
against each attempt's cap. A saturated high-weight provider can delay selection;
if every provider rejects its wait, the operation fails rather than queueing for
the caller's entire budget. Non-timeout transport errors are returned without
retry or provider demotion, unlike default selection.

Cancellation releases local resources but cannot undo remote work or restore
consumed rate-limit tokens. Canceling HTTP/1.1 requests can close their keep-alive
connections, adding TCP/TLS handshakes on later calls; HTTP/2 can cancel individual
streams. Immediate fan-out intentionally retains this cost. Check provider quotas,
connection reuse and subscription traffic before enabling on a shared client.

Provider call counters count actual copies, not logical operations. Errors and
latency samples caused by coordinator cancellation after selection are excluded.
Caller cancellation, attempt expiry and real provider failures retain existing
metric handling; batch error counters count element errors. Logical-call latency,
fallback frequency and selected weight need instrumentation in the consuming app.

Run the concurrent latency benchmark with `-cpu=4` to compare four application
callers, with metrics enabled and zero or 1 ms simulated provider delay:

```sh
go test ./rpc -run '^$' -bench '^BenchmarkRPCConcurrent$' -benchmem -cpu=4 -benchtime=500ms -count=3
```

These synthetic measurements isolate overhead; they do not predict live provider
latency. Also validate using the consuming application's Go/geth versions.

Use different provider weights when measuring preference and timeout behavior;
equal-weight benchmarks measure the race within one weight group.
