# Optional parallel RPC calls

Existing callers do not need to change their code. `MultiRpcClientConfig`,
`DialMulti`, and `DialMultiContext` retain their pre-feature layouts/signatures
and default behavior in both `rpc` and `eth`. This includes positional config
literals and variables holding the original constructor function types.

Opt in using the additional constructor:

```go
client, err := eth.DialMultiContextWithOptions(ctx, cfg,
    rpc.MultiRpcClientOptions{ParallelCalls: true})
```

Passing zero-valued options also preserves the default behavior.

## Behavior

- Calls race all healthy providers capable of serving the request. Weights and
  HTTP preference do not exclude eligible providers from the race. The existing
  health checks and per-provider limits still apply.
- The first result or non-transport error wins. Every JSON-RPC error is accepted
  unchanged, including reverts, regardless of its code or message. No other
  provider has to finish. Ordinary losing copies are canceled, including copies
  queued behind rate/concurrency limits that may never reach their provider.
- Typed network errors, HTTP status failures, EOF/closed connections, closed
  clients, and provider context timeouts/cancellation allow other copies to
  continue. A JSON-RPC error saying "request timed out" is still an accepted
  answer. No RPC codes or error message text are used for classification.
- `eth_sendRawTransaction` copies continue after the first accepted response, subject to
  the caller's context. These calls include send-only providers. Batches continue
  to use read-capable providers; batches containing a transaction broadcast are
  also exempt from cancellation on an accepted response.
- Batch elements accept results or non-transport errors independently. Return
  once every element has an outcome, or all providers have responded. Element
  errors remain in `BatchElem.Error`; an overall non-transport failure ends the
  batch immediately, and exhausted transport failures are aggregated. A batch
  is not an atomic snapshot.
- If every provider has a transport failure, return all failures with provider
  IDs. `errors.Is` and `errors.As` retain access to their causes. Accepted RPC
  errors are returned directly, preserving direct interface assertions, codes,
  and revert data. Caller cancellation/deadline stops the whole race promptly,
  with errors observed so far; it does not wait for pending provider errors.
- Subscription and notification APIs keep their existing selection behavior.

Selection happens before decoding the result into the caller's destination.
A valid JSON `null` can win; method-specific checks performed later by `eth` (for example, converting
a missing transaction into `ethereum.NotFound`) are outside this race.

Workers never access the caller's result and snapshot mutable arguments before
starting. Only the selected response is decoded, once, on the calling goroutine.
Decoding errors are returned immediately; they do not select another provider.
This retains the usual
`encoding/json` semantics for initialized receivers, maps, and custom decoders.
As with a normal RPC call, unsuccessful decoding can partially update a result.
There is no rollback of custom decoder side effects, but a second provider can
never decode into that same destination after a failed decode.

Cancellation releases local resources; it cannot undo processing already done
by a provider. WebSocket JSON-RPC has no general remote cancellation mechanism.
Existing provider metrics count individual copies, including cancellations as
errors; they do not represent the number of failed application calls.

## Compatibility and implementation review

The initial implementation added a field to `MultiRpcClientConfig`. That broke
positional literals even though keyed literals remained compatible. The field
has been removed in favor of additive constructors/options. Existing constructor
function types are unchanged, and the legacy transaction-broadcast implementation
is retained for callers that do not opt in.

The review also corrected:

- Workers marshaling mutable caller arguments after the call returned.
- Selecting responses before decoding, preserving initialized/custom decoder state.
- Flattened error strings discarding RPC error types and revert data.
- Canceled rate-limit/concurrency waits leaking the private queue counter.
- Unnecessary goroutines, channels, and copying with only one eligible provider.

The single-provider path delegates directly. Multiple providers use a small
stack-backed candidate slice, one buffered response channel, and one goroutine
per provider. There is no new explicit mutex, wait group, closer goroutine, or
wait for losing calls. Error formatting is deferred until total failure.
Channels, context cancellation, the existing transport, metrics, and rate
limiters still perform synchronization; the client is not lock-free.

The default success path gains only the opt-in branch. The queue-counter repair
adds accounting on limiter/semaphore failure, with no added success-path work.

## Latency limits and measurements

Fan-out cannot guarantee identical latency or throughput at every load. It adds
goroutine scheduling, argument snapshots, responses, and provider traffic. If
copies reach every endpoint, two providers each limited to 50 requests/second
can sustain about 50 fully replicated application calls/second, rather than the
roughly 100 possible when distributing requests. Health checks also use quota.
Canceling queued copies helps, but cannot restore tokens already consumed.

Measurements on 2026-09-16 used Go 1.22.2, darwin/arm64, `GOMAXPROCS=4`, and an
in-memory HTTP transport returning a small JSON result. Prometheus metrics were
enabled for the numbers below, as in the auctioneer. No live provider calls were
made. The measurements below are historical, from revision `1717c55`, before
the change to accepting the first non-transport response. They are synthetic
overhead measurements, not production latency claims for the current code.

Sequential calls, zero transport delay, median of three 200 ms runs:

| Eligible providers | Default | Initial parallel implementation | Reviewed parallel implementation |
| --- | ---: | ---: | ---: |
| 1 | 4.84 us | 15.30 us | 4.94 us |
| 2 | 4.90 us | 14.50 us | 16.99 us |
| 4 | 4.93 us | 18.07 us | 16.18 us |

The one-provider regression is removed. Multi-provider calls still cost about
11–12 us more than default in this benchmark. Cancellation and safe input
ownership are not free; the two-provider result is slower than the initial
implementation. Default allocations remain unchanged (59/60/61 allocations per
call for 1/2/4 providers). With a 1 ms synthetic provider delay, sequential call
times were about 1.17–1.23 ms for default and 1.16–1.19 ms for parallel; timer
and scheduler noise prevent treating these as proof of equal latency.

Reproduce the sequential benchmark:

```sh
go test ./rpc -run '^$' -bench BenchmarkRPCCall -benchmem -benchtime=200ms -count=3 -cpu=4
```

The concurrent benchmark fixes four in-flight application calls and reports
individual-call percentiles separately from aggregate throughput (`ns/op`).
Run it without other builds or benchmarks competing for CPU:

```sh
go test ./rpc -run '^$' -bench BenchmarkRPCConcurrent -benchmem -benchtime=1s -count=5 -cpu=4
```

Concurrent results below are medians of five 1 s runs, in microseconds:

| Provider delay | Providers | Default p50 / p95 / p99 | Reviewed parallel p50 / p95 / p99 |
| --- | ---: | ---: | ---: |
| 0 | 2 | 6.13 / 26.8 / 103 | 11.7 / 137 / 549 |
| 0 | 4 | 6.21 / 34.6 / 122 | 12.9 / 48.7 / 1687 |
| 1 ms | 2 | 1157 / 1285 / 2378 | 1167 / 1289 / 2039 |
| 1 ms | 4 | 1157 / 1266 / 2396 | 1162 / 1305 / 2012 |

The saturated zero-delay case exposes a substantial tail-latency cost, despite
lower parallel median latency than the initial implementation. Its initial
parallel p99 values were 245 us (two providers) and 426 us (four providers).
The revised implementation does not satisfy an absolute no-slowdown guarantee.
The 1 ms cases are close at the median, but are not a substitute for measuring
the auctioneer's actual load, payloads, quotas, and providers.

After changing selection to the first non-transport response, the same concurrent
benchmark was run with the auctioneer's Go 1.24.3 and geth 1.15.3 dependencies.
Medians of three 500 ms runs follow, in microseconds. These used four callers,
metrics enabled, and no concurrent builds/tests or live providers.

| Provider delay | Providers | Default p50 / p95 / p99 | Parallel p50 / p95 / p99 |
| --- | ---: | ---: | ---: |
| 0 | 2 | 5.88 / 36.3 / 139 | 11.5 / 39.3 / 187 |
| 0 | 4 | 5.96 / 35.1 / 127 | 12.2 / 63.2 / 229 |
| 1 ms | 2 | 1157 / 1243 / 1726 | 1167 / 1291 / 2975 |
| 1 ms | 4 | 1161 / 1274 / 1661 | 1160 / 1302 / 2424 |

Workers now check cancellation before entering the underlying RPC client, so
copies that have already lost can skip serialization and transport work. This
particularly reduces work in the zero-delay benchmark. The 1 ms medians remain
close; tail measurements are variable and higher for parallel calls in these
runs. These results do not establish a production latency guarantee.

## Validation

- Full package race tests: `go test -race ./... -timeout 60s`.
- Ten repetitions of concurrency/cancellation tests under the race detector.
- Library race tests with the auctioneer's selected dependency versions.
- Compile checks for original positional config and constructor function types.
- An unmodified `atlas-bundler` built against the revised library using a
  temporary module replacement; its source and module files were unchanged.
- The auctioneer's complete test suite passed against the revised library.

Tests cover early results and RPC errors with blocked losers, all-provider transport failures, ordinary
and queued cancellation, continued transaction broadcasts, ownership of inputs
and results after return, initialized decoders, single decoding, error types,
batch element/transport failures, provider eligibility, and default opt-out.
