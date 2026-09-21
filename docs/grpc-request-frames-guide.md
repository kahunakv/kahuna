# Kahuna gRPC request frames guide

This guide explains how the Kahuna client sends several key/value requests in one gRPC stream message,
what that does to throughput and latency, and the rules a maintainer must preserve. It is written for two
audiences:

- **Developers using Kahuna** who want to know what the feature costs, what it gains, and how to turn it
  off.
- **Developers maintaining Kahuna** who need the wire contract, the negotiation rule, and the limits in
  one place.

The measured numbers are in [section 5](#5-measured-results). The earlier sections explain what was
measured.

---

## 1. The problem frames solve

The .NET client multiplexes every key/value request to a node over one long-lived bidirectional gRPC
stream per connection. Before frames, each request was its own stream message, and so was each response.

A stream message has a fixed cost on both ends that does not depend on what the request does: a gRPC
message frame, an HTTP/2 DATA frame, the transport's pipe locks, the protobuf envelope, and five to six
thread hops. For a small request that fixed cost is most of the total. A script that touches no key at
all (`RETURN 1`) cost about 40 µs of node CPU and 25 µs of client CPU per request, and topped out near
105k requests per second on an 8-core machine, however little work the request asked for.

A **frame** is one stream message that carries several independent requests, or several independent
responses. The fixed cost is paid once per frame instead of once per request.

## 2. What a frame is, and is not

- Each item in a frame is a complete single request with its own type and its own request id, exactly as
  it would be sent alone. The node runs every item through the same code path as a lone request.
- Items are independent. One item that fails, is refused, or is cancelled does not affect the others.
  Each item is answered by its own id.
- A frame never waits. The client packs only the requests that are **already waiting** when it writes;
  the node packs only the responses that are **already ready**. No request is held back to make a frame
  fuller, so there is no added latency at low load.
- A request that is alone travels as the plain single message it always was. A quiet connection is
  byte-identical to a connection without frames.
- Frames cover the key/value stream: get, set, delete, extend, exists, set-many, delete-many, scripts,
  bucket and prefix reads, and interactive transaction start/commit/rollback. The lock stream and the
  sequence calls still carry one request per message.
- A frame does not make its items atomic, and does not order them. It is a transport optimization only.
  Use a script or a transaction for atomicity.

## 3. Negotiation: announce, never probe

Frames are used only between peers that both support them, decided **per stream**.

1. When a batch stream opens, a node that reads request frames says so in the response header
   `kahuna-batch-frames` (value `1`).
2. The client sends a frame on a stream only after it has seen that header on that same stream. It never
   waits for the header: until the header arrives, requests travel one per message.
3. The node sends response frames only on a stream that already carried a request frame. A client that
   sent a frame has proved it can read one.

This is deliberately not a probe. A node built before frames does not answer a batch type it does not
know, so a frame sent to it would leave every request inside unanswered until its deadline. Silence is
never read as support.

Consequences:

- A new client works with an old node, and an old client works with a new node. Neither sees a frame.
- A client with endpoints of mixed versions uses frames with exactly the nodes that announced them. This
  makes a rolling upgrade safe in either order.

## 4. Limits and configuration

| Limit | Value | Behaviour at the limit |
|---|---|---|
| Items per frame | 256 | The client starts another frame. A node that receives more runs the first 256 and refuses the rest with the retryable `MustRetry` answer of each item's type, so nothing is lost. |
| Bytes per frame | 1 MiB of serialized items | The client starts another frame. A request or response that is larger than this on its own travels as a plain single message. |

The byte budget sits far below the 4 MB default gRPC message limit on purpose. A message over the
transport's limit is rejected before the receiver can parse it, which resets the stream that every other
request shares, so the client must never build one.

**Client option.** `KahunaOptions.GrpcRequestFrames` (default `true`). Set it to `false` to send every
request as its own message even to a node that announced frames. There is no node-side switch: a node
only answers with frames to a client that used them first.

**Benchmark switch.** `kahuna-bench --no-request-frames` sets that option, so one build of the tool can
run both arms of an A/B comparison. See the [benchmarking guide](benchmarking-guide.md).

**Failure behaviour.** A frame whose write fails may or may not have reached the node, and that is true
of every request in it. The batcher therefore never resends a frame: every request in it fails with the
transport error, and each operation's own retry contract decides what happens next. This is the same
rule that already applied to a batch of several single messages.

## 5. Measured results

### Setup

- One 8-core Apple Silicon Mac (4 performance + 4 efficiency cores). **The node and the benchmark client
  ran on the same machine and competed for the same cores**, and the machine was CPU-saturated in every
  run at concurrency 64. Read the numbers as a comparison between the two arms, not as the capacity of a
  deployment.
- Standalone single node, Release build, `--storage memory --wal-storage memory`, one partition.
- Cleartext HTTP/2 (`--grpc-cleartext-ports`), 2 gRPC connections, `--durability ephemeral`,
  `--key-space 10000`, 128-byte values, closed loop.
- `kahuna-bench` built from the same source for both arms; the only difference is `--no-request-frames`.
- Measured 2026-09-21. Run-to-run noise on this machine is roughly ±15%, so treat smaller differences
  as no difference.

### Throughput at concurrency 64

| Workload | One request per message | Frames | Change |
|---|---:|---:|---:|
| Script `RETURN 1` (no storage work) | 104k–107k req/s | 416k–438k req/s | 4.0× |
| `rate-limit` (one counter script per request) | 83.6k–83.7k req/s | 136k–140k req/s | +65% |
| `get` | 103.9k req/s | 290.6k req/s | 2.8× |
| `set` | 84.2k req/s | 218.3k req/s | 2.6× |
| `mixed` | 103.8k req/s | 233.5k req/s | 2.2× |
| `txn` (interactive transaction, 2PC) | 8.9k req/s | 18.7k req/s | 2.1× |
| `bank` (contended read-modify-write) | 19.1k req/s | 46.9k req/s | 2.5× |
| `set-many` (already batched per request) | 3.5k req/s | 4.7k req/s | +35% |
| `lock` (not framed) | 49.0k req/s | 48.5k req/s | none |
| `sequence` (not framed) | 113.9k req/s | 113.5k req/s | none |

`RETURN 1` and `rate-limit` are two A/B/A/B runs of 8 seconds each; the other rows are single 6-second
runs per arm. `rate-limit` used `--rate-limit-budget 1000000` so that every request takes the admitted
(read + write + commit) path.

### Latency

| Workload | One request per message | Frames |
|---|---|---|
| `rate-limit`, c=64 | p50 700 µs, p99 4.0 ms | p50 400 µs, p99 3.5 ms |
| `RETURN 1`, c=64 | p50 601 µs, p99 805 µs | p50 140 µs, p99 317 µs |
| `mixed`, c=8 | p50 102 µs, 73.7k req/s | p50 89 µs, 85.8k req/s |
| `mixed`, c=1 | p50 71 µs, 13.8k req/s | p50 72 µs, 13.7k req/s |

At concurrency 1 there is never a second request waiting, so no frame is ever built and the two arms are
the same. Lower latency at higher concurrency is a closed-loop effect: the same number of workers finish
each request sooner because each stream message carries more of them.

### CPU per request

| Workload | Process | One request per message | Frames |
|---|---|---:|---:|
| `RETURN 1` | node | 39.9 µs | 9.2 µs |
| `RETURN 1` | client | 25 µs | 6.6 µs |
| `rate-limit` | node | 79 µs | 44 µs |
| `rate-limit` | client | 22 µs | 8 µs |

Computed as process CPU (from `top`) divided by completed requests per second.

### What frames do not fix

With frames, the `rate-limit` run has the node at about 610% CPU and the client at about 110%. The
transport is no longer the limit for that workload; the work of the transaction itself is. An admitted
rate-limit request is a pessimistic single-key transaction that sends six messages to the actor that owns
the key (lock, read, write, prepare, conflict probe, commit). For reference, Valkey on the same machine
ran the same counter as a Lua `EVAL` at 169k req/s and a plain `GET` at 255k req/s with
`valkey-benchmark -c 64`.

Two other observations from the same runs, both independent of frames:

- A node under sustained fixed-window rate-limit load slows down over time (about 92k → 80k req/s in 90
  seconds without frames). Fixed windows create one new short-lived key per subject per window. Compare
  arms on a fresh node.
- Raising `--concurrency` above 64 on this machine lowered throughput in every arm, because the machine
  is already CPU-saturated.

### Reproducing

```
# Node (from a Release build of Kahuna.Server)
dotnet Kahuna.Server.dll --raft-nodename kahuna1 --raft-nodeid 1 --raft-host 127.0.0.1 --raft-port 8082 \
  --http-ports 8081 --https-ports 8082 --https-certificate <pfx> --allow-plaintext-listener \
  --initial-cluster-partitions 1 --grpc-cleartext-ports 8083 --storage memory --wal-storage memory

# Frames (default), then the same run without them
kahuna-bench -c http://localhost:8083 --workload rate-limit --durability ephemeral \
  --key-space 10000 --rate-limit-budget 1000000 --duration 10
kahuna-bench -c http://localhost:8083 --workload rate-limit --durability ephemeral \
  --key-space 10000 --rate-limit-budget 1000000 --duration 10 --no-request-frames
```

Restart the node between comparisons, alternate the arms (A/B/A/B), and run the client on another
machine if you want numbers that describe the node rather than the pair.

## 6. Maintainer notes

- **Wire.** `GrpcClientBatchType.CLIENT_BATCH_FRAME`, and the `Frame` arm of
  `GrpcBatchClientKeyValueRequest` / `GrpcBatchClientKeyValueResponse`, each wrapping a repeated list of
  the ordinary single message. A frame inside a frame is never run (node) and never followed (client).
- **Shared constants.** `Kahuna.Shared/Communication/Grpc/ClientBatchFrames.cs` holds the header name,
  the version, and both limits. Both ends read them from there.
- **Node.** `KeyValueClientBatcher`: `DispatchFrame` hands each item to the same `Dispatch` a lone
  request uses, and the single writer loop packs responses with `TryFillFrame`. The loop peeks, measures,
  and only then takes a response, which is correct because the response channel has one reader.
- **Client.** `GrpcBatcher.RunKeyValueFrames` builds frames, `WriteKeyValueFrame` writes them under the
  stream's write lock, and `DispatchKeyValueResponse` fans a response frame out. The wait for the write
  lock is bounded by the cancellation token of a request that is still live, so a stream that stops
  accepting writes costs each caller its own deadline and never parks the dispatch loop.
- **Envelope reuse.** Both ends refill one frame envelope per stream (node) or per batcher (client). That
  is safe only because a write has serialized its message by the time the awaited write returns. A test
  double that records messages must copy them.
- **Tests.** `TestClientBatchFrames` (node) and `TestClientRequestFrames` (client) in
  `Kahuna.Server.Tests`. Both are hermetic.

Invariants to keep:

1. Never send a frame on a stream whose node did not announce support. Never treat a missing header as
   support, and never block a request on the header.
2. Never send a response frame on a stream that did not carry a request frame.
3. Register every item of a frame for response matching before the frame is written.
4. Never resend a frame.
5. Never hold a request or a response back to fill a frame.
6. Keep every frame inside both limits on the sending side.
