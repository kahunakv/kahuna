# Kahuna benchmarking guide

This guide explains how to use **`kahuna-bench`**, the load-testing tool that sends sustained traffic
to a Kahuna cluster and reports latency percentiles (p50–p99.9) and throughput (req/s). It is written
for operators and contributors who want to measure what a deployment can actually do — or to compare
two builds, two configs, or two hardware profiles.

`kahuna-bench` drives load through the normal `Kahuna.Client`, so it measures the **client-observed**
SLA over the real network and gRPC path — the same path your applications use. It is not a
correctness test (that is the unit/integration suite) and not a server-side profiler (for that, see
the metrics guide).

---

## 1. Install

`kahuna-bench` ships as a .NET global tool.

```
dotnet tool install --global Kahuna.Benchmark
kahuna-bench --connection-source "https://host1:8082,https://host2:8084" --workload mixed --duration 60
```

To build and pack it from a checkout:

```
dotnet pack Kahuna.Benchmark -c Release
dotnet tool install --global --add-source Kahuna.Benchmark/nupkg Kahuna.Benchmark
```

---

## 2. Quick start

```
# 50/50 read-write mix, 128 closed-loop clients, 60s measured + 5s warmup
kahuna-bench -c https://localhost:8082 --workload mixed --duration 60 --concurrency 128

# Pure reads against a 1M-key space
kahuna-bench -c https://localhost:8082 --workload get --key-space 1000000 --duration 30

# Drive a fixed target rate (open-loop) and watch the tail under that load
kahuna-bench -c https://localhost:8082 --workload set --rate 20000 --duration 60
```

A run has three phases, printed as it goes (to stderr when `--format` is `json`/`csv`, so stdout
stays clean — see §7):

1. **Seed** — for `get`/`mixed`, writes up to 100 000 keys so reads have something to find; for
   `sequence`, creates the shared sequence.
2. **Warmup** — `--warmup` seconds of load whose samples are **discarded** (lets caches, JITs and
   connections settle).
3. **Measurement** — `--duration` seconds of load that is actually recorded and reported.

---

## 3. Workloads

| `--workload` | What each request does |
|---|---|
| `set` | `SetKeyValue` with a random `--value-size` payload. |
| `get` | `GetKeyValue`. A not-found read is counted as a **miss**, not a hit (see §5). |
| `mixed` | Per request, `--read-pct`% reads, the rest writes. |
| `lock` | Acquire then release a lock (`GetOrCreateLock`) — one acquire+release per op. |
| `sequence` | `NextSequenceValue` against a single shared sequence (sequencer hot path). |
| `script` | Runs the transaction script at `--script <path>.4gl` once per op (transaction throughput). |

Keys are `bench:{n}` cycled across `--key-space`. A smaller key-space means more contention and more
cache hits; a larger one spreads load and lowers the hit rate.

---

## 4. Flag reference

| Flag | Default | Meaning |
|---|---|---|
| `-c, --connection-source` | (required) | Comma-separated endpoints, e.g. `https://h1:8082,https://h2:8084`. |
| `--workload` | `mixed` | `set` \| `get` \| `mixed` \| `lock` \| `sequence` \| `script`. |
| `--duration` | `30` | Measured window, seconds (excludes warmup). |
| `--warmup` | `5` | Warmup seconds; samples discarded. |
| `--concurrency` | `64` | Concurrent workers (closed-loop) / consumers (open-loop). |
| `--rate` | `0` | Target req/s. `0` = closed-loop; `>0` = open-loop at this rate (§6). |
| `--key-space` | `10000` | Number of distinct keys cycled through. |
| `--value-size` | `128` | Write payload size, bytes. |
| `--read-pct` | `50` | For `mixed`: percent reads. |
| `--durability` | `persistent` | `persistent` \| `ephemeral`. |
| `--script` | — | Path to a `.4gl` script for `--workload script`. |
| `--timeout` | `10` | Per-request timeout, seconds. Also sets the latency histogram ceiling (timeout × 1.5). |
| `--format` | `console` | `console` \| `json` \| `csv` (§7). |
| `--output` | stdout | File path for `json`/`csv`. |
| `--insecure` | `false` | Skip TLS validation (auto-enabled for all-localhost endpoints). |
| `--seed` | `0` | RNG seed for reproducible key/value selection (`0` = time-based). |

---

## 5. Reading the output

```
 Operation     Count    req/s    p50    p90    p95    p99   p99.9    max   mean  errors  misses
 ────────────────────────────────────────────────────────────────────────────────────────────
 get       1,842,301   30,705   0.9ms  1.8ms  2.1ms  6.4ms   18ms   41ms  1.1ms      0   2,940
 set       1,838,902   30,648   1.3ms  2.6ms  3.0ms  9.1ms   27ms   63ms  1.7ms     12       0
 ────────────────────────────────────────────────────────────────────────────────────────────
 TOTAL     3,681,203   61,353   1.1ms  2.2ms  2.6ms  7.8ms   22ms   63ms  1.4ms     12   2,940
```

- **Count / req/s** — successful requests and successes per measured second. Misses, errors and
  timeouts are **not** in `req/s`.
- **p50…p99.9, max** — latency percentiles over **successful** requests only. The tail (p99/p99.9)
  is what matters for an SLA; a great p50 with a bad p99 means real users still see stalls.
- **mean** — success-weighted average latency. Compare to p50: mean ≫ p50 signals a heavy tail.
- **errors** — failed requests (exceptions / non-success responses). In the **console** table this
  column **includes timeouts**; the JSON/CSV output reports `errors` and `timeouts` as separate
  fields. So console `errors` = JSON `errors + timeouts`.
- **misses** — reads that found no value (`get`/`mixed` only). High misses usually means the
  key-space is larger than what was seeded (seeding caps at 100 000 keys) — not a server problem,
  just that the benchmark is reading keys that were never written.

**Live status line** (console format only) updates once per second during the run:

```
  elapsed  12s/60s  in-flight   128  rps   61,420  p99   7.9ms
```

`p99` here is the rolling p99 over the last second, so you can watch the tail react to load in real
time. The final table is the authoritative result.

---

## 6. Closed-loop vs open-loop — and why it changes your p99

This is the most important thing to get right when reading tail latency.

**Closed-loop (`--rate 0`, default).** `--concurrency` workers each loop "send a request, wait for
the reply, send the next." Throughput is whatever those N clients can extract. This answers *"how
fast can N clients go?"* — but under saturation it **understates the tail**: while the server is
slow, a closed-loop client simply isn't sending, so the slow period is under-sampled. This is
**coordinated omission**.

**Open-loop (`--rate > 0`).** Requests are scheduled at a fixed cadence (`1 / rate`), each stamped
with an **intended start time**, and latency is measured from that intended start — not from when
the request actually got dispatched. If the server stalls, the backlog of requests that *should*
have gone out is counted as latency, so the tail reflects true server lag. Use open-loop with a
target `--rate` when you have an SLA throughput in mind and want an honest p99 at that rate.

Rule of thumb:
- *"What's the max throughput of this cluster?"* → closed-loop, raise `--concurrency` until req/s
  plateaus.
- *"At 20k req/s, what tail latency do users see?"* → `--rate 20000` and read p99/p99.9.

> If the open-loop p99 explodes while achieved req/s sits below `--rate`, the server can't sustain
> that rate — the tool is correctly surfacing saturation rather than hiding it.

---

## 7. JSON / CSV output for CI

`--format json` and `--format csv` emit a machine-readable record so CI can trend p99 and req/s
across commits. In these modes the banner and progress go to **stderr** and only the record goes to
**stdout**, so piping works:

```
kahuna-bench -c https://localhost:8082 --workload mixed --duration 30 --format json | jq '.aggregate.p99Ms'

kahuna-bench -c https://localhost:8082 --workload get --duration 30 --format csv --output run.csv
```

Field names are stable (`p50Ms`, `p99Ms`, `p999Ms`, `meanMs`, `rps`, `errors`, `timeouts`,
`misses`, …) and the JSON record carries the full run `parameters`, so a stored result is
self-describing. JSON and CSV report `errors` and `timeouts` separately (unlike the console table,
§5).

---

## 8. Tips and caveats

- **Warm up, and run long enough.** Defaults (5s warmup, 30s measure) are a floor; for stable tails
  prefer 10s+ warmup and 60s+ measurement.
- **Latencies above `--timeout` × 1.5 are clamped** in the histogram. The default 10s timeout gives a
  15s ceiling, which covers everything a 10s-timeout request can produce. If you raise `--timeout`,
  the ceiling tracks it automatically.
- **`get` benchmarks measure a hit/miss mix** when `--key-space` exceeds the seeded 100 000 keys —
  watch the `misses` column. Keep the key-space at or below the seed cap for a pure-hit read test.
- **Localhost runs disable TLS validation automatically** (the banner prints `tls: disabled
  (localhost)`); use `--insecure` to force it for non-localhost self-signed certs.
- **Open-loop at high rates spins a thread** to pace sub-millisecond intervals accurately. This is
  expected; budget one extra core for the generator on high-rate runs.
- **Benchmark from a separate machine** (or at least a machine that isn't also running the cluster
  under test) so the load generator doesn't compete with the server for CPU and skew the numbers.

---

## 9. Reproducible runs

For comparing two builds, pin everything that varies:

```
kahuna-bench -c $ENDPOINTS --workload mixed --read-pct 50 \
  --duration 60 --warmup 10 --concurrency 128 \
  --key-space 100000 --value-size 256 --seed 42 \
  --format json --output build-A.json
```

`--seed` fixes the key/value selection so two runs exercise the same keys; diff the resulting JSON
records (e.g. `.aggregate.p99Ms`, `.aggregate.rps`) to attribute a regression to the build rather
than to run-to-run noise.
