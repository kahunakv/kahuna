# Sequencer guide

Kahuna sequences hand out monotonically increasing numbers — order ids, invoice numbers, anything that
needs a unique, ever-increasing value. This guide covers the semantics callers must design around, the
tuning knob that trades durability granularity for throughput, and the guarantees that hold across
restarts, failovers, and retries.

## The model

A sequence is a durable record stored as a key-value entry under `__kahuna:sequences:{name}`. The
whole `__kahuna:` namespace is reserved: the public key-value API rejects reads, writes, deletes,
expiry extensions, and locks on keys under it with `InvalidInput`, so a client cannot corrupt a
sequence record out from under its owner. The sequence is *owned* by the node leading that key's
partition: a request arriving anywhere else is redirected there, exactly as a lock request is — and
redirected at most once: the receiving node re-checks leadership itself and answers `MustRetry` if
the forward went stale, rather than forwarding again. Because the owner also leads the partition
holding the record, the writes below are local.

The owner keeps an in-memory *block*: a window of values it reserved by compare-and-swapping the
record's high-water mark upwards in a single write. Values are handed out of that window with no storage
traffic at all until it is drained, at which point the owner reserves the next one.

That is why one Raft commit covers `SequencerBlockSize` values instead of one.

```
client → any node → owner of __kahuna:sequences:orders → SequenceActor("orders")

record.CurrentValue = 4000      ← highest value ever reserved
owner's block:       3001 … 4000
                     ↑ 3001..3247 already issued, 3248..4000 still to come
```

Blocks never overlap. A reservation only lands if the record still carries the revision the reserver
read, so even during a failover — when a former owner may not yet know it lost the partition — the new
owner's first reservation compare-and-swaps *above* whatever the old one holds. The two can never issue
the same value.

## What you get, and what you do not

**Guaranteed.**

- **Uniqueness.** A value is issued at most once, for the lifetime of the sequence. This holds across
  concurrent callers, across nodes, across leadership changes, and across restarts.
- **Contiguity within one reserve.** `reserve(count)` returns `count` consecutive values. A run is never
  split across two reservations.
- **Monotonic while ownership is stable.** One node owns a sequence at a time, so successive allocations
  climb regardless of which node the caller talked to.

**Not guaranteed.**

- **No gaps.** Whatever remains in a block is abandoned when the owner restarts, loses the partition, or
  evicts the sequence from memory. Those values are never issued. This is the same trade a conventional
  database sequence cache makes (PostgreSQL `CACHE`, SQL Server sequence cache).
- **Ordering across an ownership change.** A former owner that has not yet learned it lost the partition
  can still drain its window while the new owner issues higher values, so during that window a value
  handed out later may be numerically lower. The values are still unique — only their order is not.
  That stale-drain window is bounded by `SequencerBlockLease` (default 5 s): a block that has not
  touched the durable record within the lease is revalidated — a routed read answered by the real
  leader — before anything more is served from it.

If your application needs gap-free numbering (a legally sequential invoice register, for example), set
`SequencerBlockSize = 1`, which reserves each value durably before handing it out — one commit per value,
exactly as if there were no block at all.

## Reading a sequence

`GetSequence` returns the durable record, and its `CurrentValue` is the **reserved high-water mark, not
the last value handed out**. After creating a sequence at 0 and calling `next` four times with the
default block size of 1000, `CurrentValue` reads 1000: four values were issued, out of a thousand that
were reserved to issue them from.

`CurrentValue` is therefore an upper bound on issued values. Treat it as "no value above this has been
issued", never as "this value has been issued".

## Idempotent reserves

Passing an idempotency key makes a reserve replayable: retrying the same key returns the identical
allocation instead of consuming fresh values. This is what makes a client retry after a timeout safe.

Idempotent requests pay for that guarantee. The allocation is written to the durable record *before* the
caller is answered, so a retry that lands on a different node — or on the same node after it has
forgotten everything — still replays. A plain reserve inside a block writes nothing.

An idempotency key must always describe the same request: replaying a recorded allocation under a
different `count` is rejected with `InvalidInput` rather than silently returning a range of the wrong
size.

The retention window is bounded, because the record is rewritten on every reservation and an unbounded
map would make every write more expensive than the last:

- `SequencerIdempotencyRetentionMax` (default 256) caps retained entries per sequence; the oldest are
  dropped first.
- `SequencerIdempotencyRetentionTtl` (default 10 minutes) drops entries older than the window.

**Replay is guaranteed only within that window.** A retry arriving after the entry has been reclaimed
allocates fresh values rather than replaying. Size the window against how long your clients actually
retry for, not against how long you keep their records.

## Delete and recreate

A delete is routed to the sequence's owner, which discards its block before removing the record, so the
recreated sequence starts clean. The one residual window is a former owner that has not yet learned it
lost the partition: it can drain values from the deleted incarnation until it discovers the change —
its next reservation fails the compare-and-swap and forces a re-read, and even a block served purely
from memory is revalidated once its `SequencerBlockLease` (default 5 s) expires, which detects the new
incarnation and voids the stale window. Recreated-name collisions are therefore possible only inside
that lease, on a node that is simultaneously stale about leadership.

If you recreate a name and need the new incarnation to be authoritative everywhere immediately, use a
fresh name — the simplest and safest option — or run that sequence with `SequencerBlockSize = 1`, where
no block is ever held to drain.

## Configuration

| Setting | CLI flag | Default | What it controls |
|---|---|---|---|
| `SequencerBlockSize` | `--sequencer-block-size` | 1000 | Values reserved per commit. `1` = gap-free, one commit per value. |
| `SequencerWorkers` | `--sequencer-workers` | 128 (server) | Sequence actors. Each name is routed to one; bounds how many distinct sequences allocate concurrently. |
| `SequencerIdempotencyRetentionMax` | `--sequencer-idempotency-retention-max` | 256 | Retained idempotency entries per sequence. `0` disables the cap. |
| `SequencerIdempotencyRetentionTtl` | `--sequencer-idempotency-retention-ttl` | 600 s | Age at which an idempotency entry is dropped. `0` disables age pruning. |
| `SequencerMaxSequencesPerActor` | `--sequencer-max-sequences-per-actor` | 10000 | Resident sequences per actor before the least recently used are evicted (abandoning their blocks). |
| `SequencerBlockLease` | `--sequencer-block-lease` | 5 s | How long a block may be served purely from memory before it is revalidated against the durable record. `0` disables revalidation. |

The same names exist on `EmbeddedKahunaOptions` for in-process hosts.

### Choosing a block size

Larger blocks mean fewer commits and larger gaps. A single restart, eviction, or ownership change costs
at most `blockSize - 1` skipped values, because only the owning node holds a window.

- **High-throughput ids** (surrogate keys, event ids) — raise it. Gaps are irrelevant and the commit rate
  is what limits you.
- **Human-facing numbering** — the default of 1000 is usually fine; gaps are visible but harmless.
- **Gap-free registers** — `1`. Expect one Raft commit, with its fsync, per value.

## Boundaries and errors

- `MaxValueExceeded` — the requested run would pass the sequence's maximum, or would overflow `long`. A
  reservation is clamped to the largest value the maximum allows, so the final block may be shorter than
  `SequencerBlockSize`. Once exhausted, the failure is reported without any storage round trip.
- `NotFound` — the sequence does not exist (or was deleted).
- `AlreadyExists` — create raced another create.
- `MustRetry` — transient; the attempt consumed nothing durable and can be retried as-is.
- `InvalidInput` — a non-positive count or increment, a maximum below the initial value, an empty or
  reserved-prefix name, an idempotency key longer than 1 KB, or an idempotency key replayed with a
  different count than it was recorded with.

## Record compatibility

Three record formats are readable: the original JSON encoding, the first binary encoding, and the current
binary encoding (which adds a timestamp per idempotency entry so retention can age entries out). Only the
current format is written; reading an older record and reserving from it migrates it forward in the same
write. No migration step is required.
