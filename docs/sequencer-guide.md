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

That is why one Raft commit covers `SequencerBlockSize` values instead of one. The block size is
server-wide by default, and a sequence can carry its own; see *Block size, per sequence* below.

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

- **Uniqueness within an incarnation.** A value is issued at most once for the lifetime of an
  *incarnation* of the sequence. This holds across concurrent callers, across nodes, across leadership
  changes, and across restarts. An `update` starts a new incarnation, and an update that lowers the
  current value makes the new incarnation hand out values the old one already issued — deliberately.
  See *Updating a sequence*.
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

If your application needs gap-free numbering (a legally sequential invoice register, for example), give
*that sequence* a `blockSize` of 1, which reserves each value durably before handing it out — one commit
per value, exactly as if there were no block at all. Do not reach for the server-wide
`SequencerBlockSize` for this; it would impose the same cost on every sequence on the node.

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

## Updating a sequence

`update` rewrites a sequence's parameters. It is the operation behind SQL's `setval`,
`ALTER SEQUENCE … RESTART`, `ALTER SEQUENCE … INCREMENT BY` / `MAXVALUE`, and
`TRUNCATE … RESTART IDENTITY`.

Every parameter is optional, and an omitted one is left exactly as the record has it:

| Field | Effect |
|---|---|
| `currentValue` | New reserved high-water mark. The next value issued is this plus the increment. |
| `increment` | New step between values. Must be positive. |
| `initialValue` | New recorded starting value. Descriptive only; it does not move the counter. |
| `maxValue` / `removeMaxValue` | Sets or removes the maximum. |
| `blockSize` / `removeBlockSize` | Sets or removes this sequence's own block size. |

`maxValue` and `blockSize` are optional on the record as well, so omitting one cannot also mean "remove
it". Each has a companion `remove*` flag that does. Supplying both a value and its `remove*` flag is a
contradiction and is rejected.

An update is refused with `InvalidInput` when it would leave the sequence unable to allocate: a
non-positive increment, a block size below 1, a maximum below the current value, or a change set that
changes nothing at all. The check runs against the record *as it will be*, so lowering the maximum below
a current value you did not touch is refused too. A refused update writes nothing.

### Incarnation

Each update **breaks the sequence's identity as a value stream** and bumps its `incarnation`, a counter
you can read through `get`. Everything the sequence guarantees is scoped to one incarnation:

> A value is issued at most once for the lifetime of an incarnation.

Setting the current value *downwards* therefore reissues values the sequence has already handed out.
That is what `setval` is for, and it is your decision — Kahuna will not stop you, and it will not
pretend the old values were never issued. If you need the two ranges not to overlap, do not move the
value down into one that has been used.

### Why an update takes about five seconds

**`update` withholds its answer for one `SequencerBlockLease`, and the sequence refuses allocations with
`MustRetry` for the same interval.** Both are deliberate.

A reserved block is served with no storage traffic at all. A node that has lost the sequence's partition
without noticing keeps handing out values from the window it already reserved, and nothing tells it the
record changed until its lease forces a revalidation. So for one lease after the record is written there
are potentially two live streams: the replaced incarnation's window on that node, and the new
incarnation on the owner.

Kahuna closes both sides of that window rather than documenting it:

1. The update does not report success until a window reserved from the replaced incarnation can no
   longer be served anywhere.
2. For the same interval, **no node issues anything from the new incarnation either** — allocations get
   `MustRetry`, which means the attempt consumed nothing durable and may be repeated as-is. Without this
   half, the workload's `nextval` calls would be served from the new stream long before the operator's
   update returned, which is exactly the collision the first half is paying to avoid.

Every caller of an update is a DDL-shaped statement, not a hot path, so a bounded delay is the right
price. An instant answer that is wrong for five seconds is the failure being avoided.

Two consequences worth planning for:

- **Client and proxy deadlines must exceed `SequencerBlockLease`.** A default shorter than the lease
  turns a correct update into a timeout.
- **`SequencerBlockLease = 0` refuses the operation.** That setting disables revalidation, so a stale
  window would never be voided and no wait would be long enough. The update returns `InvalidInput`
  rather than reporting a guarantee the node cannot keep.

The wait assumes every node in the cluster runs the same `SequencerBlockLease`. A node configured with a
longer one holds its window past the interval the update waited out. Keep the setting uniform.

### What an update does not carry forward

Recorded idempotency entries belong to the incarnation being replaced, so **the idempotency map is
cleared**. A keyed reserve recorded before an update allocates fresh values after it, rather than
replaying an allocation the new incarnation never reserved.

## Block size, per sequence

`SequencerBlockSize` is a server-wide setting: it is what every sequence on the node amortizes its
commits over. A sequence can override it with its own `blockSize`, set at `create` or at `update`, and
removed again with `removeBlockSize`.

This exists because "gap-free" is a property of one sequence, not of a node. Lowering the server-wide
setting to 1 for a single invoice register would impose one commit, with its fsync, per value on every
surrogate-key sequence on that node as well.

```sh
# Gap-free: one commit, with its fsync, per value.
kahuna-cli --create-sequence invoices --block-size 1

# Follows the server-wide setting, and keeps following it.
kahuna-cli --create-sequence events

# Retune one sequence later. This is an update, so it costs a lease period.
kahuna-cli --update-sequence invoices --current-value 5000 --block-size 1
```

A `blockSize` of 1 costs **one Raft commit, with its fsync, per value**. That is the point of it, and it
is a large throughput difference. Do not set it by habit.

A sequence with no `blockSize` follows the server-wide setting and *keeps* following it: the value is
resolved at every reservation, never frozen into the record, so retuning the node moves existing
sequences with it.

## Delete and recreate

A delete is routed to the sequence's owner, which discards its block before removing the record, so the
recreated sequence starts clean. The one residual window is a former owner that has not yet learned it
lost the partition: it can drain values from the deleted incarnation until it discovers the change —
its next reservation fails the compare-and-swap and forces a re-read, and even a block served purely
from memory is revalidated once its `SequencerBlockLease` (default 5 s) expires, which detects the new
incarnation and voids the stale window. Recreated-name collisions are therefore possible only inside
that lease, on a node that is simultaneously stale about leadership.

**Prefer `update` over delete-and-recreate.** It is the operation that closes that window rather than
documenting it: it waits the lease out before reporting success, and it keeps the new incarnation quiet
meanwhile. Delete-and-recreate still carries the window, because a create cannot wait — a fresh sequence
that refused its first value for five seconds would be a worse trade than the one it fixes. If you must
recreate a name and need the result authoritative everywhere immediately, follow the create with an
update, or use a fresh name.

## Configuration

| Setting | CLI flag | Default | What it controls |
|---|---|---|---|
| `SequencerBlockSize` | `--sequencer-block-size` | 1000 | Values reserved per commit, for sequences that do not carry their own `blockSize`. `1` = gap-free, one commit per value. |
| `SequencerWorkers` | `--sequencer-workers` | 128 (server) | Sequence actors. Each name is routed to one; bounds how many distinct sequences allocate concurrently. |
| `SequencerIdempotencyRetentionMax` | `--sequencer-idempotency-retention-max` | 256 | Retained idempotency entries per sequence. `0` disables the cap. |
| `SequencerIdempotencyRetentionTtl` | `--sequencer-idempotency-retention-ttl` | 600 s | Age at which an idempotency entry is dropped. `0` disables age pruning. |
| `SequencerMaxSequencesPerActor` | `--sequencer-max-sequences-per-actor` | 10000 | Resident sequences per actor before the least recently used are evicted (abandoning their blocks). |
| `SequencerBlockLease` | `--sequencer-block-lease` | 5 s | How long a block may be served purely from memory before it is revalidated against the durable record. Also how long an `update` withholds its answer, and how long the updated sequence refuses allocations. `0` disables revalidation, and therefore refuses `update`. |

The same names exist on `EmbeddedKahunaOptions` for in-process hosts.

### Choosing a block size

Larger blocks mean fewer commits and larger gaps. A single restart, eviction, or ownership change costs
at most `blockSize - 1` skipped values, because only the owning node holds a window.

- **High-throughput ids** (surrogate keys, event ids) — raise it. Gaps are irrelevant and the commit rate
  is what limits you.
- **Human-facing numbering** — the default of 1000 is usually fine; gaps are visible but harmless.
- **Gap-free registers** — `1`, set on the sequence rather than on the server. Expect one Raft commit,
  with its fsync, per value.

## Boundaries and errors

- `MaxValueExceeded` — the requested run would pass the sequence's maximum, or would overflow `long`. A
  reservation is clamped to the largest value the maximum allows, so the final block may be shorter than
  `SequencerBlockSize`. Once exhausted, the failure is reported without any storage round trip.
- `NotFound` — the sequence does not exist (or was deleted).
- `AlreadyExists` — create raced another create.
- `MustRetry` — transient; the attempt consumed nothing durable and can be retried as-is. This is also
  what an allocation gets while a recently updated sequence is holding its new incarnation quiet, for up
  to one `SequencerBlockLease` after the update.
- `InvalidInput` — a non-positive count or increment, a maximum below the initial value, an empty or
  reserved-prefix name, an idempotency key longer than 1 KB, or an idempotency key replayed with a
  different count than it was recorded with. On an `update` specifically: a block size below 1, a maximum
  below the folded record's current value, a change set that changes nothing, a field supplied together
  with its own `remove*` flag, or an update attempted on a node whose `SequencerBlockLease` is `0`.

## Record compatibility

Four record formats are readable: the original JSON encoding, the first binary encoding, the second
(which adds a timestamp per idempotency entry so retention can age entries out), and the current one
(which adds the per-sequence block size, the incarnation counter, and the instant of the last break).
Only the current format is written; reading an older record and writing to it migrates it forward in the
same write. No migration step is required.

A record in any older format reads as `incarnation = 0` with no `blockSize`, which is correct: it has
never been updated, and it follows the server-wide block size.
