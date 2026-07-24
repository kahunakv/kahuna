# Kahuna reusable transaction coordinator guide

This guide explains how Kahuna coordinates interactive key/value transactions, how to use the
transaction session safely, and how the server keeps a complete working set through retries,
concurrent finalization, failures, and recovery. It is written for two audiences:

- **Developers using Kahuna** who want to choose transaction policies, commit or roll back a session,
  and understand what each outcome means.
- **Developers maintaining Kahuna** who need the lifecycle, routing, durability, and cleanup
  invariants in one place.

No knowledge of the coordinator internals is required. If you are new to Kahuna transactions, start
with the example below and then read the lifecycle sections in order.

> **Looking for the internals?** This guide covers the *interactive session API and its semantics*. For
> the end-to-end execution path — how a request travels from gRPC/REST/embedded through routing and
> staging into durable-intent 2PC, out through the partition write aggregator to the Raft WAL and the
> persistence backend, and what happens in the deferred-settlement window afterwards — read
> **`transaction-lifecycle-guide.md`**. For the aggregator's batching and backpressure knobs in depth,
> read **`partition-write-coalescing-guide.md`**.

---

## 1. The big picture

An interactive transaction can touch keys owned by several partitions and several nodes. The
coordinator is the server component that turns those separate operations into one transaction.

The important design choice is that **the server owns the transaction working set**. As operations
complete, the coordinator records the keys that changed, the reads that may need validation or MVCC
cleanup, and every point, prefix, and range lock that was acquired. Commit and rollback use that
record directly; they do not accept a client-built list of work to finalize.

That gives Kahuna three useful properties:

1. A successful operation cannot be accidentally omitted from commit or cleanup.
2. Finalization can close the session, drain work already in flight, and reject anything that arrives
   too late.
3. Retries can be deduplicated with an operation identity instead of applying the same mutation twice.

For persistent transactions that need a recoverable commit decision, the coordinator finalizes through
durable-intent 2PC: a replicated canonical record decides the outcome and each modified key's committed
value is staged as a replicated intent. If the live coordinator disappears, the participant partition
leaders finish or presume-abort the transaction from that record — the prepared writes and the decision
are durable, not just in-memory.

> **Concept — coordinator and participant.** The coordinator owns the transaction-wide decision and
> working set. A participant is a key/partition that performs one of the transaction's reads, writes,
> or locks. They may live on different nodes.

---

## 2. Using an interactive transaction

Interactive sessions are available through the gRPC transport. The REST communication implementation
does not expose session start, commit, or rollback.

A typical persistent transaction looks like this:

```csharp
using Kahuna.Client;
using Kahuna.Shared.KeyValue;

CancellationToken cancellationToken = CancellationToken.None;

string[] urls =
[
    "https://localhost:8082",
    "https://localhost:8084",
    "https://localhost:8086"
];

KahunaClient client = new(urls);

KahunaTransactionOptions options = new()
{
    Locking = KeyValueTransactionLocking.Pessimistic,
    Timeout = 10_000,
    DecisionDurability = DecisionDurability.Durable
};

await using KahunaTransactionSession session =
    await client.StartTransactionSession(options, cancellationToken);

KahunaKeyValue debit = await session.SetKeyValue(
    "accounts/alice",
    "90",
    durability: KeyValueDurability.Persistent,
    cancellationToken: cancellationToken);

KahunaKeyValue credit = await session.SetKeyValue(
    "accounts/bob",
    "110",
    durability: KeyValueDurability.Persistent,
    cancellationToken: cancellationToken);

if (!debit.Success || !credit.Success)
    throw new InvalidOperationException("The transaction write was not accepted.");

if (!await session.Commit(cancellationToken))
    throw new InvalidOperationException("The transaction did not reach a final outcome.");
```

Call `Commit` explicitly when the work should become visible. Disposing a still-pending session calls
`Rollback`, which makes `await using` a convenient safety net for exceptions and early returns.
`AutoCommit` is present on `KahunaTransactionOptions` and is carried on the wire, but the current
interactive session does not use it to commit during disposal; an explicit `Commit` is still required.

The session exposes `Status`, `TransactionId`, `Handle`, and `RecordAnchorKey` for diagnostics and
advanced integrations. Ordinary callers should keep the session object and let it carry the routing
identity. After finalization starts, do not issue more reads or writes through that session.

### Supported session operations

`KahunaTransactionSession` provides transaction-scoped variants of:

- set and conditional set;
- get and exists;
- extend and delete;
- batch delete;
- bucket/prefix reads;
- bounded range reads;
- commit and rollback.

The session acquires the needed write or predicate locks for its locking mode. The server records
confirmed lock acquisitions and releases, so the local lock sets in the SDK are only an optimization
that avoids redundant acquisition calls; they are not the source of truth for finalization.

### Choosing transaction options

| Option | What it controls | Guidance |
|---|---|---|
| `Locking` | Pessimistic or optimistic concurrency behavior. | Use pessimistic when reads need point/predicate locks. Use optimistic when conflicts should be detected from read observations at commit. |
| `Timeout` | Session operation/finalize drain deadline in milliseconds. | Choose a value long enough for the largest expected participant fan-out. Non-positive values use the server default. |
| `AsyncRelease` | Whether eligible post-commit cleanup may continue asynchronously. | Leave off when prompt cleanup matters. Rollback still requires confirmed mandatory releases before it reports success. |
| `ReadValidation` | Whether reads are tracked and checked for revision/write-intent changes at commit. | Optimistic locking always validates its read set at commit (validate-at-commit is what makes it optimistic), so `Optimistic` transactions perform the check regardless of this value. `TrackAndValidate` additionally requests the read-set check for a `Pessimistic` transaction; `None` on a pessimistic transaction relies on its locks alone. |
| `ReadTimestamp` | A historical HLC snapshot applied to every read the transaction performs; zero means latest. | The read timestamp is a per-transaction property, so point, bucket, range, and paginated-range reads all observe the same pinned snapshot. A fixed timestamp cannot be combined with `TrackAndValidate`; Begin rejects that combination. |
| `DecisionDurability` | Whether the commit decision is best-effort or recoverable after it is installed. | `BestEffort` is the default. Choose `Durable` only for all-persistent write sets that need post-decision recovery. |

`DecisionDurability` and `KeyValueDurability` answer different questions. The first controls recovery
of the transaction decision; the second controls whether an individual value is persistent or
ephemeral. Durable decision mode rejects a transaction that confirmed any ephemeral modification.

---

## 3. The transaction handle

Every transaction has one canonical `TransactionHandle`:

```text
TransactionHandle
  TransactionId      globally unique HLC identity
  CoordinatorKey     routes the live in-memory session
  RecordAnchorKey    optional route to a durable decision
```

The two keys have deliberately different jobs:

- **`CoordinatorKey`** is created at Begin and remains stable for the session. Routing it finds the
  data-partition leader that owns the live `TransactionContext`.
- **`RecordAnchorKey`** is assigned exactly once, when the coordinator folds the first confirmed
  persistent modified key into the working set. It places the canonical transaction record and routes a
  consult of it after the live session is gone.

The anchor is a logical user key used for placement; the canonical record itself is internal metadata
and is never written into the user key/value namespace. For a batch, successful modifications are
folded in canonical request order so the chosen anchor is deterministic. Failed conditional writes,
read-only transactions, and ephemeral-only changes do not create an anchor.

The SDK exposes the current handle through `KahunaTransactionSession.Handle`. Its
`RecordAnchorKey` is populated from the canonical anchor returned by commit. Lower-level protocol
integrations that retain an anchored handle can use it to resolve a commit after the in-memory
coordinator session has disappeared.

---

## 4. How an operation becomes part of the transaction

Each registered operation has a 128-bit `TransactionOperationId` and a structured declaration. The
declaration includes an `OperationKind` plus a SHA-256 digest of all inputs that define the operation.
Fields are length-prefixed before hashing, which keeps different field boundaries unambiguous.

The operation follows this path:

```text
client / caller
      │
      │  handle + operation id + declaration
      ▼
coordinator: BeginOperation
      │  New
      ▼
participant leader: execute existing key/value or lock path
      │
      │  response + confirmed effects
      ▼
coordinator: CompleteOperation
      │
      ├─ cache response for duplicate registration
      ├─ fold effects into the working set
      └─ acknowledge completion
```

`BeginOperation` is atomic with the finalize fence. Its outcomes mean:

| Outcome | Meaning |
|---|---|
| `New` | This declaration has not run; the participant may execute it. |
| `AlreadyPending` | The same declaration is already in flight. A retained participant result may be used to finish its completion; otherwise return `MustRetry`. |
| `AlreadyCompleted` | Return the cached response without applying the effect again. |
| `RejectedSessionClosed` | Finalization or reaping already closed the session; do not touch participant state. |
| `RejectedDuplicate` | The operation ID was reused with a different kind or digest. |
| `RejectedCapacity` | The session already has the maximum number of pending operations. |

The .NET session creates an operation ID for each SDK call. The communication layer may retry that
call with the same request. A custom protocol client that retries a logical operation must retain and
reuse its operation ID; creating a new ID describes new work.

### Confirmed effects

Only observable, confirmed effects enter the working set:

| Effect | What the coordinator records |
|---|---|
| Successful write/delete/extend | Modified key and its implicit point lock. |
| Successful batch delete | Successful modified keys in request order. |
| Point/prefix/range lock acquire | The exact held lock descriptor. |
| Explicit lock release | Removal of the matching descriptor. |
| Latest point read | Existence and base revision for cleanup and optional validation. |
| Registered scan | The returned items as point read observations; this is not predicate validation. |
| Snapshot read | No live read dependency, because it reads at a fixed historical timestamp. |

A failed conditional write or failed lock acquisition adds no successful effect. A range-lock upgrade
replaces the recorded mode for the same logical bounds instead of adding a second lock.

### The acknowledgement-loss window

There is a subtle failure window after a participant has applied an operation but before the
coordinator acknowledges `CompleteOperation`. Reapplying the operation could duplicate a mutation;
forgetting it could leave commit waiting forever.

`ParticipantOperationCache` closes the normal retry window. The participant stores the original
response and completion payload before sending completion. If that RPC is lost, it returns
`MustRetry`; a retry with the same operation ID replays completion from the cache without applying the
operation again. The cache is node-local and bounded, so it is a short-lived retry mechanism, not a
durable transaction log.

---

## 5. The server-owned working set

`TransactionContext` keeps the authoritative transaction state:

- modified keys and durability;
- point, prefix, and range locks still held;
- first read observations and any observation instability;
- registered operations and pending count;
- locking, read-validation, snapshot, timeout, and decision-durability policies;
- lifecycle, current finalize attempt, 2PC state, record anchor, and staged durable mutations.

Two query shapes exist for server-side integrations:

- **`GetTransactionWorkingSet`** returns an independent copy of the current collections. It is useful
  for advisory decisions such as inspecting held range locks, but the result may become stale while
  the session still accepts operations.
- **`CloseTransaction`** enters the same fence as commit/rollback, rejects new registrations, waits
  for earlier operations to drain, and stores an immutable snapshot. Repeated closes return that same
  snapshot. This is the right API when an integration must publish or invalidate external state from
  exactly the set that will be finalized.

These are server/inter-node protocol surfaces rather than high-level methods on `KahunaClient`.
Integrations should route them with the transaction's coordinator identity and must never send a
modified working set back to commit.

---

## 6. Finalization and concurrency

Commit, rollback, close, and the abandoned-session reaper share one finalize slot per session.
Exactly one caller owns an attempt; concurrent callers wait on the same `FinalizeAttempt` and observe
the owner's result. This is what prevents commit and rollback from independently running over the same
mutable transaction.

The first attempt also changes the lifecycle from `AcceptingOperations` to `Finalizing`. That
transition is permanent: a retryable finalize releases the attempt slot for another finalize, but it
does not reopen the session to data operations.

Finalization proceeds in this order:

1. Close registration to new work.
2. Wait for every operation registered before the fence to complete or be safely cancelled.
3. Capture the frozen server-owned working set and record anchor.
4. Run commit or rollback from that set.
5. Perform the required cleanup.
6. Publish one outcome to the owner and all mirroring callers.
7. Retain terminal outcomes before removing the active session, so duplicate finalization never sees
   a gap between the two stores.

If the drain deadline expires, finalization returns `MustRetry`. The session stays closed, and a later
commit or rollback rejoins the same drain.

### Commit

Commit validates the enabled read policy, prepares every confirmed modified key, checks for concurrent
write intents when read validation is active, and then commits the prepared mutations. A read-only
transaction is valid and commits without a prepare round.

After a successful best-effort commit, modified-key locks were already settled by 2PC; the coordinator
then releases any remaining point/prefix/range locks and read MVCC entries. Eligible post-commit
cleanup may be detached when `AsyncRelease` is enabled.

### Rollback

Rollback does not prepare. It releases staged writes, point locks, prefix locks, range locks, and read
MVCC entries from the coordinator working set. Every cleanup item is attempted even if another item
fails. `RolledBack` is returned only after all intent-bearing releases have a positive acknowledgement;
otherwise the session remains available for another finalize attempt and returns `MustRetry`.

---

## 7. Outcomes and retries

The response type describes what the caller should do next:

| Outcome | Meaning | Caller action |
|---|---|---|
| `Committed` | The transaction reached terminal commit. | Stop; the session is complete. |
| `RolledBack` | Required rollback cleanup was acknowledged. | Stop; the session is complete. |
| `MustRetry` | This final outcome is not known yet, or transient work remains. | Retry commit/rollback with the same handle; do not add operations. |
| `Aborted` | This session reached a definite non-committing outcome. | Start a new transaction if the application wants to try the business operation again. |
| `Errored` | The handle is unknown/expired or an unexpected failure made the outcome unavailable. | Do not reinterpret it as a conflict; investigate or resolve at the application boundary. |
| `InvalidInput` | The request or policy combination is invalid. | Fix the request. |

The gRPC client retries `MustRetry` responses from start, commit, and rollback a bounded number of
times. If those retries are exhausted, it throws `KahunaException`. Inspect
`KahunaException.KeyValueErrorCode` rather than matching message text.

Terminal outcomes for live best-effort sessions are retained in memory so duplicate commit/rollback
requests can return the same answer. This is a bounded idempotency window, not permanent history.
Once an outcome ages out or is evicted, a duplicate returns unknown `Errored`, never a fabricated
conflict.

---

## 8. Durable commit decisions

`DecisionDurability.Durable` finalizes an all-persistent transaction through **durable-intent 2PC** — now
the only path for a persistent commit (the manual ticket path has been retired). It does not persist the
active coordinator *session* (its live
in-memory context still lives only on the coordinator-partition leader), but every step that decides
the outcome is a replicated Raft record, so the prepared writes and the commit decision survive process
loss and are finished by recovery on the participant leaders.

Two replicated, per-partition stores carry that state:

- a **canonical transaction record** (`TransactionRecordStore`) keyed by `(TransactionId, Epoch)` — the
  single source of truth for the outcome. It moves `Undecided → Commit` or `Undecided → Abort` exactly
  once, by compare-and-set. It lives on the anchor key's data partition.
- a **prepared intent** per modified key (`PreparedIntentStore`) — the staged committed value, carried
  as an idempotent delta on that key's data partition.

The first confirmed persistent modified key is the **record anchor**, and it places the canonical
record; the modified keys' partitions carry their intents. No manual Raft proposal ticket is used on
this path.

### The finalize sequence

`DurableTransactionFinalizer` drives one transaction to a durable outcome:

1. **Initialize + prepare (one barrier).** The anchor key belongs to one of the participant partitions, so
   the canonical record's initialization (`Undecided`, freezing the commit timestamp, decision deadline and
   participant manifest) is submitted together with *that partition's* prepared intents as **one atomic
   ordered proposal**; every other partition's prepare fans out concurrently. The bundle reports two
   separate signals — whether the batch committed (is the record durable?) and whether the anchor prepare
   was acknowledged — because a committed batch whose prepare was rejected must drive a truthful abort,
   while a batch that never committed is a clean retry with nothing durable. If the anchor key routes
   outside the participant partitions, this falls back to initialize-then-prepare.
   Every partition is attempted even if one fails, because a truthful abort needs each partition's result.
2. **Prepare retry.** A prepare rejected *only* because the key still holds a predecessor's
   committed-but-unsettled intent is retried in place a bounded number of times — idempotent for the
   partitions that already prepared — **before any decision is written**, so a healthy commit is not
   aborted merely because the predecessor's background settlement had not caught up yet.
3. **Validate** the read set (revision comparison plus a concurrent-writer probe, read through the
   intent-aware path) — only meaningful once every prepare is durable.
4. **Decision barrier** — compare-and-set the canonical record: `Commit` only when every prepare is
   durable **and** validation passed; otherwise `Abort`. A failed prepare is a *retryable* abort; a
   failed validation is a *conflict* abort.
5. **Resolve** — apply the terminal decision to every intent: on commit, materialize each intent as an
   ordinary key/value record (so the normal replicator makes followers converge) and apply it on the
   leader; on abort, clear the staged write intent/MVCC. Then settle each intent (resolve + remove in
   one atomic delta).

The decision compare-and-set is the point of no return. Because the record is the single authority, the
finalizer reports whatever the record actually became after apply — a concurrent recovery abort can win
the race in the log — not what this attempt requested.

### The decision deadline

Each finalize freezes a **decision deadline** = commit timestamp + a margin derived as
`clamp(DurableDecisionDeadlineMultiplier × observed-finalize-p99, floor, ceiling)`. The p99 is a
rolling estimate of real finalize latency (measured with a monotonic stopwatch — a local duration, not
a distributed event), so the deadline tracks load instead of a fixed guess: too small spuriously aborts
slow-but-alive coordinators, too large delays recovery of dead ones. During warmup the floor applies.

A commit attempt whose HLC has already passed the frozen deadline is rejected by the record state
machine (the record stays `Undecided`) and yields to presumed-abort recovery; that rejection increments
a `late_commit_rejections` counter. A rising rate means the deadline is too tight for current latency.

### Abort classification

The outcome maps onto the `MustRetry`/`Aborted` contract: only a **conflict** abort (validation found a
concurrent writer or a stale read) is reported as `Aborted`. Every other abort — a prepare that did not
replicate, a deadline expiry, a presumed abort — and every infrastructural failure is `MustRetry`, so a
caller never sees a false conflict for a transient failure.

### Settlement timing

Resolution runs **deferred (in the background) by default**: `DurableDeferredSettlement` defaults to
`true` on both `KahunaConfiguration` and `EmbeddedKahunaOptions`. The finalizer returns as soon as the
**decision record is durable**, and materialize + settle run on a background task — so the client-visible
commit point is the durable decision and settlement is off the commit critical path.

This is safe because the cross-node canonical-record lookup now exists: a read (or a scan, or a write)
that meets a committed-but-unmaterialized foreign intent resolves it against the canonical record —
locally, or routed to the anchor-partition leader — instead of serving the stale prior value. Setting the
flag to `false` restores fully synchronous settlement, where a committed value is materialized into
visible MVCC before the caller gets `Committed`.

Either way the canonical decision is already durable before the caller is told anything, so recovery on
the participant leaders finishes any settlement a lost background run would have dropped.

> For the mechanics of that window — what lingers, how each operation resolves it, and the routed
> decision lookup — see **`transaction-lifecycle-guide.md` §8**.

### Recovery

Every node runs a `PreparedIntentRecoveryActor` that periodically sweeps the **prepared intents on the
partitions it currently leads** (`DurableTransactionRecovery`). For each unresolved intent past its
recovery deadline it looks up the transaction's canonical record and:

- record says `Commit` → materialize the value, then settle the intent;
- record says `Abort` → discard, then settle the intent;
- record `Undecided` but still within its decision deadline → leave it; the coordinator may still be
  finalizing;
- record `Undecided` past its deadline, or **no record at all** (an orphan prepare that outlived a
  failed initialization) → drive an idempotent presumed-abort at the anchor, then resolve the intent to
  whatever the record actually became (a concurrent in-flight commit can still win).

Recovery never guesses an outcome and never resolves an intent whose record is undecided but still
inside its window. The request path and recovery may race; initialize, prepare, decide, materialize, and
settle are all idempotent, so correctness does not depend on choosing a winner. A deadline-expiry abort
that actually wins increments `deadline_expiry_aborts`.

### Resolving a commit after the session is gone

If a `Commit`/`Rollback` arrives for a transaction whose live session is gone (evicted, or it lived on a
failed node), the coordinator first checks the retained terminal-outcome window, then consults the local
canonical record: `Commit → Committed`, a conflict abort → `Aborted`, undecided or any other abort →
`MustRetry` (recovery finishes it). A record not resident on this node stays unknown `Errored` rather
than a fabricated conflict — a **cross-node canonical-record lookup is a follow-up**, matching the
replication coverage the read path has today.

### Completion receipts

When an intent materializes, the participant records a `CompletionReceipt` on the same key/value commit.
A duplicate commit or a recovery re-drive can consult that receipt after the original MVCC entry and
write intent are gone, including after replication or restart, to recognize an "already committed"
without reapplying. A receipt lookup validates the full immutable identity — transaction, key, and
durability — so a persistent receipt never satisfies an ephemeral request for the same logical key, and
vice versa. Receipts are restored from the committed key/value log and snapshotted before WAL retention
advances, and are not count-evicted.

Range split and merge hand off completion receipts to the destination partition as part of the
pre-cutover handoff, **replicated onto the destination partition's Raft log** so every replica holds
them and the handoff **gates cutover** — a split or merge aborts before cutover rather than retire the
source range with an outstanding receipt lost. Range-lock transfer is separate and best-effort (locks
are in-memory, non-replicated actor state); only the correctness metadata gates cutover.

### What durable mode does not recover

Durable mode has clear boundaries:

- The active in-memory **session** is not persisted: if the coordinator-partition leader is lost, the
  session and its acquired-lock set are gone. A committed transaction is still resolvable from its
  canonical record; an undecided one is presumed-aborted by recovery after its deadline.
- **Ephemeral** modified keys are rejected: neither the value nor a receipt can survive process loss, so
  a durable transaction that modified an ephemeral key is aborted before any intent is staged. A durable
  transaction with modifications must also have an anchor.
- A read-only transaction creates no record and needs no recovery.
- A **cross-node** consult of the canonical record from a non-resident node is not yet implemented; such
  a duplicate finalize returns `MustRetry`/`Errored` (never a false outcome) until the local record or
  recovery resolves it.

Unlike the retired manual ticket path (whose prepare state lived only in the participant leader's memory), a
prepared intent is *durable* here: a participant leader change after prepare does not lose the staged value,
so recovery on the new leader can still commit or abort it from the canonical record.

---

## 9. Abandoned sessions and retention

`TransactionReaperActor` periodically asks the coordinator to reclaim sessions whose callers never
committed or rolled back. Deadlines use the cluster HLC rather than wall-clock ordering.

The reaper waits for the transaction timeout plus a 15-second grace period. If an operation is still
pending, it adds another 15 seconds so a dispatched participant effect has time to expire before the
session is removed.

When the deadline passes, the reaper claims the same finalize slot as explicit finalization:

- A session with no unresolved operation is rolled back from its confirmed working set.
- If mandatory cleanup returns a transient result, the session stays in `Reaping` and a later sweep
  retries it.
- If an operation is still unresolved after the extended deadline, the session expires with unknown
  `Errored` rather than falsely claiming `RolledBack`. A late completion finds no coordinator session,
  receives no success acknowledgement, and its participant intent eventually expires.
- A durable session whose canonical record is already committed is never rolled back by the reaper. Its
  outcome is resolvable from the record; the prepared intents are settled by the intent-recovery sweep.

### Range-lock lease renewal and leader change

On the same periodic tick, the coordinator renews the range locks held by its live sessions. Renewal is
entirely server-side — there is no client heartbeat. Each sweep re-acquires every range lock in a
session's confirmed working set with a fresh TTL (`RangeLockRenewalTtlMs`, derived from
`CollectionInterval`), so a lock a caller acquired with a short TTL stays effective for as long as the
session is alive without the caller having to extend it.

Two leader-local facts shape what renewal can and cannot guarantee. Neither range locks nor their write
intents are Raft-replicated: they are in-memory state on the **range-lock partition leader** and are not
reconstructed by a new leader. Likewise, the session and its acquired-lock set live only on the
**coordinator-partition leader**. Renewal bridges the first of these: a range-lock acquire always routes
to the current partition leader, so if that leader changes, the next sweep simply re-establishes the lock
on the new leader — a range-lock-partition leader change is transparently re-covered as long as the
session still exists.

What renewal does not bridge is loss of the session itself. If the **coordinator-partition** leader
changes, the session and its acquired-lock set are gone, renewal stops, and the range locks lapse at their
current TTL. This is not a silent inconsistency: a commit that depended on a now-missing lock returns the
retryable `MustRetry`, never a false all-commit, and MVCC read-set validation in the two-phase commit is
the actual correctness backstop — range locks are a best-effort fencing and contention-reduction layer on
top of it. This is also why an end-to-end "is the lock still held" assertion is only stable on a single
node or a cluster with a settled leader: while leadership is still moving (for example a young in-memory
test cluster), the authoritative copy migrates between nodes and a probe can momentarily observe a stale
follower copy.

Two details keep the sweep well-behaved. It runs under a self-imposed deadline (half the renewal TTL) with
bounded concurrency and passes that deadline into each acquire, so one slow or stuck participant is
cancelled at the budget instead of making the whole sweep — and therefore the reaper tick — as slow as the
slowest RPC. And renewal continues through a finalize drain: a `Finalizing` session whose in-flight
operations are still draining keeps renewing its locks, and renewal stops only once cleanup atomically
takes ownership of the lock set, so the predicate lock never lapses in the gap between "finalize began" and
"cleanup released the locks."

### Relevant bounds

| Setting or limit | Default | Purpose |
|---|---:|---|
| `TransactionOutcomeRetentionMax` | 10,000 | Strict maximum retained terminal outcomes; a non-positive value disables best-effort outcome retention. Independent of the durable-decision admission budget. |
| `DurableDecisionOutstandingMax` | 100,000 | Strict maximum **outstanding** (undecided) canonical transaction records this node admits; a non-positive value disables the bound. Decided records do not count against it. |
| `DurableDecisionDeadlineFloorMs` | 5,000 | Lower clamp on the per-transaction decision-deadline margin, and the value used during estimator warmup. |
| `DurableDecisionDeadlineCeilingMs` | 60,000 | Upper clamp on the decision-deadline margin, capping how long a dead coordinator's undecided record can block recovery. Must be ≥ the floor. |
| `DurableDecisionDeadlineMultiplier` | 4 | Multiplier applied to the observed finalize p99 before clamping to the floor/ceiling. |
| `TransactionOutcomeRetentionTtl` | 5 minutes | Age window for terminal outcomes; a non-positive value disables age-based removal. |
| `CollectionInterval` | 60 seconds | Tick interval for the transaction reaper and the prepared-intent recovery actor. |
| Pending operations per session | 4,096 | Fixed safety bound on *in-flight* operations; additional registrations receive `RejectedCapacity` (transient — a completion frees a pending slot, so the caller retries in place). |
| Total operations per session | 65,536 | Fixed bound on *retained* operation records (pending **plus** completed). Completed records are never evicted — they stay for duplicate-response replay — so this counter only rises within a session and the rejection is terminal: registrations beyond it receive `RejectedSessionBudget`, surfaced as `Aborted` (retry as a new transaction, never `MustRetry`). A non-positive value disables the bound. |
| Participant in-doubt results | 8,192 per node | Fixed bound for normal acknowledgement-loss recovery. |
| Participant finalize retries | 20 retries, 250 ms apart | Fixed retry window used while committing or rolling back prepared participants. |

Durable admission is bounded by `DurableDecisionOutstandingMax`, counted over **outstanding**
(undecided) canonical records only — decided records held for the idempotency window are evictable
retention and never consume admission budget, so retained outcomes do not throttle steady durable
throughput. A new durable transaction reserves one slot before prepare; when the budget is full it is
rejected before prepare, and the coordinator never evicts recovery state to make room. This budget is
deliberately independent of `TransactionOutcomeRetentionMax` (the best-effort terminal-outcome cache).

---

## 10. Maintaining and extending the coordinator

The safest way to add a transaction-scoped operation is to treat registration, participant execution,
and completion as one protocol. Use this checklist:

1. Add an `OperationKind` without renumbering existing values, and keep its gRPC enum value in
   lockstep.
2. Build a structured digest that includes every input capable of changing behavior. Include null,
   empty, bounds, inclusivity, durability, timestamps, flags, and canonical batch order where
   applicable.
3. Require both the coordinator key and operation ID for an interactive request. Supplying only one
   is malformed; never silently fall back to an unregistered mutation path.
4. Register outside the participant actor before dispatching any side effect.
5. Define the exact confirmed `OperationEffect`. Do not record a modified key or lock for a failed or
   transient response.
6. Cache a participant result before sending completion, and drive completion without the caller's
   cancellation token once the participant has mutated.
7. Replay the original response and completion payload for the same operation ID. A conflicting
   declaration must be rejected.
8. Make cleanup idempotent and state which acknowledgements are mandatory before `RolledBack` may be
   published.
9. If the operation adds a persistent mutation shape, trace it through durable prepare, the canonical
   record decision, resolution/materialization, replication/restore, intent recovery, and range
   split/merge receipt movement.
10. Exercise local and remote coordinators, duplicate IDs, lost completion, operation-versus-finalize,
    commit-versus-rollback, restart, and partial participant failure.

### Invariants worth keeping close

- The server working set is authoritative; clients may inspect it but never supply it to finalize.
- Registration and the accepting/finalizing transition share one lock. There must be no check-then-act
  gap that lets an operation join after the fence.
- Only the owner of the current finalize attempt executes finalization; all other callers mirror it.
- A non-terminal `MustRetry` releases the attempt slot but never reopens data operations.
- The first confirmed persistent modified key is the immutable anchor. Batch folding order must be
  deterministic and string comparisons must remain ordinal.
- Once the canonical record's compare-and-set commits, rollback is no longer legal. Settling the
  prepared intents belongs to resolution and recovery.
- The canonical record moves `Undecided → Commit`/`Abort` exactly once; the commit timestamp, decision
  deadline, and participant manifest frozen at initialization never change afterward.
- The finalizer reports whatever the record actually became after apply, never what the attempt
  requested — a concurrent recovery abort can win the race in the log.
- Terminal retention is written before the active session is removed, closing the duplicate-finalize
  race.
- Reapers and recovery use HLC time and the same routing/finalize gates as request traffic.

### A practical review path

When reviewing a coordinator change, trace one successful operation and one failure through all of
these boundaries:

```text
SDK or server caller
  -> external transport
  -> coordinator-key routing
  -> BeginOperation
  -> participant routing and actor
  -> participant result cache
  -> CompleteOperation
  -> frozen working set
  -> prepare / commit or rollback
  -> canonical record decision + intent resolution
  -> recovery / reaper / split-merge receipt movement
```

A green happy-path test is useful, but the most valuable cases are acknowledgement loss, concurrent
finalizers, a remote coordinator, a leader change, and a crash after the canonical record commits but
before every prepared intent is materialized and settled.

---

## 11. Diagnostics and focused tests

The coordinator currently relies primarily on structured logs. Useful messages include:

- interactive transaction started, committed, or rolled back;
- unknown commit/rollback handles;
- unacknowledged operation completion retained for retry;
- read dependency revision changes and write-skew aborts;
- participant commit/rollback retry exhaustion;
- canonical-record and prepared-intent replication errors;
- prepared-intent recovery sweep failures;
- completion-receipt snapshot load/save errors;
- split/merge receipt handoff not durable, aborting cutover;
- abandoned-session reaping and cleanup failures.

Durable-path health is also exposed as metrics: `late_commit_rejections` (a commit attempt that missed
its decision deadline), `deadline_expiry_aborts` (recovery presume-aborting an undecided record whose
deadline passed), and the `decision_deadline_margin_ms` histogram (the adaptive margin being chosen). A
rising rejection/expiry rate is the signal to widen the deadline configuration.

The focused server tests are organized around the same boundaries:

| Area | Test file |
|---|---|
| Registry, digest, working-set copies | `TestTransactionOperationRegistry.cs` |
| Routing, deduplication, close/finalize fence, anchors | `TestTransactionRegistrationRouting.cs` |
| Locking, read validation, snapshot policy | `TestTransactionConcurrencyPolicy.cs` |
| Concurrent commit/rollback/finalize attempts | `TestFinalizeSlot.cs` |
| Public SDK session behavior | `TestClientTransactionRegisterRemote.cs` |
| Reaper/finalize-slot interaction | `TestSessionReaping.cs` |
| Completion receipt persistence and movement | `TestCompletionReceiptDurability.cs`, `TestCompletionReceiptTransfer.cs`, `TestReceiptReleaseReplication.cs` |
| Canonical record state machine and store | `TestTransactionRecordStateMachine.cs`, `TestPreparedIntentStore.cs` |
| Durable finalize protocol and recovery | `TestDurableTransactionFinalizer.cs`, `TestDurableTransactionRecovery.cs`, `TestDurableFinalizeInputBuilder.cs` |
| Decision-deadline estimator and consult | `TestFinalizeLatencyEstimator.cs`, `TestDurableOutcomeConsultation.cs` |
| Durable-intent read/scan/write visibility | `TestDurableIntentReadVisibility.cs`, `TestDurableIntentScanVisibility.cs`, `TestDurableIntentWriteVisibility.cs`, `TestPreparedIntentVisibility.cs`, `TestPreparedIntentScanMerge.cs` |
| Durable activation, persistence, replication, end-to-end | `TestDurableIntentActivation.cs`, `TestDurableIntentPersistence.cs`, `TestDurableIntentReplication.cs`, `TestDurableIntentTransactionEndToEnd.cs`, `TestDurableSchedulerCoalescing.cs` |
| Two-phase commit and failover recovery | `TestTwoPhaseCommitRecovery.cs`, `TestKeyValueFailoverCoherence.cs` |

Run focused filters first. For example:

```sh
dotnet test Kahuna.Server.Tests/Kahuna.Server.Tests.csproj -c Debug \
  --filter FullyQualifiedName~TestTransactionOperationRegistry \
  --logger "trx;LogFileName=transaction-coordinator.trx" \
  --logger "console;verbosity=normal"
```

Run only one `dotnet test` process at a time. The Docker-backed client project needs a live cluster;
the server project does not.

---

## 12. File map

| File | Responsibility |
|---|---|
| `Kahuna.Client/KahunaTransactionSession.cs` | Public session API, per-call operation IDs, local status, handle, and commit/rollback behavior. |
| `Kahuna.Client/KahunaTransactionOptions.cs` | Public policy options. |
| `Kahuna.Shared/KeyValue/TransactionHandle.cs` | Canonical transaction identity and durable record anchor. |
| `Kahuna.Shared/KeyValue/TransactionOperationId.cs` | Stable 128-bit operation identity. |
| `Kahuna.Shared/KeyValue/TransactionReadPolicy.cs` | Read-validation and decision-durability enums. |
| `Kahuna.Core/KeyValues/Transactions/TransactionCoordinator.cs` | Session registry, finalize slot, read validation, 2PC, cleanup, terminal retention, and reaping. |
| `Kahuna.Core/KeyValues/Transactions/Data/TransactionContext.cs` | Lifecycle lock, operation registry, working-set folding, anchor assignment, drain signal, and snapshots. |
| `Kahuna.Core/KeyValues/Transactions/Data/OperationDigest.cs` | Stable structured request digests. |
| `Kahuna.Core/KeyValues/Transactions/Data/OperationCompletionPayload.cs` | Transport-neutral confirmed effects and cached response data. |
| `Kahuna.Core/KeyValues/Transactions/Data/ParticipantOperationCache.cs` | Node-local recovery for an applied operation whose coordinator completion was not acknowledged. |
| `Kahuna.Core/KeyValues/KeyValuesManager.cs` | Register-remote wrappers, completion folding, operation-result recovery, durable-record/intent replication seam, and the prepared-intent recovery sweep. |
| `Kahuna.Core/KeyValues/KeyValueLocator.cs` | Coordinator-key, participant-key, and anchor-key routing. |
| `Kahuna.Core/Communication/External/Grpc/KeyValuesService.cs` | External transaction and internal registration/finalize protocol endpoints. |
| `Kahuna.Shared/Communication/Grpc/Protos/keyvalues.proto` | Stable wire messages and enum values. |
| `Kahuna.Core/KeyValues/CompletionReceiptStore.cs` | Persistent-participant completion proof and receipt snapshots. |
| `Kahuna.Core/KeyValues/Transactions/TransactionRecordStore.cs`, `TransactionRecordStateMachine.cs` | Canonical `(TransactionId, Epoch)` record: compare-and-set decision, replication, snapshots, and retention removal. |
| `Kahuna.Core/KeyValues/Transactions/PreparedIntentStore.cs`, `PreparedIntentStateMachine.cs` | Per-key prepared intent: idempotent prepare/resolve/remove deltas and recovery due-set queries. |
| `Kahuna.Core/KeyValues/Transactions/DurableTransactionFinalizer.cs`, `DurableFinalizeInputBuilder.cs` | The durable-intent finalize protocol (initialize → prepare → validate → decide → resolve) and the frozen input it drives. |
| `Kahuna.Core/KeyValues/Transactions/DurableTransactionRecovery.cs`, `PreparedIntentMaterializer.cs` | Participant-side recovery sweep and intent-to-key/value materialization. |
| `Kahuna.Core/KeyValues/Transactions/FinalizeLatencyEstimator.cs`, `DurableTransactionMetrics.cs` | Rolling finalize-p99 estimate that sizes decision deadlines, and the durable-path counters/histogram. |
| `Kahuna.Core/KeyValues/Transactions/PreparedIntentVisibility.cs`, `PreparedIntentScanMerge.cs`, `Handlers/DurableReadVisibility.cs`, `Handlers/ForeignIntentWriteResolver.cs` | Reading and scanning committed-but-unmaterialized intents, and resolving a foreign intent on a conflicting write. |
| `Kahuna.Core/KeyValues/PreparedIntentRecoveryActor.cs`, `TransactionReaperActor.cs` | Periodic prepared-intent recovery and abandoned-session cleanup triggers. |
| `Kahuna.Core/KeyValues/Handlers/TryPrepareMutationsHandler.cs`, `TryCommitMutationsHandler.cs` | Ephemeral 2PC prepare/commit (a persistent key is rejected here — it finalizes through the durable-intent path, which materializes the committed value and records the completion receipt via the normal key/value commit apply). |
| `Kahuna.Core/KeyValues/KeyValueReplicator.cs`, `KeyValueRestorer.cs` | Receipt reconstruction on replication and WAL restore. |
| `Kahuna.Core/Persistence/BackgroundWriterActor.cs` | Receipt snapshot ordering before partition checkpoints advance WAL retention. |
| `Kahuna.Core/KeyValues/Ranges/RangeSplitter.cs`, `RangeMerger.cs` | Replicated, cutover-gating completion-receipt handoff across routing changes. |

---

## 13. Mental model in one paragraph

A transaction begins as an in-memory server session routed by a stable coordinator key. Every
interactive operation registers a unique declaration before touching participant state, then reports
only its confirmed effects back to the coordinator, which builds the authoritative working set and
deduplicates retries. The first commit, rollback, close, or reap closes registration and owns a single
finalize attempt while concurrent callers mirror its outcome; it drains earlier operations, freezes
the working set, and runs 2PC or cleanup from that snapshot. Best-effort outcomes remain replayable
only inside a bounded in-memory window. Durable mode finalizes an all-persistent transaction through
durable-intent 2PC: a canonical `(TransactionId, Epoch)` record decides the outcome by compare-and-set,
each modified key is staged as a replicated intent, and participant leaders materialize or presume-abort
those intents from the record — so the prepared writes and the decision survive coordinator loss even
though the active in-memory session does not. Durability begins at prepare, not at Begin.
