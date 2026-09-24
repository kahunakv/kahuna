# Kahuna transaction lifecycle guide

How a transaction actually executes in Kahuna, end to end: from the gRPC / REST / embedded entry point,
through routing and staging, into durable-intent 2PC, out to the partition write aggregator, down to the
Raft WAL, and finally into the persistence backend — plus what happens *after* the commit returns, under
deferred settlement.

This is the **architecture / internals** guide. Two companions cover adjacent ground and are not repeated
here:

- **`reusable-transaction-coordinator-guide.md`** — the *usage and API* guide for interactive sessions:
  opening a session, the transaction handle, the server-owned working set, retry semantics, session
  retention. Read that if you are writing a client.
- **`partition-write-coalescing-guide.md`** — the aggregator's batching, admission and backpressure knobs
  in depth. This guide only explains the aggregator's *role* in a transaction.

---

## 1. The two transaction shapes

Everything below funnels into one of two shapes. The difference is who drives the operations.

| | Script transaction | Interactive transaction |
|---|---|---|
| Driven by | The server, from a submitted script | The client, operation by operation |
| Entry point | `TryExecuteTransactionScript` | `StartTransaction` → ops → `CommitTransaction` |
| Available on | gRPC, REST, embedded | **gRPC and embedded only** (REST has no sessions) |
| Self-contained | Yes — one call in, one result out | No — spans many round trips |
| Retry | Safe for the engine or caller to re-run the whole script | The client must re-drive the transaction |

Both converge on the same commit machinery (`TransactionCoordinator.TwoPhaseCommit`). The script path just
builds and finalizes the transaction without leaving the server between statements.

A detail that surprises people: a **bare multi-statement script is one auto-commit transaction**. In
`ScriptTransactionExecutor`, a `StmtList` (and `Let`/`If`/`For`/…) routes to
`ExecuteTransaction(ast, null, parameters, autoCommit: true)`. So this is a single transaction, and the
final `GET` is an intra-transaction read-your-own-write:

```
SET pp 'v1'
GET pp
```

whereas a *single-command* script (`SET pp 'v1'` alone) is dispatched directly as a non-transactional
operation — no 2PC, no prepared intent. `BEGIN … COMMIT END` is the explicit form.

---

## 2. Entry points converge on `IKahuna`

Three transports, one contract:

- **gRPC** — `Kahuna.Core/Communication/External/Grpc/KeyValuesService.cs` (`LocksService`,
  `SequencesService`, …), wired by `MapGrpcRoutesExtensions`.
- **REST** — `Kahuna.Server/Communication/External/Rest/KeyValuesHandlers.cs`, wired by
  `MapRoutesExtensions`.
- **Embedded** — `EmbeddedKahunaNode` runs a whole node in-process with no ASP.NET at all.

All three call **`IKahuna`**, implemented by `KahunaManager`, which delegates to the subsystem managers
(`KeyValuesManager`, `LockManager`, `SequencerManager`). From `IKahuna` down, the code path is identical
regardless of how the request arrived — which is why the embedded node is a faithful test surface for
server behaviour.

```
gRPC KeyValuesService ─┐
REST KeyValuesHandlers ─┼─► IKahuna (KahunaManager) ─► KeyValuesManager ─► … 
EmbeddedKahunaNode ────┘
```

---

## 3. Two keyspaces, deliberately isolated

Every key operation carries a `KeyValueDurability`:

- **Ephemeral** — in-memory only. No Raft-durable commit decision, no prepared intents.
- **Persistent** — replicated and persisted; eligible for durable-intent 2PC.

`KeyValuesManager` keeps **separate consistent-hash actor routers** for the two
(`ephemeralKeyValuesRouter` / `persistentKeyValuesRouter`), so `pp` in the ephemeral keyspace and `pp` in
the persistent keyspace are unrelated keys that happen to share a name.

That isolation is enforced structurally: **ephemeral actors are constructed with no prepared-intent or
transaction-record store at all** (`GetEphemeralRouter` passes dedicated empty stores). Durable-intent 2PC
never routes to an ephemeral actor, so those stores stay empty for the node's lifetime, and the entire
ephemeral path — writes, reads, exists, scans, write-intent checks — can never consult a *persistent*
key's durable intent.

> **Why this matters.** Sharing those stores once caused a real bug: an ephemeral write to a key whose
> persistent namesake held a committed-but-unsettled intent materialized that foreign intent into the
> ephemeral entry and derived its revision from it, so a *first* ephemeral write reported revision 1
> instead of 0. It was visible only while deferred settlement left the intent lingering.

A `Durable` transaction that modifies any ephemeral key is rejected outright — durability is a promise an
in-memory mutation cannot keep.

---

## 4. Routing: key → partition → leader → actor

For each operation:

1. **Key space + range** → `KeySpaceRegistry` and the range map (`RangeMapStore`) resolve the key to a
   **data partition** via `DataPartitionRouter`. Partition **0** is the meta partition (range map,
   snapshot floor, coordinator decision records).
2. **Partition → leader.** `KeyValueLocator` forwards the request to the node that leads that partition's
   Raft group. If this node is the leader, it dispatches locally; otherwise it forwards over
   `IInterNodeCommunication` (gRPC in production, in-memory for tests and embedded).
3. **Leader → actor.** The consistent-hash router picks the `KeyValueActor` shard owning the key. Actors
   are **single-threaded** (Nixie), which is what makes the per-key state machine race-free.

The range descriptor **generation** resolved here is carried forward and re-checked at dispatch (the
*fence*), so a split or merge between resolution and dispatch releases the operation retryably instead of
appending to a retired partition.

---

## 5. Staging: what an operation does before commit

Inside the owning `KeyValueActor`, a *transactional* write (one carrying a non-zero `TransactionId`) does
**not** propose anything to Raft. It stages:

- an **MVCC entry** for this transaction on the key (`entry.MvccEntries[transactionId]`) holding the new
  value/revision/expiry, and
- a **write intent** (`entry.WriteIntent`) with a lease (`DefaultTxCompleteTimeout`, 15 s), so concurrent
  transactions can detect the pending write during their own validation.

Reads inside the transaction see their own MVCC entry (read-your-own-write). A `SET` bumps the staged
revision; a `DELETE` sets state `Deleted` **without** bumping the revision — a small asymmetry that matters
later (§9).

Non-transactional writes skip all of this and go straight to a proposal (§7).

The coordinator, meanwhile, records the **server-owned working set**: modified keys, reads needing
validation, and every lock acquired. Commit and rollback use that record — the client never supplies the
list of work to finalize. (Details in the coordinator guide.)

---

## 6. Commit: durable-intent 2PC

`TransactionCoordinator.TwoPhaseCommit` splits the working set:

- **all-persistent** → the durable-intent path (below);
- **all-ephemeral** → an in-memory commit, no Raft;
- **mixed** → the ephemeral subset is prepared first (so an ephemeral failure aborts before anything
  persistent is decided), then the persistent subset is finalized durably and its decision drives the
  ephemeral commit/rollback.

### 6.1 Freezing the input

`DurableFinalizeInputBuilder.TryBuild` produces an immutable `DurableFinalizeInput`:

- identity `(TransactionId, Epoch)` and the `ManifestHash` — together the record's identity;
- the **record anchor key** and its partition;
- one canonical **commit timestamp** (HLC) and a **decision deadline**;
- the participant manifest;
- per-partition groups of **`PreparedIntent`** — the exact committed value, revision, absolute expiry and
  `NoRevision` flag for each modified key.

If any modified key cannot be staged losslessly, the build fails and the transaction aborts rather than
committing through a lesser path.

### 6.2 Two replicated stores

| Store | Scope | Role |
|---|---|---|
| `TransactionRecordStore` | anchor key's partition | The **canonical record** keyed `(TransactionId, Epoch)`. Single source of truth for the outcome. Moves `Undecided → Commit`/`Abort` exactly once by compare-and-set. |
| `PreparedIntentStore` | each modified key's partition | One **live intent per key** holding the staged committed value. |

Both are pure, deterministic state machines (`TransactionRecordStateMachine`,
`PreparedIntentStateMachine`) applied identically on leader, follower, WAL replay and state transfer.

Notable transitions: `Abort` may create a tombstone **from absence** (so a never-initialized transaction can
still be durably aborted), while `Commit` **cannot** — a commit requires an `Undecided` init as proof.

### 6.3 The finalize sequence

`DurableTransactionFinalizer.FinalizeAsync`:

1. **Initialize + prepare (one barrier).** The anchor key belongs to one of the participant partitions, so
   the record initialization and *that partition's* prepare are submitted as **one atomic ordered
   proposal** `[TransactionRecord init, PreparedIntent prepare]`; every other partition's prepare fans out
   concurrently. This removes a sequential pre-decision round trip (5 barriers → 4 inline, 3 → 2 deferred).
   The bundle reports two independent signals — *did the batch commit* (is the record durable?) and *was
   the prepare acknowledged* — because a committed batch whose prepare was rejected must drive a truthful
   abort, whereas a batch that never committed is a clean retry with nothing durable.
   If the anchor key routes outside the participant partitions, it falls back to init-then-prepare.
2. **Prepare retry.** A prepare rejected only because the key still holds a *predecessor's*
   committed-but-unsettled intent is retryable in place: the set is re-prepared a bounded number of times
   (idempotent for partitions that already prepared) **before any decision is written**, so a healthy
   commit is not aborted merely because background settlement had not caught up.
3. **Validate** the read set — revision comparison plus a concurrent-writer probe, read through the
   intent-aware path. Only meaningful once every prepare is durable.
4. **Decide.** Compare-and-set the canonical record: `Commit` only if every prepare is durable *and*
   validation passed; otherwise `Abort`. This is the point of no return. The finalizer reports whatever the
   record actually *became* — a concurrent recovery abort can win the race.
5. **Resolve** — materialize each committed intent into visible KV state, then settle (resolve + remove)
   the intent. **When** this runs is §8.

#### 6.3.1 The one-phase bundle and its gate

When the whole participant set is the anchor partition, steps 1, 3 and 4 collapse into one durable
barrier: the read set is validated up front and `[record init, prepare, commit decision]` is proposed as
one atomic batch. When another node leads the anchor partition, the bundle crosses the wire whole as one
typed operation: the receiving leader submits the three entries as one atomic scheduler submission under
the origin's range fence, and the reply carries the canonical outcome read from its record store after
the ordered apply (plus the bundled-commit gate's verdict when the record stays `Undecided`) — one extra
network hop, still one durable round. The `remote_leader` fallback reason then appears only when the
remote leader is an older node without the typed operation. With `OnePhaseApplyTimeValidation` on, the bundled commit also proves its
read set at apply time, in log order, against the partition's replicated committed-head ledger — so a
read-then-written base and a read-only point read *on the anchor partition* keep the bundle open in a
multi-process cluster. What still closes it is a dependency no deterministic apply-time check exists
for. Every finalize records its verdict on `kahuna.durable_tx.one_phase_gate{outcome}`:

| outcome | meaning |
|---|---|
| `entered` | eligible; later counted as a one-phase commit or a fallback (`one_phase_fallbacks{reason}`) |
| `disabled` | the node has no bundle path wired |
| `read_set_beyond_writes`, `validated_base` | apply-time validation **off**, multi-process: a read-only dependency, or a read-then-written base |
| `predicate_read` | apply-time validation **on**: a prefix or range lock (a predicate, not a key) |
| `off_partition_read` | apply-time validation **on**: a read-only key routed to a partition other than the anchor |
| `non_persistent_read` | apply-time validation **on**: a read-only key of a non-persistent durability |
| `multi_partition`, `anchor_off_partition` | the write set spans partitions, or the anchor is not a participant |

`off_partition_read` is a placement fact, not a flag fault: under hash routing it says the read's key
space hashed to a different partition than the written key's. A consumer that reads an index entry and
writes a row keeps the bundle open by naming the index key space in the row key space's placement group
(the prefix before the first `|` in a key space — see the key-range sharding guide, §4). The node logs
whether apply-time validation is enabled once at startup, so the setting can be confirmed without a
metrics scrape.

### 6.4 The decision deadline

Each finalize freezes `deadline = commitTimestamp + clamp(multiplier × observed-finalize-p99, floor,
ceiling)`. The p99 is a rolling *local* stopwatch measure (a duration, not a distributed event), so the
deadline tracks real load. A commit attempt whose fresh attempt-HLC has passed the frozen deadline is
rejected by the state machine and increments `kahuna.durable_tx.late_commit_rejections`. A rising rate
means the deadline is too tight.

The rejection is final, not transient: attempt HLCs only advance, so every later commit of the same
frozen input is rejected the same way. The finalizer therefore does not answer `MustRetry` and leave the
record to the recovery sweep — that produced a client retry loop that could never terminate (a
coordinator whose disk paused mid-commit re-drove the same dozen transactions for eight minutes after it
healed) — it drives the presumed abort through the record CAS itself, right where the
gate refused it, and reports what the record answers: `Aborted` with class `PresumedAbort`
(`kahuna.durable_tx.late_commit_conclusions{outcome=aborted}`), or `Committed` when a stalled bundle's
commit had already applied under the ordered log (`…{outcome=committed}`). A retry of a frozen finalize
that arrives with its deadline already behind the attempt clock is concluded the same way before any
prepare is re-driven (`kahuna.durable_tx.retries_past_deadline`).

### 6.6 Session ownership and routing

An interactive session lives on the node that began it — the leader of its coordinator key's partition at
`BEGIN` — and stays there when that partition's leadership moves: the session's finalizer forwards its durable
work (the anchor bundle, the decision, materialization) to whichever node leads the partition now. The routed
entry points (`LocateAndCommitTransaction`, `LocateAndRollbackTransaction`, operation registration and
completion, the working-set query) therefore serve a session **this node owns** locally whatever the current
leader is. Before this rule, a leader that stepped down routed every commit for its own sessions to its
successor, which had no such session, and each spun as `MustRetry` until the client's deadline (~100
indeterminate commits per leader pause under a paused disk).

A leader that receives a commit or rollback for a session it does not hold (the client learned the new
leader) offers it to its peers once, in turn; the owner serves it and every other peer answers unknown. The
probe runs only for a request that arrived directly from a caller and only on that rare path — the membership
roster does not carry peers' node ids, so the transaction id cannot name the owner directly.

### 6.7 Record-less intents past the retention horizon

Past `TransactionOutcomeRetentionTtl` an absent canonical record no longer means "never initialized": a
terminal record may have been reclaimed. The recovery sweep never presumes abort for such an intent (that would
discard a reclaimed commit's leg). It decides from the leg's **completion receipt** — written only when a leg's
value materialized, which only a committed transaction does — and settles the intent as a commit when one
exists (`kahuna.transactions.recordless_intent_receipt_commits`). Without a receipt the intent is held: its key
stays read-only until the outcome can be proven. Holds are counted in
`kahuna.transactions.recordless_intents_held` (the last pass's count on this node) and summarized in one warning
per recovery pass naming a few of the keys; a non-zero gauge is an operator signal, not a counter. The record
GC on the anchor leader never reclaims a terminal record while a prepared intent of that transaction is still
resident on the same node, so the hold can only arise for a leg resident elsewhere.

### 6.5 The pre-decision replica fence and its lag breaker

Before proposing the commit of a read-modify-write, the finalizer asks every replica of each participant
partition for its staged-base verdict and refuses the commit if any replica proves the validated base
moved (`kahuna.durable_tx.replica_fence_refusals`). A replica that cannot answer never blocks the commit —
a down replica cannot veto either — but before the lag breaker (Kahuna.Core 1.8.2 and earlier) the
finalizer still *waited* for it: a replica whose
apply had stalled (its disk paused, its WAL saturated) answered `NotApplied` only after the full 400 ms
apply wait, and every commit on the leader paid that wait for a verdict that carried nothing. In one
observed case a follower's 30 s device pause cost a leader with an intact Raft quorum 70% of
its throughput, and the follower's catch-up kept the cluster below half speed for minutes afterwards.

The fence now carries a per-replica breaker (`ReplicaFenceLagTracker`). After three consecutive
full-wait asks without an attestation (a timeout, a transport fault, or a serviced reply that is all
`NotApplied`) the replica is *lagging*: it is still asked on every commit, but with a zero apply wait and
a 100 ms call budget, so a `StaleBase` it can prove from memory still counts while the commit no longer
waits on an apply it is not going to see. Once a second one ask is sent with the full budget as a probe;
three consecutive attesting probes restore the replica (a relapse within 30 s doubles that requirement,
up to 24).

Probe latency alone is the wrong evidence for whether a replica can attest. A replica tens of thousands
of entries behind the leader answers a probe instantly from state that old, and a replica whose disk is
paused answers from memory until the entry it is asked about is the one it cannot write. After a
leader kill a restarted replica attested three fast probes while 75,000 entries behind and
stalling on a shared NVMe; the fence restored it and every commit then waited the full apply wait for a
verdict it could not give. The leader already knows both facts from every Raft acknowledgement, so the
fence reads Kommander's per-follower snapshot (`IRaft.GetFollowerProgress`, Kommander 1.6.10) on every
ask it plans: a replica whose *durable* frontier is more than `ReplicaFenceLagTracker.MaxEntriesBehind`
(1,000) committed entries behind the leader's commit index, or that reports a durable-write stall, is
tripped at once without strikes, is not probed while that holds, and its recovery streak restarts — it is
restored only by consecutive attesting probes made and answered with its frontier within the bound. The
evidence exists only where this node leads the partition asked about; for a participant partition led
elsewhere the probe rules alone apply.

`kahuna.durable_tx.replica_fence_lagging_replicas` is the number of replicas currently held as lagging,
`…replica_fence_lag_transitions{state,reason}` counts the episodes (`reason` on a `lagging` transition
is `attestation`, `frontier` or `stall`), and `…replica_fence_lagging_asks{kind}` counts the zero-wait
asks, the probes, and the asks where a due probe was `held` by the frontier evidence. One warning line
marks each transition and says why.

---

## 7. The partition write aggregator

Every durable record, prepared intent, settlement delta and materialized key/value record reaches Raft
through the **partition write aggregator**, not by proposing directly. Concurrent transactions targeting
the same partition coalesce into one `ReplicateEntries` proposal — this is what makes cross-transaction
batching possible.

A submission carries:

- an **ordered list of entries** (which is how the anchor `[init, prepare]` bundle is expressed atomically);
- an **admission class** — `Ordinary` (record init, prepare) or `Terminal` (decision, materialize, settle).
  Terminal work draws on reserved capacity so an ordinary-write burst can never starve the step that
  *finishes* an already-prepared transaction;
- an optional **fence** (key + generation) re-checked at dispatch, so a split/merge since freeze releases
  the submission retryably;
- an **on-commit callback**, which waits for the ordered Raft consumer apply of each record/intent entry
  (never applying the delta itself — the consumer apply is the stores' single live writer, on the leader as
  on a follower) and reports whether every prepare took ownership of its key.

The executor issues one `IRaft.ReplicateEntries` per batch (`IPartitionBatchExecutor`).

---

## 8. Deferred settlement (the default)

`DurableDeferredSettlement` defaults to **true** on both `KahunaConfiguration` and `EmbeddedKahunaOptions`.

- **Deferred (default):** `FinalizeAsync` returns as soon as the **decision record is durable**.
  Resolution — materialize committed values, settle intents — runs on a background task. The client-visible
  commit point is the durable decision, which removes settlement from the commit critical path. Measured on
  one embedded node with RocksDB + synchronous WAL: **+69 % committed TPS, −42 % commit p50** at 32 workers.
- **Synchronous (`= false`):** resolution is awaited inline, so a committed value is materialized into MVCC
  before the caller returns.

Either way the decision is already durable, so **recovery finishes any settlement a background run loses**.

### 8.1 The window, and what lives in it

Between the durable decision and settlement, a committed transaction's value lingers as a **prepared
intent with resolution `Pending`**. Anything touching that key in the window must resolve it correctly:

| Operation | How it resolves |
|---|---|
| Point read / exists | `DurableReadVisibility` → `PreparedIntentVisibility`. Committed intent → serve the intent's value (a committed *delete* or an expired value reads as does-not-exist); aborted → ignore; **undecided → wait**. |
| Range / bucket scan | The scan overlays the intent window and resolves the whole set at once via `TryRouteForeignScanDecisions` + `DurableReadVisibility.ScanDecision`. |
| Write (set/delete/extend) | `ForeignIntentWriteResolver` materializes a committed intent into the entry *before* the write derives its next revision, flags and existence checks. Undecided → retryable. |
| Point lock acquire | Resolved exactly like a write before the lock is granted: a committed intent is materialized into the entry, so the new holder never holds a key whose committed head it cannot see; undecided → `WaitingForReplication`, which the acquire loop waits out (routing the holder's decision when it is not local). |
| Transactional read that already holds its own MVCC entry on the key | Skips the overlay (its own view is authoritative), but if a committed intent is newer than the resident entry, the pin is behind the committed head and the read answers `Aborted` — the same answer it gives once the entry has advanced. It never returns the pinned pre-commit value. |
| A new transaction's prepare | Blocked (one live intent per key) until the predecessor settles — absorbed by the bounded prepare retry in §6.3. |

**Settlement never erases another transaction's lock state.** A commit that settles after it released its own
intent can land on a key that another transaction has since locked and read. The apply advances the entry,
but it keeps that transaction's MVCC entry (and, on the leader's durable apply, its lock): the entry records
the base the transaction read, and it is what makes that transaction's next read or write of the key answer
`Aborted`. Deleting it would let the transaction re-pin at the new head and write a value computed from the
superseded one over the commit — a lost update that every later check would accept.

**Cross-node.** The decision record lives on the anchor partition, which may be led by another node. When
the decision is not resolvable locally, the read routes a lookup to the anchor leader
(`LookupDurableRecordRouted` via `TryRouteForeignDecision`) and re-issues with the terminal decision, rather
than spinning until settlement propagates.

> **A sharp edge worth knowing.** A durable `DELETE` sets state `Deleted` *without bumping the revision*
> (§5), so a committed delete intent carries the **same** revision as the value it deletes. The write-path
> materialization guard must therefore treat "same revision, different state" as *not yet materialized* —
> otherwise a conditional write such as `SET … NX` issued right after a committed-but-unsettled delete sees
> the pre-delete value and wrongly reports the key as still existing.

---

## 9. Recovery

`DurableTransactionRecovery` is the participant-side sweep that makes the protocol survive a lost
coordinator. Per partition leader, for intents whose recovery deadline has passed:

- **Committed record** → resolve committed and materialize.
- **Abort record** → resolve aborted, no materialization.
- **Undecided past deadline**, or an **orphan prepare with no record** → drive a presumed abort (the abort
  tombstone-from-absence transition), then resolve aborted.
- **Undecided within deadline** → skip; the live coordinator may still decide.

Recovery always takes the winner the record actually became — a concurrent commit is honoured even while
recovery is trying to abort.

---

## 10. Raft, the WAL, and storage

Once a proposal is issued, Kommander owns it:

1. **Propose** — the leader appends the entry batch to its WAL and replicates to followers.
2. **Quorum durable** — a quorum has the entry on disk. This is the true Raft commit point.
3. **Commit + apply** — the committed entries are applied through the replication callbacks
   (`KeyValueReplicator`, the durable stores' `Replicate`), which is how followers converge and how the
   leader's ordered apply runs.

Two WAL behaviours are worth knowing because they dominate commit latency:

- **Group commit.** A WAL worker can coalesce several partitions' writes into one storage flush —
  one `fsync` for many partitions. `MaxWalGroupBatchPartitions` bounds the batch;
  `WalGroupCommitLingerMs` optionally waits to gather a denser batch.
- **Single-fsync commit.** By default an auto-commit proposal costs *two* serial fsyncs (propose, then the
  committed marker). `WalSingleFsyncCommit` releases the client ticket once the **propose quorum is
  durable** and demotes the committed marker to a lazy write. It does not weaken durability —
  propose-quorum-durable *is* the commit point — it moves one fsync off the caller's critical path.

`Kahuna.Server` exposes all three on the command line; embedded consumers set them on
`EmbeddedKahunaOptions` (`RaftMaxWalGroupBatchPartitions`, `RaftWalGroupCommitLingerMs`,
`RaftWalSingleFsyncCommit`). The embedded defaults deliberately mirror **Kommander's** defaults, not the
server's — notably single-fsync is **off** there, because changing durability/recovery timing for every
embedded consumer is an explicit decision, not a silent one.

**Storage.** Raft durability and *backend* persistence are separate. Committed state reaches the
key/value backend asynchronously through `BackgroundWriterActor`, which batches dirty entries to the
configured `IPersistenceBackend` (memory, SQLite, RocksDB). The WAL is the durability authority; the
backend is the materialized store that serves reads after eviction and restart.

---

## 11. End-to-end: one durable transaction

A `BEGIN SET a … SET b … COMMIT END` over two partitions, deferred settlement on:

```
client ─gRPC/REST/embedded─► IKahuna ─► KeyValuesManager
  │
  ├─ per statement: locate key → partition leader → KeyValueActor
  │     └─ stage MVCC entry + write intent      (no Raft yet)
  │
  └─ COMMIT → TransactionCoordinator.TwoPhaseCommit
        └─ DurableFinalizeInputBuilder.TryBuild   (freeze ts, deadline, manifest, intents)
             └─ DurableTransactionFinalizer.FinalizeAsync
                  1. anchor partition: [record init + prepare]  ─┐
                     other partitions: prepare                  ─┼─► aggregator ─► ReplicateEntries ─► WAL
                  2. (bounded prepare retry if a predecessor's intent still holds a key)
                  3. validate read set
                  4. decide (CAS the canonical record)          ───► aggregator ─► WAL   ◄── commit returns here
                  5. resolve  ── scheduled on a background task ───► materialize + settle ─► WAL
                                                                            │
                                                        BackgroundWriterActor ─► persistence backend
```

The caller gets `Committed` at step 4. Steps 5 and the backend write happen after — and any reader meeting
the still-pending intent resolves it through §8.1 rather than seeing a stale value.

---

## 12. Outcome contract

Every path maps onto three outcomes, and the distinction is load-bearing:

| Outcome | Meaning | Caller action |
|---|---|---|
| `Committed` / `Set` / `Get` … | Succeeded | Proceed |
| **`Aborted`** | A genuine **conflict** — validation found a stale read or a concurrent writer | Re-plan; retrying immediately will likely conflict again |
| **`MustRetry`** | Retryable: nothing durable was decided by this attempt | Safe to retry |
| `Errored` / `InvalidInput` | Malformed input or an internal error | Fix the request |

A prepare that did not replicate, an admission rejection and every infrastructural failure are
`MustRetry`, so a caller never sees a false conflict for a transient failure. An `Aborted` carries the
abort class in its reason: `Transaction conflict` for a genuine conflict, and `Transaction aborted:
PresumedAbort` when the frozen decision deadline passed before the commit could be decided (§6.4) — the
record is then durably aborted, so retrying the commit cannot succeed and the caller restarts the
transaction.

A script transaction reports the same three outcomes. A statement that answers `MustRetry`, `Aborted` or
`Errored` stops the script before commit, and the script reports that statement's outcome as its own. The
reason names the statement, the key and the durability, for example `SET orders/1 (Persistent) returned
MustRetry`. Nothing durable happened at that point, so a `MustRetry` from a statement is safe to re-run as a
whole script. A script that ends by its own control flow (`ROLLBACK`, `RETURN`, or no `COMMIT`) answers
`Aborted` with the reason `Transaction aborted`.

---

## 13. Bounds and backpressure

| Knob | Bounds |
|---|---|
| `DurableDecisionOutstandingMax` | Concurrent durable transactions admitted through finalize (**hard**, atomic slot reservation) |
| `DurablePreparedIntentMaxCount` / `…MaxBytes` | Resident prepared-intent count/bytes (**soft**, a non-reserving read — it governs sustained inflow, and a fully simultaneous burst can race past it) |
| `KeyValueWriteMaxBatchItems` / `…Bytes` | Entries and payload per aggregator Raft call |
| `KeyValueWriteMaxQueued*` (partition and global, + terminal reserve) | Admitted-but-not-completed work |
| `MaxTransactionTimeout` / `DefaultTransactionTimeout` | Session lifetime (and the orphaned-snapshot reclamation horizon) |

Observability: `kahuna.durable_tx.resident_prepared_intents`, `…resident_prepared_intent_bytes`,
`…outstanding`, `…resident_records`, `…admission_rejections`, `…late_commit_rejections`,
`…late_commit_conclusions{outcome}`, `…retries_past_deadline{outcome}`,
`…replica_fence_lagging_replicas`, `…replica_fence_lag_transitions{state,reason}`, `…replica_fence_lagging_asks{kind}`.

---

## 14. File map

| Concern | Location |
|---|---|
| Entry points | `Kahuna.Core/Communication/External/Grpc/`, `Kahuna.Server/Communication/External/Rest/`, `Kahuna.Core/Embedding/EmbeddedKahunaNode.cs` |
| Façade | `Kahuna.Core/IKahuna.cs`, `KahunaManager.cs` |
| Orchestration / routing | `KeyValues/KeyValuesManager.cs`, `KeyValues/KeyValueLocator.cs`, `KeyValues/Ranges/` |
| Per-key state | `KeyValues/KeyValueActor.cs`, `KeyValues/Handlers/` |
| Script execution | `KeyValues/Transactions/ScriptTransactionExecutor.cs`, `ScriptParser/` |
| Coordination / 2PC | `KeyValues/Transactions/TransactionCoordinator.cs`, `DurableTransactionFinalizer.cs`, `DurableFinalizeInputBuilder.cs` |
| Durable stores | `Transactions/TransactionRecordStore.cs`, `PreparedIntentStore.cs` (+ their state machines) |
| Deferred-window visibility | `Handlers/DurableReadVisibility.cs`, `PreparedIntentVisibility.cs`, `ForeignIntentWriteResolver.cs` |
| Recovery | `Transactions/DurableTransactionRecovery.cs` |
| Aggregator | `KeyValues/Writes/` (`PartitionWriteAggregator`, `DurableProposalSubmission`, `IPartitionBatchExecutor`) |
| Replication | `Replication/` (`ReplicationTypes`, `ReplicationSerializer`, restorers) |
| Persistence | `Persistence/BackgroundWriterActor.cs`, `Persistence/Backend/` |
| Raft / WAL / HLC | Kommander (`/Users/andresgutierrez/kommander`, source-only reference) |

---

## 15. Mental model in one paragraph

A transaction stages its writes as per-key MVCC entries under a write intent, touching no Raft. At commit
the coordinator freezes an immutable description of the transaction — identity, one commit timestamp, a
decision deadline, and the exact committed value for every modified key — and drives durable-intent 2PC: the
anchor partition's record initialization is bundled with its own prepare into one proposal while the other
partitions prepare concurrently, the read set is validated, and a single compare-and-set on the canonical
record decides the outcome. That decision is the commit point the caller sees. Everything that makes the
value *visible* — materializing each intent and settling it — happens afterwards on a background task, and
any read or write that meets a still-pending intent in that window resolves it against the canonical record
(locally, or routed to the anchor leader) rather than serving a stale value. If the coordinator dies at any
point, the participant leaders finish or presume-abort the transaction from the same durable record.
