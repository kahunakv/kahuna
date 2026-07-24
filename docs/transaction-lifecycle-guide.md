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

### 6.4 The decision deadline

Each finalize freezes `deadline = commitTimestamp + clamp(multiplier × observed-finalize-p99, floor,
ceiling)`. The p99 is a rolling *local* stopwatch measure (a duration, not a distributed event), so the
deadline tracks real load. A commit attempt whose fresh attempt-HLC has passed the frozen deadline is
rejected by the state machine — the record stays `Undecided` and yields to presumed-abort recovery — and
increments `kahuna.durable_tx.late_commit_rejections`. A rising rate means the deadline is too tight.

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
- an **apply-on-commit callback**, which is the single ordered apply owner: it applies each record/intent
  delta to its store in Raft-commit order and reports whether every prepare took ownership of its key.

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
| A new transaction's prepare | Blocked (one live intent per key) until the predecessor settles — absorbed by the bounded prepare retry in §6.3. |

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

Only a conflict abort is `Aborted`. A prepare that did not replicate, a deadline expiry, a presumed abort,
an admission rejection and every infrastructural failure are `MustRetry`, so a caller never sees a false
conflict for a transient failure. See `specs/spec-mustretry-aborted-contract.md`.

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
`…outstanding`, `…resident_records`, `…admission_rejections`, `…late_commit_rejections`.

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
