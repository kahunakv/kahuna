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

For persistent transactions that need a recoverable commit decision, the coordinator can also anchor
a replicated decision record to the transaction's first persistent modified key. If the live
coordinator disappears after that decision is installed, the anchor partition's leader can finish
the remaining participant commits.

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
| `ReadValidation` | Whether reads are tracked and checked for revision/write-intent changes at commit. | `TrackAndValidate` performs the read-set check; `None` takes last-value reads with no check. This is independent of the locking mode: an optimistic transaction with `None` skips validation, a pessimistic transaction with `TrackAndValidate` still validates. |
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
  persistent modified key into the working set. It routes the durable decision record after the live
  session is gone.

The anchor is a logical user key used for placement; the decision record itself is internal metadata
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
- lifecycle, current finalize attempt, 2PC state, record anchor, and durable-decision state.

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

`DecisionDurability.Durable` protects the transaction **after a commit decision exists**. It does not
persist the active coordinator session or participant prepare state.

The first confirmed persistent modified key becomes the record anchor. Durable prepare and commit
then use this order:

1. Prepare all non-anchor participants so their proposal tickets are known.
2. Prepare the anchor last, embedding an initial `CommitDecided` record with the frozen participant
   set.
3. Commit the anchor first. Applying that one committed proposal installs the anchor value, its
   completion receipt, and the decision record together.
4. Commit the remaining participants and advance their acknowledgement state on the anchored record.
5. Mark the record `Completed` after every participant is acknowledged.

Committing the anchor first is the point of no return. Before it commits, the prepared set may still
roll back. After it commits, the coordinator must never report a definite abort: any participant it
cannot finish is left for recovery and the caller receives `MustRetry` until the record reaches
`Completed`.

### Completion receipts

A persistent participant records a `CompletionReceipt` when its committed value is applied. A
duplicate commit can consult that receipt after the original MVCC entry and write intent are gone,
including after replication or restart. This lets recovery distinguish “already committed” from “the
prepare is missing.” A receipt lookup validates the full immutable receipt identity — transaction, key,
durability, and (when the caller carries it) the record anchor — so a persistent receipt never
satisfies an ephemeral request for the same logical key, and vice versa.

Receipts are restored from the committed key/value log and are also snapshotted before WAL retention
advances. They are not count-evicted. A receipt may be forgotten only after the anchored decision
durably acknowledges that participant. Forgetting is itself a **replicated participant-partition
operation**, routed to the leader of each participant *key's* partition (where the receipt lives, not
the anchor's) and applied on every replica via the shared receipt replication entry with a forget flag,
so followers drop the proof rather than accumulating it. The decision record persists
`ReceiptReleased=true` for a participant **only after** that replicated forget is durably acknowledged;
a forget that cannot be made durable leaves the participant pending for a later idempotent sweep.
Keep this ordering intact when changing progress persistence:

```text
participant value + receipt committed
        -> participant acknowledgement persisted on decision record
        -> receipt forget replicated on the participant partition
        -> ReceiptReleased persisted on decision record
```

### Decision placement and movement

`CoordinatorDecisionStore` replicates decision deltas on the data partition that currently owns the
anchor key. The record keeps logical participant keys, not fixed partition IDs, so recovery reuses the
current router after a split, merge, or leadership change.

Range split and merge orchestration hands both decision records and completion receipts to the
destination partition as part of the pre-cutover handoff. The handoff is **replicated onto the
destination partition's Raft log**, not merely written into the current leader's memory, so every
replica of the destination range holds it — a re-commit, re-drive, or finalize routed to the
destination after cutover still resolves even if the destination leader changes. Decision records ride
the same decision-delta replication as steady-state progress; completion receipts, which in steady
state ride their key/value commit and have no standalone log entry, get a dedicated replicated handoff
entry. Imports are idempotent by transaction/key identity, so a temporary duplicate copy during
handoff is safe.

The handoff **gates cutover**: if the receipt or decision transfer cannot be made durable on the
destination, the split or merge aborts before cutover instead of retiring the source range with the
only outstanding record lost. Range-lock transfer is separate and remains best-effort (locks are
in-memory, non-replicated actor state, hardened by a post-cutover confirm-and-reimport loop); only the
correctness metadata gates cutover.

### Recovery

Every node runs a `CoordinatorDecisionRecoveryActor`. A node drives only records whose anchor
partition it currently leads. The actor wakes periodically and when the node gains data-partition
leadership. For each outstanding record it:

- re-commits participants that are not acknowledged;
- uses completion receipts to recognize commits whose original response was lost;
- advances acknowledgement and receipt-release progress;
- marks a fully acknowledged decision `Completed`;
- removes completed records after the retention window and required release progress.

The request path and recovery actor may race. Participant commit, decision upsert, receipt removal,
and record removal are designed to be idempotent, so correctness does not depend on choosing one
winner.

### What durable mode does not recover

Durable mode has clear boundaries:

- If the coordinator disappears **before** the anchor decision is installed, the active session is
  lost just like a best-effort session.
- If a participant changes leader **after prepare but before commit**, its in-memory prepared value
  and proposal ticket may be gone. Recovery cannot recreate that prepare and can remain retryable.
- Ephemeral modified keys are rejected because neither the value nor a participant receipt can
  survive process loss.
- A read-only or otherwise unanchored transaction creates no decision record.

These boundaries are important when choosing `Durable`: it is durable decision/acknowledgement
recovery, not durable active-transaction or durable-prepare recovery.

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
- A durable session with an outstanding commit decision is never rolled back. Recovery finishes the
  record; the reaper releases its working set only after the record is `Completed`.

### Relevant bounds

| Setting or limit | Default | Purpose |
|---|---:|---|
| `TransactionOutcomeRetentionMax` | 10,000 | Strict maximum retained terminal outcomes; a non-positive value disables best-effort outcome retention. Independent of the durable-decision admission budget. |
| `DurableDecisionOutstandingMax` | 100,000 | Strict maximum **outstanding** durable decision records this node admits; a non-positive value disables the bound. Completed records do not count against it. |
| `TransactionOutcomeRetentionTtl` | 5 minutes | Age window for terminal outcomes and completed durable decisions; a non-positive value disables age-based removal. |
| `CollectionInterval` | 60 seconds | Tick interval for the transaction reaper and durable-decision recovery actor. |
| Pending operations per session | 4,096 | Fixed safety bound; additional registrations receive `RejectedCapacity`. |
| Participant in-doubt results | 8,192 per node | Fixed bound for normal acknowledgement-loss recovery. |
| Participant finalize retries | 20 retries, 250 ms apart | Fixed retry window used while committing or rolling back prepared participants. |

Durable admission is bounded by `DurableDecisionOutstandingMax`, counted over **outstanding**
(not-yet-completed) decision records only — completed records held for the idempotency window are
evictable retention and never consume admission budget, so retained outcomes do not throttle steady
durable throughput. A new durable transaction atomically reserves one slot before prepare (concurrent
admissions can never collectively exceed the budget); when the budget is full it is rejected before
prepare, and the coordinator never evicts recovery state to make room. This budget is deliberately
independent of `TransactionOutcomeRetentionMax` (the best-effort terminal-outcome cache).

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
9. If the operation adds a persistent mutation shape, trace it through prepare, commit receipt,
   replication/restore, decision recovery, and range split/merge movement.
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
- Once the anchor decision commits, rollback is no longer legal. Partial progress belongs to recovery.
- Participant sets in a written decision record are frozen; progress updates may change only
  acknowledgement, receipt-release, cleanup, status, and timestamps.
- A durable participant acknowledgement must exist before its completion receipt is forgotten.
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
  -> receipt + decision progress
  -> recovery / reaper / split-merge movement
```

A green happy-path test is useful, but the most valuable cases are acknowledgement loss, concurrent
finalizers, a remote coordinator, a leader change, and a crash after the anchor commits but before all
participants acknowledge.

---

## 11. Diagnostics and focused tests

The coordinator currently relies primarily on structured logs. Useful messages include:

- interactive transaction started, committed, or rolled back;
- unknown commit/rollback handles;
- unacknowledged operation completion retained for retry;
- read dependency revision changes and write-skew aborts;
- participant commit/rollback retry exhaustion;
- outstanding durable decisions deferred to recovery;
- decision replication, snapshot load/save, and recovery errors;
- completion-receipt snapshot load/save errors;
- split/merge receipt or decision handoff not durable, aborting cutover;
- abandoned-session reaping and cleanup failures.

The focused server tests are organized around the same boundaries:

| Area | Test file |
|---|---|
| Registry, digest, working-set copies | `TestTransactionOperationRegistry.cs` |
| Routing, deduplication, close/finalize fence, anchors | `TestTransactionRegistrationRouting.cs` |
| Locking, read validation, snapshot policy | `TestTransactionConcurrencyPolicy.cs` |
| Concurrent commit/rollback/finalize attempts | `TestFinalizeSlot.cs` |
| Public SDK session behavior | `TestClientTransactionRegisterRemote.cs` |
| Reaper/finalize-slot interaction | `TestSessionReaping.cs` |
| Completion receipt persistence and movement | `TestCompletionReceiptDurability.cs`, `TestCompletionReceiptTransfer.cs` |
| Decision persistence, movement, and recovery | `TestCoordinatorDecisionStore.cs`, `TestCoordinatorDecisionTransfer.cs`, `TestCoordinatorDecisionRecovery.cs` |
| Replicated, cutover-gating split/merge handoff | `TestSplitMergeCorrectnessHandoff.cs` |
| Durable commit and restart behavior | `TestDurableCoordinatorDecision.cs`, `TestDurablePersistentRestart.cs` |
| Partial commit and uncertain anchor outcomes | `TestDurablePartialCommitRecovery.cs`, `TestDurableAnchorUncertainty.cs` |

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
| `Kahuna.Core/KeyValues/KeyValuesManager.cs` | Register-remote wrappers, completion folding, operation-result recovery, and durable-decision recovery loop. |
| `Kahuna.Core/KeyValues/KeyValueLocator.cs` | Coordinator-key, participant-key, and anchor-key routing. |
| `Kahuna.Core/Communication/External/Grpc/KeyValuesService.cs` | External transaction and internal registration/finalize protocol endpoints. |
| `Kahuna.Shared/Communication/Grpc/Protos/keyvalues.proto` | Stable wire messages and enum values. |
| `Kahuna.Core/KeyValues/CompletionReceiptStore.cs` | Persistent-participant completion proof and receipt snapshots. |
| `Kahuna.Core/KeyValues/Transactions/CoordinatorDecisionStore.cs` | Anchor-partition decision replication, snapshots, import, and retention removal. |
| `Kahuna.Core/KeyValues/Transactions/Data/CoordinatorDecisionRecord.cs` | Durable record, participants, status, and progress fields. |
| `Kahuna.Core/KeyValues/CoordinatorDecisionRecoveryActor.cs` | Periodic off-mailbox recovery trigger. |
| `Kahuna.Core/KeyValues/TransactionReaperActor.cs` | Periodic abandoned-session cleanup trigger. |
| `Kahuna.Core/KeyValues/Handlers/TryPrepareMutationsHandler.cs` | Prepare tickets and embedded anchor decision. |
| `Kahuna.Core/KeyValues/Handlers/TryCommitMutationsHandler.cs` | Participant commit, receipt lookup/recording, and inline anchor-decision installation. |
| `Kahuna.Core/KeyValues/KeyValueReplicator.cs`, `KeyValueRestorer.cs` | Receipt and embedded-decision reconstruction on replication and WAL restore. |
| `Kahuna.Core/Persistence/BackgroundWriterActor.cs` | Receipt snapshot ordering before partition checkpoints advance WAL retention. |
| `Kahuna.Core/KeyValues/Ranges/RangeSplitter.cs`, `RangeMerger.cs` | Replicated, cutover-gating receipt and decision handoff across routing changes. |

---

## 13. Mental model in one paragraph

A transaction begins as an in-memory server session routed by a stable coordinator key. Every
interactive operation registers a unique declaration before touching participant state, then reports
only its confirmed effects back to the coordinator, which builds the authoritative working set and
deduplicates retries. The first commit, rollback, close, or reap closes registration and owns a single
finalize attempt while concurrent callers mirror its outcome; it drains earlier operations, freezes
the working set, and runs 2PC or cleanup from that snapshot. Best-effort outcomes remain replayable
only inside a bounded in-memory window. Durable mode anchors a decision to the first persistent
modified key, commits that anchor before secondary participants, records persistent completion
receipts, and lets the anchor partition leader finish an outstanding decision after coordinator loss.
The active session and participant prepares are still memory-bound, so durability begins at the
anchor decision—not at Begin.
