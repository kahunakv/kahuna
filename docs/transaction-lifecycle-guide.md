# Kahuna transaction lifecycle guide

How a transaction executes in Kahuna, end to end: from the gRPC / REST / embedded entry point,
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

Both converge on the same commit machinery (`TransactionCoordinator.TwoPhaseCommit`). The script path
builds and finalizes the transaction without leaving the server between statements.

A **bare multi-statement script is one auto-commit transaction**. In
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

## 3. Two isolated keyspaces

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

> **Rationale.** Sharing those stores once caused a real bug: an ephemeral write to a key whose
> persistent namesake held a committed-but-unsettled intent materialized that foreign intent into the
> ephemeral entry and derived its revision from it, so a *first* ephemeral write reported revision 1
> instead of 0. It was visible only while deferred settlement left the intent lingering.

A `Durable` transaction that modifies any ephemeral key is rejected, because an in-memory mutation cannot
be made durable.

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
   are **single-threaded** (Nixie), which makes the per-key state machine race-free.

The range descriptor **generation** resolved here is carried forward and re-checked at dispatch (the
*fence*), so a split or merge between resolution and dispatch releases the operation retryably instead of
appending to a retired partition. A durable transaction's partition group carries one fence per key space
it touches — one key per range descriptor, each at the generation **that key** resolved to, plus one per
hash-routed space at generation zero — so a group that mixes a key-range row space with a hash-routed
schema-log space, or two key-range spaces at different generations, is fenced against every space and
never pairs one key with another key's generation (that pairing read as a moved range on every dispatch
and refused the commit on every round).

---

## 5. Staging: what an operation does before commit

Inside the owning `KeyValueActor`, a *transactional* write (one carrying a non-zero `TransactionId`) does
**not** propose anything to Raft. It stages:

- an **MVCC entry** for this transaction on the key (`entry.MvccEntries[transactionId]`) holding the new
  value/revision/expiry, and
- a **write intent** (`entry.WriteIntent`) with a lease (`DefaultTxCompleteTimeout`, 15 s), so concurrent
  transactions can detect the pending write during their own validation.

Reads inside the transaction see their own MVCC entry (read-your-own-write). Both `SET` and `DELETE`
advance the staged revision; a delete stages a `Deleted` tombstone at the next revision, preserving the
previous live revision for historical reads. `EXTEND` changes expiry without advancing the revision.

A first latest transactional `GET` or `EXISTS` pins the committed state, including a committed-but-unsettled
intent. If a competing commit advances the head, a follow-up read or write can answer `Aborted` immediately;
commit-time read validation is still required. This pin is per key, not a cluster-wide historical snapshot.
A fixed `ReadTimestamp` uses the separate as-of revision path.

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
- one canonical **commit timestamp** (HLC) and a **decision deadline**. The commit timestamp is minted
  strictly above the transaction id, above every participant's stamp on the staged writes, and above every
  *commit floor* the commit probe reported: the highest snapshot timestamp at which a read was served the
  committed head over one of the staged writes instead of waiting for the writer (see the
  [read semantics guide](transaction-read-and-lock-semantics-guide.md), fixed-timestamp reads);
- the participant manifest;
- per-partition groups of **`PreparedIntent`** — the exact committed value, revision, absolute expiry and
  `NoRevision` flag for each modified key.

If any modified key cannot be staged losslessly, the build fails and the transaction aborts rather than
committing through a lesser path.

The commit timestamp is also minted strictly above the transaction's **read dependency floor** (§6.1.1).

#### 6.1.1 Commit timestamp order

The coordinator mints the commit timestamp on its own clock. The clocks of two nodes do not agree. Kommander
carries a clock lead from one node to another only in Raft appends and acknowledgements, so a node follows the
fastest clock about one heartbeat behind (500 ms by default in `kahuna-server`, longer on a quiesced partition).
These rules state which conflicts between two transactions T1 and T2 the commit timestamps put in order.

**Write-write on one key.** Each mutation of a key has a later commit timestamp than the mutation before it. See
[Delivery and ordering](keyvalue-watch-streams-guide.md#delivery-and-ordering) in the watch streams guide.

**Write-read on any partitions.** T1 commits a write at c1. T2 reads that value and commits at c2. Then c2 is
above c1, also when the key T2 read and the keys T2 wrote are on different partitions with different leaders.

- Each read registered with the coordinator reports its read dependency on its completion: the highest of its read
  timestamp and the `LastModified` of every entry it served (`OperationCompletionPayload.ReadDependency`). A scan
  counts every row it served, tombstones included.
- The coordinator keeps the highest read dependency of the transaction (`TransactionContext.ReadDependencyFloor`)
  and mints the commit timestamp strictly above it.
- A script transaction raises the same floor from its `GET`, `EXISTS`, `GET BY BUCKET` and `SCAN BY PREFIX`
  statements.

Without the floor, T2 can commit below c1 when T1's coordinator clock runs ahead and no Raft message carried that
lead to T2's nodes yet. A snapshot read at a timestamp between c2 and c1 then sees T2's write and not the value T2
read. `TestCommitTimestampReadDependency` reproduces that case: T1's commit is 3 s ahead of every clock.

The floor does not cover these reads:

- A read without a coordinator key. It is not registered with the coordinator.
- A latest read that finds a deleted key. The answer carries no entry, so it has no timestamp. A read at a
  timestamp (a statement read or an as-of read) gives its read timestamp.
- A read retried under an operation id whose first completion already landed. The read runs again, but the floor
  keeps the value of the first read.

**Read-write anti-dependency.** T1 reads a key, T2 then overwrites that key, and T1 commits after T2. No order is
promised: T1 can commit at a later timestamp than T2. When T1 read at a timestamp, the snapshot clock fence stamps
a later write on the leader that served the read above that timestamp. T1's commit timestamp has no relation to
T2's.

**No persistent write.** A transaction that wrote no persistent key through the durable path has no commit
timestamp, and its commit answer carries zero. An all-ephemeral transaction commits at a manual commit id, which
the answer does not return.

**Every commit answer of a durable commit** carries the same commit timestamp: the first answer, a duplicate
commit that replays the retained outcome, a mirrored concurrent commit, a duplicate commit after the reaper or a
rollback found the transaction committed, and a commit answered from the transaction's record after the
coordinator lost the session.

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

0. **Claim the staged intents.** Before the first proposal, one batched probe per node asks each written
   key's leader whether the transaction's staged write is still covered by the intent it was staged under,
   claims that intent for the commit, and reports its commit floor. Until the claim, a snapshot read that
   meets the staged write is served the committed head and raises the floor instead of waiting; from the
   claim on it waits. A lost intent decides nothing here (step 3 refuses it and names the cause);
   a floor at or above the frozen commit timestamp answers `MustRetry` with the floor, and the coordinator
   freezes the input again above it and runs the finalize again (`kahuna.transactions.commit_floor_refreezes`).
   The claim precedes every proposal that carries the commit timestamp, so no read can push a floor above
   it afterwards. On the one-phase bundle (§6.3.1) the up-front validation's probe is the claim.
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
   commit is not aborted because background settlement had not caught up. Between two rounds a helping pass
   settles the holders whose record is already terminal and reports what holds the keys. The pass runs on the
   leader of the refused partition: here when this node leads it, otherwise through `SettleDecidedBlockers`,
   an inter-node call bounded by 2 s. A session is finalized by the node that began it (§6.6), so after a
   leader change, or for a transaction over several partitions, that leader is another node.
   `kahuna.durable_tx.helping_forwards{result}` counts the calls; every result but `inspected` leaves the
   refused prepare to the eight-round budget. A node of an older version does not know the call
   (`unsupported`).
3. **Validate** the read set — revision comparison plus a concurrent-writer probe, read through the
   intent-aware path. Only meaningful once every prepare is durable.
4. **Decide.** Compare-and-set the canonical record: `Commit` only if every prepare is durable *and*
   validation passed; otherwise `Abort`. This is the point of no return. The finalizer reports whatever the
   record *became* — a concurrent recovery abort can win the race.
5. **Resolve** — install committed values and settle (resolve + remove) their intents, or discard aborted
   staging and settle aborted intents. By default (`DurableMaterializeOnResolve`) the settlement apply installs
   the values itself; with the option off, commits write materialization records before settlement. **When** this runs
   is §8; the two encodings and their upgrade requirements are in the
   [durable settlement guide](durable-settlement-guide.md).

**The staged-base check before the first proposal.** Before step 0, and on the one-phase path again just
before the bundle is proposed, the finalizer compares the validated base of each read-modify-write key with
what the coordinator node knows. A committed head in the node's memory at or above the staged revision of any
write is a conflict. A foreign intent on the key is judged by its record:

- An undecided holder decides nothing, and the attempt answers `MustRetry`.
- A committed holder that is the validated base is accepted: the same revision for an existing base, and a
  committed delete or a value that expired before the transaction began for an absent base.
- A committed holder above an existing base, at its revision in another state, or a committed value over an
  absent base is a moved base, and the commit aborts with `StagedBaseMoved`.
- A committed holder below an existing base is history that the base was written over. Its settlement has
  not applied on this node yet, for example after a non-transactional write installed it and wrote the next
  revision (§8.1), or on a replica behind the leader of the key's partition. The key is then judged as a key
  with no intent, and `kahuna.durable_tx.staged_base_superseded_holders` counts it.

A key with no intent, or with an aborted holder, is compared on its leader by the staged-base conflict
probe. An absent base records no revision, so a committed value on the key stays a conflict until it
settles, also under a later delete. This check is an early one: the fence at the prepare's apply position
and the bundled commit gate judge the base again.

#### 6.3.1 The one-phase bundle and its gate

When the whole participant set is the anchor partition, steps 1, 3 and 4 collapse into one durable
barrier: the read set is validated up front and `[record init, prepare, commit decision]` is proposed as
one atomic batch. The validation's commit probe is also the claim of step 0: it reports the staged
intents' commit floor, and a floor at or above the frozen commit timestamp refuses the attempt before the
bundle is proposed, for a fresh freeze above it. When another node leads the anchor partition, the bundle crosses the wire whole as one
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

**A written key that another transaction's intent still holds.** The bundled prepare is refused over a held
key, and the decision in the same batch cannot be withheld, so the attempt reads the intent store before it
validates. With deferred settlement (§8) the usual holder is a transaction that committed a moment before and
whose settlement has not applied yet: a counter, the head of a queue, one hot row. Such a holder does not end the
attempt. The bundle carries its settlement as the first entry of the batch, `[settlement of the holder, record
init, prepare, commit decision]`. The settlement installs the holder's value and records its revision as the
committed head of the key before the prepare applies, and the base and the staged revision of the bundle are
judged against that head. The settlement is the one the holder's own resolution proposes; whichever of the two
applies second changes nothing. The attempt waits for nothing: it reads the holder's record on this node, and
right before the propose it leaves out each holder that settled in the meantime.

The bundle settles a holder when all of these are true:

- `OnePhaseApplyTimeValidation` is on. Without it the batch commits whatever the staged-base fence says about its
  prepare, and a holder that moved the base would be overwritten.
- `DurableMaterializeOnResolve` is on. The other settlement needs a materialization record and a confirmed apply
  before it (see the [durable settlement guide](durable-settlement-guide.md)).
- The holder's record is a commit, and this node leads the partition of that record.
- This node leads the anchor partition. A node that does not lead it sees the holder on a replica at most.

Any other holder (undecided, aborted, or decided on a record that another node leads) falls back to the two-phase
flow with the reason `foreign_intent`, and the helping pass of step 2 deals with it.
`kahuna.durable_tx.one_phase_carried_settles` counts the intents whose settlement a committed bundle carried.

A leader change between the validation and the propose refuses the bundle, as it does without a holder: the bundle
is fenced to the term the validation was made under. The attempt answers a retry and nothing is durable.

**One log entry for the bundle.** With `DurableOnePhaseBundleEntry` on (server option
`--durable-one-phase-bundle-entry true`), the anchor leader proposes the record init, the prepare and the commit
decision as one log entry of the type `onephasebundle`, in place of three entries. The entry holds the three deltas
in their own encodings, in apply order. Every replica applies them at the position of the one entry, to the same
stores and in the same order as the three entries: the gate of the decision reads the intent store that the prepare
left, and the rejection memo, the stale-base vetoes and the term comparison do not change. A carried settlement of a
holder stays an entry of its own, ahead of the bundle entry, in the same submission. A checkpoint or a snapshot
boundary cannot fall between the prepare and the decision of a one-phase commit.

The leader that proposes the commit selects the shape by its own setting, so a forwarded one-phase commit takes the
shape of the receiving leader. A node of this build applies the entry whatever its setting. An older build skips the
entry with no error and then has no record and no intent of the transaction, so turn the option on only after every
node runs a build that applies it. The leader also asks each voter and learner of the partition which log types it
applies, when it takes the leadership, when the replicas change, and at an interval while one of them did not
confirm. While a replica did not confirm the type in the current term, the leader proposes the three entries, and
`kahuna.durable_tx.one_phase_bundle_guard_fallbacks` counts each such commit. The check does not see a node of an
older build that joins later or starts again on an older build between two asks, so the upgrade order stays the
rule. Turning the option off is safe at any time. An older build is safe again only when no log and no backup
segment that it can read holds a bundle entry.

On the receiving leader, a forwarded durable request (an operation, a bundle, a one-phase bundle, a
decision or a record lookup) starts as soon as the inter-node stream delivers it. It does not wait for
the earlier durable requests on its stream, so concurrent forwards reach the partition's write
aggregator together and can share one Raft round, as the commits of a local coordinator do. Each request
is self-contained: it carries its own term and range fences and its transaction identity, and the record
and intent state machines decide duplicates and races at apply.

Nothing on the stream limits how many forwarded durable requests a receiver holds at once. The write
aggregator's admission bounds the part of each request's life spent admitted to it
(`kahuna.kv.write.queued_items`, rejections on `kahuna.kv.write.rejections{reason}`), but a request also
spends time outside it: resolving the partition's leadership or redirecting to another leader before
admission, waiting for the ordered apply of its committed batch, applying a commit or rollback resolution
on the key-value actors (which never enter the aggregator), and writing its answer.
`kahuna.internode.inbound_durable_in_flight{request}` counts the forwarded durable requests a node has
started and not yet answered, by type (`durable_operation`, `durable_bundle`, `durable_one_phase`,
`durable_decision`, `lookup_record`), across all of those stages. It returns to zero when the node's
inbound durable work drains; a value that keeps rising while the senders' load is steady means the receiver
is accepting forwarded work faster than it answers it.

The placement requests on the same stream (seed or remove a key range, export and import range locks,
import completion receipts, copy a range page, read a range's transaction state) do run on an ordered
lane: one at a time per stream, in arrival order, each holding its turn until it answers. The receiver
records the wait of each on `kahuna.internode.ordered_lane_wait_ms{request}`: one sample per lane
request, from the moment the read loop gives the request its turn to the moment its predecessor
completes. Durable requests take no turn and record no sample there.

### 6.4 The decision deadline

Each finalize freezes `deadline = commitTimestamp + clamp(multiplier × max(observed-finalize-p99,
observed-per-intent-p99 × intents), floor, ceiling)`. Both p99s are rolling *local* stopwatch measures (a
duration, not a distributed event), so the deadline tracks real load. The per-intent term prices the
manifest by its own size, so a bulk write after a stream of small commits is not held to a margin sized
for those commits; it is fed by finalizes of 64 or more intents, where the fixed cost is amortized, and
by every finalize until one of those is in the window. The estimates are live from the first sample;
before it the floor applies. A commit attempt whose fresh attempt-HLC has passed the frozen deadline is
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

A frozen input whose key range moved is the same dead end. Every prepare and every record transition is
fenced with the partition and range generation each key was frozen against, and a split between the freeze
and the dispatch releases it at the leader instead of appending it to a partition the key left. After the
cutover nothing stamped with the frozen route can land anywhere, so a retry under the same handle is
concluded through the record fence before any prepare is re-driven, exactly like a retry past the deadline.
The fence itself follows the record: a split's hand-off carries the records anchored in the moving half to
the child partition, so the abort is sent to the anchor key's current partition under its current
generation. A rollback and the session reaper run the same fence, which releases the claimed write
intents of an abandoned attempt on every key it touched, on the half that moved and on the half that did
not; before this, such an attempt could not be concluded at all and its intents blocked writers and snapshot
reads of those keys until the session liveness ceiling.

**The deadline is a lease a live coordinator renews.** A finalize that is alive and inside a productive
wait — its prepare refused only by predecessors whose records are already decided and whose intents are
still settling, or a prepare and validation that consumed most of the margin — proposes an
`ExtendDecisionDeadline` transition on the canonical record, moving the deadline to *now + the frozen
margin*. The record accepts it only while the lease is still held (the renewing attempt's HLC has not
passed the current deadline) and only forward, so a dead coordinator is presumed aborted exactly as before
and a lapsed lease cannot be revived. A renewal is sent only when less than half of the lease remains, so a
healthy commit never pays the extra record round; each attempt renews at most
`DurableDecisionDeadlineMaxExtensions` times (0 disables renewal, which a mixed-version rollout needs because
an older node cannot decode the transition). The check runs between the prepare loop's rounds and, while the
first prepare barrier, a helping pass, a re-prepare or the validation of an attempt that already waited is in
flight, every quarter of the lease in wall-clock time — a single round over thousands of intents, or a prepare
queued behind a saturated partition, can outlast the half-lease on its own and used to let the lease lapse in
the middle of a productive wait. A due renewal that could not be replicated is sent again after 25 ms while the
lease still holds, up to 64 times: the write scheduler that is saturated enough to turn a renewal away is the
one holding the predecessors the attempt waits for. The scheduler never releases a terminal-class submission
for queue age, so a renewal, a decision or a settle that queued behind a burst is proposed however long it
waited, where an ordinary write is released and resent by its producer. The recovery sweep reads the deadline
off the record, so a renewed record is left alone until the renewed deadline.
`kahuna.durable_tx.deadline_extensions{outcome}` counts the renewals.

**Settlement lag aborts only when the lease cannot be held.** The prepare retry loop (§6.3) reads the
helping pass's report: when every holder of a refused key is a transaction whose record is already
terminal, the wait is finite and the loop is bounded by the (renewed) deadline instead of its eight rounds.
If the lease still lapses with renewal on — the renewal budget is spent, or a renewal could not be
replicated — the attempt records the abort itself with class `RetryableFailure` and refusal `SettlementLag`,
naming the rounds, the renewals and the holders (`kahuna.durable_tx.prepare_retry_loops{outcome=deadline_lapsed}`);
a `MustRetry` there would only be fenced into a presumed abort by the next same-identity retry, since the
record's gate rejects every commit whose attempt clock passed the deadline. With renewal off the attempt
answers `MustRetry` with class `SettlementLag` and its prepares installed, and the retry, the reaper or a
rollback concludes it through the record fence. A holder that is still undecided is a
live conflict and keeps the eight-round budget, after which the loop records the abort with class
`PrepareExhausted` and logs a warning naming the transaction, its intent count and what held its keys.

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
waits on an apply it is not going to see. Once a second, one ask is sent with the full budget as a probe;
three consecutive attesting probes restore the replica (a relapse within 30 s doubles that requirement,
up to 24).

**A replica attests from its memory, not only from the applied prepare.** The ask carries the judged
fields of every fenced intent (`KeyValueStagedBaseProbe`: the validated base revision and state, the
staged revision, and whether the write stages a revision). A replica that holds the transaction's
still-pending intent judges the intent, exactly as its apply did. A replica that does not hold it —
the prepare has not applied there yet, or the node does not host the partition — judges the probe
against its committed-head memory and answers `Clear` or `StaleBase` at once
(`kahuna.durable_tx.replica_fence_memory_verdicts`), instead of waiting for the apply and then answering
`NotApplied`. The memory verdict is sound: heads only advance, and the key's head is frozen while the
transaction's own intent is live, so the memory now is at or past the memory at the prepare's apply
position, and a refusal from memory is one the apply-time fence gives too. The one head that is not a
competitor's — the transaction's own settlement, already applied on that node — answers `Clear` through
the settled-identity memory. That memory is fed by the settle whether or not the intent is still present
when the settle applies, and a prepare is rejected as a settled replay when any slice of the node
remembers its identity: a key that moved between its prepare and its settle has the two in different
logs, and a node that applies the destination's log first must not re-install the leg from the source's
(see the [key-range sharding guide](key-range-sharding-guide.md)). The same guide describes the carrier
of a live intent: a prepare is refused as held by another transaction only by an intent that the same
partition's log carried, and the fence does not judge a replayed prepare of a moved key against a head
of another partition's slice that is its own commit.
So apply lag no longer leaves a commit unattested; only an unreachable
replica (or one on a build that ignores the probes) still does, and the lag breaker still bounds how
long such a replica can hold a commit. Under a range-split cascade, where replicas lag the prepares of
freshly created partitions, this closes the window in which the detached stale-base veto raced
the commit and lost (`kahuna.durable_tx.stale_base_vetoes_late`); the hand-off of committed heads on a
range move (see the [key-range sharding guide](key-range-sharding-guide.md)) removes the other half of
that window, the destination leader's missing memory of the moved keys.

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

### 6.6 Session ownership and routing

An interactive session lives on the node that began it — the leader of its coordinator key's partition at
`BEGIN` — and stays there when that partition's leadership moves: the session's finalizer forwards its durable
work (the anchor bundle, the decision, materialization) to whichever node leads the partition now. The routed
entry points (`LocateAndCommitTransaction`, `LocateAndRollbackTransaction`, operation registration and
completion, `LocateAndGetTransactionWorkingSet`, `LocateAndCloseTransaction`) therefore serve a session **this
node owns** locally whatever the current leader is. Before this rule, a leader that stepped down routed every
commit for its own sessions to its successor, which had no such session, and each spun as `MustRetry` until the
client's deadline (~100 indeterminate commits per leader pause under a paused disk).

A leader that receives a commit, rollback, close, working-set query, operation registration or completion for
a session it does not hold (the session began on the previous leader) offers it to its peers once, in turn;
the owner serves it and every other peer answers unknown. The probe runs whether the request reached the leader
directly or was forwarded to it, which is how a client's commit arrives when the client talks to the data leader
rather than to the coordinator leader. The forwarding node never probes. A probe is terminal
(`ForwardedRequestScope.EnterTerminal`): the peer answers for itself, sends the request to no other node and
starts no probes of its own. A request therefore costs at most two forwards and one probe of each peer, whichever
node the caller reached and whether or not any node holds the session. Before the forwarded request was admitted,
a commit the data leader forwarded to the new coordinator leader was answered as an unknown transaction, and the
session's range locks stayed on the data leaders until their lease ran out. The membership roster does not carry
peers' node ids, so the transaction id cannot name the owner directly.

All of these calls land on the receiver's routed entry point, on both inter-node transports, so the rule above
holds for a forwarded request as it does for the caller's own. The inbound completion landing point follows the
same ownership rule: a node that holds the session folds the completion whatever the current leader is, so the
owner a leader found among its peers does not redirect the completion back to that leader. The working-set query
and the close landed on the receiver's node-local session table before, so a third node that forwarded one of
them to the new leader read no working set, or `Errored`, for a session its owner still held.

A close answers `MustRetry` when the owner answered it (an operation is still pending at the drain deadline, or
another finalize holds the session) and when a peer could not be reached. It answers `Errored` only when every
peer answered that it does not hold the session.

### 6.7 Record-less intents past the retention horizon

Past `TransactionOutcomeRetentionTtl` an absent canonical record no longer means "never initialized": a
terminal record may have been reclaimed. The recovery sweep never presumes abort for such an intent (that would
discard a reclaimed commit's leg). It decides from the leg's **completion receipt** — written only when a leg's
value materialized, which only a committed transaction does — and settles the intent as a commit when one
exists (`kahuna.transactions.recordless_intent_receipt_commits`). Without a receipt the intent is held: its key
stays read-only until the outcome can be proven. Holds are counted in
`kahuna.transactions.recordless_intents_held` (the last pass's count on this node) and summarized in one
warning
per recovery pass naming a few of the keys; a non-zero gauge is an operator signal, not a counter. The record
GC on the anchor leader never reclaims a terminal record while a prepared intent of that transaction is still
resident on the same node, so the hold can only arise for a leg resident elsewhere.

Before holding, the sweep asks the partition's other replicas whether they still hold the intent
(inter-node read `GetPreparedIntentPresence`, 2 s per peer, at most eight intents per partition per pass).
The intent stores apply from the log alone, so a **majority of the replica set** (counted with this node as a
holder) that no longer holds the intent, each at or past this node's applied kv log id, proves the settlement
is in the log below what this node marked applied: the hold is this replica's divergence, not the
transaction's ambiguity. A peer behind this node's applied id is unknown (its "not held" may be "not prepared
yet"), and with two replicas no majority is possible. The intent stays held either way — nothing is presumed
locally; the settled value lives on the peers — but a stale hold is logged at error level
(`Recovery: prepared intent for key … is held record-less past the retention horizon, but 2 of 2
consulted replicas (…) … no longer hold it`),
counted in `kahuna.transactions.recordless_intents_stale_detected`, and the partition is handed to divergence
containment (leadership fencing guide, *Containment*): gated on this node, leadership transferred to a
replica that settled the intent, keys served from there. Containment then has the replica re-seeded
(a requested whole-partition install from the leader), which replaces its intent slice with the peers'
settled state: the holds drop to zero when the gate lifts.

### 6.8 Commit call timing

A node times each commit call of an interactive transaction, from its entry into
`LocateAndCommitTransaction` (or the local `CommitTransaction`) to its answer. A script commits inside its own
execution and is not recorded here.

| Instrument | Kind | Tags | Content |
|---|---|---|---|
| `kahuna.transactions.commit_call_ms` | histogram | `path`, `outcome` | Wall time of the call. One sample for each call that answers. |
| `kahuna.transactions.commit_stage_ms` | observable counter | `stage`, `path` | Total time the calls spent in each stage since the node started. A pair that no call has used is not reported. |

A call is in exactly one stage at any moment, so its stages add up to it. For one path, over one interval:

- The increase of `commit_stage_ms`, summed over `stage`, equals the increase of the sum of `commit_call_ms`,
  summed over `outcome`.
- The increase of `commit_stage_ms` for one stage, divided by the increase of the count of `commit_call_ms`, is
  the mean of that stage. The stage series has no distribution.

| Stage | Content |
|---|---|
| `route` | From the entry of the call to the coordinator: the check for the session's owner and, for a session this node does not hold, the leadership check, the probe of the peers and the forward |
| `session` | The session lookup and the finalize slot. A call that does not own the finalize spends its time here |
| `drain` | The wait for the operations of the session that are still in flight |
| `freeze` | The checks before the commit, the pin of a yielding transaction's intents, the commit timestamp, the build of the finalize input, the admission gates and the one-phase eligibility |
| `preflight` | The staged-base validation before the first proposal (`finalize_preflight_ms`) |
| `encode` | The serialization of the record init and of the prepares, and the one-phase gate |
| `attempt` | The one-phase attempt: `one_phase_pre_bundle_ms` plus `one_phase_bundle_ms` |
| `apply` | After the reply of the bundle: the verdict, and the apply of the committed values on the leader |
| `prepare` | Two-phase: the claim of the staged intents, the prepare barrier and its retries |
| `validate` | The commit-time checks of the two-phase, read-only and ephemeral paths. The one-phase path runs its checks inside `attempt` |
| `decision` | A terminal transition of the record on its own round: the two-phase decision, an abort driven through the record, or the fence of an earlier attempt |
| `resolution` | The hand-off of the resolution to the background, or the whole resolution when deferred settlement is off |
| `ephemeral` | The in-memory prepare, probe, commit and rollback of ephemeral keys |
| `release` | The release of the transaction's locks and read snapshots (§6.9). With `AsyncRelease` the part after its first wait is outside the call |
| `answer` | The outcome on the session, the retained outcome, the end of the session and the publication to the callers that wait for it |

| Path | Content |
|---|---|
| `one_phase` | The one-phase attempt gave the outcome |
| `two_phase` | The two-phase flow gave the outcome. Its `attempt` stage has time when a one-phase attempt fell back |
| `ephemeral` | Every written key is ephemeral |
| `read_only` | The transaction wrote nothing |
| `mirrored` | Another call owned the finalize and this one answered its outcome |
| `replayed` | The session was gone, and the answer came from the retained outcome or the durable record |
| `forwarded` | Another node served the call. That node records it too, so exclude this path to count commits across a cluster. The time of a forwarded call contains the hop and the time of the serving node |
| `none` | The call ended before a path was chosen: a drain that timed out, a refusal by a check or an admission gate, a fence of an earlier attempt, or a session that is being reaped |

`outcome` is `committed`, `aborted`, `rolled_back`, `must_retry` or `errored`. A finalize that throws keeps its
time in the stage it threw in.

The existing histograms split some stages further: `finalize_validate_ms`, `finalize_first_prepare_ms`,
`finalize_decision_replicate_ms` and, for the wait of the bundle in the write aggregator,
`kahuna.kv.write.submission_queue_delay`. `finalize_prepare_ms` starts at the entry of the finalize, so without a
one-phase attempt it also contains `preflight` and `encode`.

A call reads the clock once at each stage boundary and records one histogram sample. The stages add to totals
that a listener reads when it collects, so they cost no listener callback for each commit. With no listener on
either instrument the call opens no clock.

### 6.9 Working-set release

When a transaction ends, its coordinator releases what the transaction still holds: each point lock, each
staged write, each read snapshot, each prefix lock and each range lock. The per-key items go to the leader of
each key in one batched request. A release of a key removes the transaction's write intent and its MVCC
entry. For a yielding transaction that lost its intent on the key to a takeover, the release removes the
record of that loss. It removes nothing else.

A one-phase commit applies each committed value on the leader before it answers (the `apply` stage). That
apply removes the same write intent and the same MVCC entry, in the turn of the actor that owns the key. The
actor says so in its answer, and the coordinator then sends no release for the key. A read-modify-write
commit on the leader of its keys sends no per-key release at all.

A key is left out only when all of these are true:

- The commit took the one-phase path and its decision is `Commit`.
- The apply of the key ran on the node that holds the session. An apply that is forwarded to another leader
  answers only whether it succeeded, so its key keeps the release.
- The apply found the write intent of the transaction on the key and removed it. An apply that found the head
  already in place, or found no intent of the transaction, reports nothing.

Everything else is released as before:

| Item | Release |
|---|---|
| A key the transaction only read | Sent. The commit applies nothing on it |
| A point lock on a key the transaction did not write | Sent |
| A prefix lock, a range lock | Sent, each with its own request |
| Every key of an abort, a rollback or a reaped session | Sent |
| Every key of a two-phase commit | Sent. With deferred settlement its apply runs after the answer, so the release is what clears the intent |
| A written key whose apply failed, ran on another node, or found no intent | Sent |

A key that is left out is not a pending release: it does not enter the list that the detached retry sends
again (`kahuna.transactions.lock_release_retries`).

The leader of a key can change after the apply. The node that stops leading a partition drops its write
intents and MVCC entries for that partition, and a release goes only to the current leader, so the new
leader holds nothing of the transaction with or without the release.

`kahuna.transactions.working_set_release_keys{result}` counts the per-key items: `sent` for a release that
was sent, and `commit_applied` for a written key that needed none. A one-phase workload whose written keys
stay under `sent` commits on nodes that do not lead those keys.

---

## 7. The partition write aggregator

Every durable record, prepared intent, settlement delta and materialized key/value record reaches Raft
through the **partition write aggregator**, not by proposing directly. Concurrent transactions targeting
the same partition coalesce into one `ReplicateEntries` proposal, which makes cross-transaction
batching possible.

A submission carries:

- an **ordered list of entries** (which is how the anchor `[init, prepare]` bundle is expressed atomically);
- an **admission class** — `Ordinary` (record init, prepare) or `Terminal` (decision, materialize, settle).
  Terminal work draws on reserved capacity so an ordinary-write burst can never starve the step that
  *finishes* an already-prepared transaction;
- an optional list of **fences** (one key + generation per range descriptor or hash space the group's keys
  resolved through) re-checked at dispatch, so a split/merge of any of them since freeze releases the
  submission retryably; the anchor record's own fence rides with the anchor group's;
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

A deferred settlement that does not land is sent again, up to five times over about two seconds, through the
current routing of its keys. The usual reason is a leadership that moved between the routing of the settlement
and its proposal, which the proposal answers with `NodeIsNotLeader` or `TermMismatch`. Every send reads the
direction from the canonical record again, and a settlement that landed before applies as a no-op. A node that is
shutting down sends nothing again. `kahuna.durable_tx.settle_resends{result}` counts the intents sent again, and
`kahuna.durable_tx.resolution_settle_failures` counts the intents that stayed unsettled, which the partition
leader settles later (§9).

### 8.1 The settlement window

Between the durable decision and settlement, a committed transaction's value lingers as a **prepared
intent with resolution `Pending`**. Anything touching that key in the window must resolve it correctly:

| Operation | How it resolves |
|---|---|
| Point read / exists | `DurableReadVisibility` → `PreparedIntentVisibility`. Committed intent → serve the intent's value (a committed *delete* or an expired value reads as does-not-exist); aborted → ignore; **undecided → wait**. |
| Range / bucket scan | The scan overlays the intent window and resolves the whole set at once via `TryRouteForeignScanDecisions` + `DurableReadVisibility.ScanDecision`. A latest scan judges each intent as a latest point read does, whatever the intent's commit timestamp: a coordinator with a faster clock mints a commit timestamp above the clock of the serving leader, and the scan still serves that committed value, and still waits when the intent is undecided. Only a scan at a read timestamp leaves out an intent that commits above that timestamp. The window is **captured on the actor before every disk page is read** (`RangeScanContinuation.CaptureIntentsAhead`, the bucket scan's stage 1) and each key's earliest capture is kept for the final overlay: a settlement that applies between a page's read and the overlay removes the intent and lands the value in the base, and a page read before it would otherwise be overlaid with neither (a committed insert missing from the scan, a committed delete still in it). The staging leader never shows this, since its in-memory write intents make the page wait; a newly promoted leader, where the predecessor's commits settle late through the handover, does. A snapshot scan also keeps a committed intent under a head it cannot see (`DurableSnapshotSource.HeadSupersedesIntent`): the merge judges the page's own row against the intent by revision. |
| First transactional read of the key (no MVCC entry yet) | Does not answer from the overlay: it records its MVCC pin from the committed intent (`DurableSnapshotSource`), exactly as a transactional scan does, so a competing commit that lands afterwards makes the transaction's next read or write of the key answer `Aborted` early rather than only failing read-set validation at commit. Undecided → wait. |
| Write (set/delete/extend) | `ForeignIntentWriteResolver` materializes a committed intent into the entry *before* the write derives its next revision, flags and existence checks. Undecided → `WaitingForReplication` naming the holder: lag, not a lock conflict (the holder is inside its decision window and waits on no one), so a client can give it a longer budget than a deadlock wait. The same answer covers a live write intent whose owner is already durably decided and only waiting for its settlement to clear it; a live undecided write intent, a prefix lock and a foreign range lock keep `MustRetry`. |
| Any read after a later write moved the head past the intent | A non-transactional write does not wait for settlement: it materializes the committed intent and writes the next revision, and the intent lingers until settled. A head **strictly newer** than the intent wins everywhere — point reads (latest and snapshot; a persistent cache miss loads the head first), transactional pins, and scans (a newer row stands, and a newer delete stays deleted) — so a client never reads back the value its own write replaced. An equal revision is not proof (an extend reuses it), so there the intent is served. |
| Staged-base check of a read-modify-write that read the newer head | The intent is below the validated base, so it is history that the base was written over: the check before the first proposal compares the base on the key's leader, as for a key with no intent (§6.3). |
| Point lock acquire | Resolved exactly like a write before the lock is granted: a committed intent is materialized into the entry, so the new holder never holds a key whose committed head it cannot see; undecided → `WaitingForReplication`, which the acquire loop waits out (routing the holder's decision when it is not local). A wait budget that runs out answers `WaitingForReplication` too, never `MustRetry`. |
| Transactional read that already holds its own MVCC entry on the key | Skips the overlay (its own view is authoritative), but if a committed intent is newer than the resident entry, the pin is behind the committed head and the read answers `Aborted` — the same answer it gives once the entry has advanced. It never returns the pinned pre-commit value. |
| A new transaction's prepare | A one-phase bundle carries the settlement of a committed predecessor and applies behind it (§6.3.1). Any other prepare is blocked (one live intent per key) until the predecessor settles — absorbed by the prepare retry in §6.3, which a decided predecessor bounds by the decision deadline (renewed while the wait lasts, §6.4) and a live one by eight rounds. |

**Settlement never erases another transaction's lock state.** A commit that settles after it released its own
intent can land on a key that another transaction has since locked and read. The apply advances the entry,
but it keeps that transaction's MVCC entry (and, on the leader's durable apply, its lock): the entry records
the base the transaction read, and it is what makes that transaction's next read or write of the key answer
`Aborted`. Deleting it would let the transaction re-pin at the new head and write a value computed from the
superseded one over the commit — a lost update that every later check would accept.

A leader change does delete it (`DropLeaderStateHandler` drops every staged entry of the partition the
node stopped leading), and the new leader has never seen the transaction, so the re-pin happens there. The
coordinator closes that hole: it holds every staged revision, and it refuses the commit (`Aborted`,
`Lost staging: …`) when a key is restaged at a revision that does not continue the earlier one, or when a
point read of a staged key answers anything but the staged revision. See
`docs/leadership-fencing-and-apply-fingerprint-guide.md`.

The same leader change drops the transaction's locks, and the pin of a key it only read with them. A
reader whose Shared range lock was lost re-pins at the new head on its next read and sees a second
committed state of the key; a writer that read under that lock and then upgrades is granted a fresh lock
over a competitor's commit. Neither case leaves a staging to compare, so the coordinator proves the locks
instead: every lock grant reports the Raft term it was issued under, and the commit is refused (`Aborted`,
`Lost lock: …`) unless every partition the transaction holds a lock on is still led under that term. The
same guide describes the proof and its counters.

Both refusals are a *lost exclusion*, not a conflict: no other transaction need have touched the keys, and
nothing the transaction staged was committed. The durable record classes the abort as `LostExclusion`.
What the caller gets depends on the entry point. An interactive session gets a terminal `Aborted`, because
its reads were made under the lost lock and the leadership term will not return: start a new transaction.
A script gets `MustRetry`: it is self-contained, so a new run takes new locks under the new leader and
reads again, which is exactly what the client layer already does for `MustRetry`. Leader churn alone
therefore never surfaces to a script's caller as an abort.

**Cross-node.** The decision record lives on the anchor partition, which may be led by another node. When
the decision is not resolvable locally, the read routes a lookup to the anchor leader
(`LookupDurableRecordRouted` via `TryRouteForeignDecision`) and re-issues with the terminal decision, rather
than spinning until settlement propagates.

A committed delete intent is an absent base for conditional writes such as `SET … NX`, even before
settlement. Its tombstone has a new revision. Equal revision alone is still insufficient to prove
materialization: an expiry extension retains the revision, so expiry and state must also agree.

---

## 9. Recovery

`DurableTransactionRecovery` makes the protocol survive a lost coordinator and a lost settlement. It runs on
each partition leader, in two passes.

The **deadline sweep** takes the intents whose recovery deadline has passed:

- **Committed record** → resolve committed and materialize.
- **Abort record** → resolve aborted, no materialization.
- **Undecided past deadline**, or an **orphan prepare with no record within the outcome-retention horizon**
  → drive a presumed abort (the abort tombstone-from-absence transition), then resolve to the canonical
  winner. An absent record past the retention horizon instead follows the receipt/hold rules in §6.7.
- **Undecided within deadline** → skip; the live coordinator may still decide.

Recovery always takes the winner the record became — a concurrent commit is honoured even while
recovery is trying to abort.

The **decided pass** takes pending intents before their deadline and settles those whose record is already
terminal. It decides nothing: an intent whose record is undecided, absent or not readable stays with its
coordinator, or with the deadline sweep. It runs in two cases:

- **At a leadership gain**, over the intents of the gained partition that committed before the gain. A leader
  change is what can lose the settlement of a decided transaction, and the new leader does not wait for the
  decision deadline of the transaction to settle it. While an inherited intent is left (its decision is still
  on its way, or the leader of its anchor is not elected yet), the pass repeats after 100 ms and then after
  growing waits, until the periodic pass covers the intent.
- **In each maintenance tick** (`DurableMaintenanceInterval`), over the intents older than one tick (at least
  1 s). A younger intent is ordinary settlement lag, and its own settlement is on its way.

A pass handles at most 1,024 transactions of a partition, starts nothing after 5 s, and resumes where it stopped.
The intents it settles are counted under `source=decided_sweep` in `kahuna.durable_tx.settled_intents`.

---

## 10. Raft, the WAL, and storage

Once a proposal is issued, Kommander owns it:

1. **Propose** — the leader appends the entry batch to its WAL and replicates to followers.
2. **Quorum durable** — a quorum has the entry on disk. This is the true Raft commit point.
3. **Commit + apply** — the committed entries are applied through the replication callbacks
   (`KeyValueReplicator`, the durable stores' `Replicate`), which is how followers converge and how the
   leader's ordered apply runs.

Two WAL behaviours dominate commit latency:

- **Group commit.** A WAL worker can coalesce several partitions' writes into one storage flush —
  one `fsync` for many partitions. `MaxWalGroupBatchPartitions` bounds the batch;
  `WalGroupCommitLingerMs` optionally waits to gather a denser batch.
- **Single-fsync commit.** By default an auto-commit proposal costs *two* serial fsyncs (propose, then the
  committed marker). `WalSingleFsyncCommit` releases the client ticket once the **propose quorum is
  durable** and demotes the committed marker to a lazy write. Durability is unchanged,
  because propose-quorum-durable *is* the commit point; the setting moves one fsync off the caller's
  critical path.

`Kahuna.Server` exposes all three on the command line; embedded consumers set them on
`EmbeddedKahunaOptions` (`RaftMaxWalGroupBatchPartitions`, `RaftWalGroupCommitLingerMs`,
`RaftWalSingleFsyncCommit`). Check the host's effective configuration rather than assuming the server
and embedded defaults match:
`RaftWalSingleFsyncCommit` is **false** by default on `EmbeddedKahunaOptions`, whereas the server's
`--raft-wal-single-fsync-commit` defaults to **true**, on a standalone node and on a clustered one. The flag
takes a value: pass `--raft-wal-single-fsync-commit false` to turn the fast path off. New replication scheduling and retention controls
are described in the [snapshot and Raft recovery guide](snapshot-and-raft-recovery-guide.md#replication-scheduling-and-log-retention).

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

Every path maps onto three outcomes:

| Outcome | Meaning | Caller action |
|---|---|---|
| `Committed` / `Set` / `Get` … | Succeeded | Proceed |
| **`Aborted`** | A conflict, a lost lock or staging on an interactive session, or a terminal durable abort (including `PresumedAbort`) | Start a new transaction; this transaction cannot commit |
| **`MustRetry`** | This attempt did not establish a terminal outcome; or a script whose lock or staging was dropped by a leader change (its run was durably aborted, and a re-run is a new transaction) | Interactive finalize: retry with the same identity; a script: re-run the script |
| `Errored` / `InvalidInput` | Malformed input or an internal error | Fix the request |

The commit answer (`TransactionCommitAnswer` from `LocateAndCommitTransaction`, and the `Refusal` / `Reason`
fields of the gRPC and REST commit replies) also names **which arm refused** through `CommitRefusalClass`:
`Conflict`, `StagedBaseMoved`, `DecisionDeadline`, `PrepareExhausted`, `LostLock`, `LostStagedIntent`,
`StagedChainBreak`, `PresumedAbort`, `ExplicitRollback`, `InvalidWorkingSet` or `Faulted` on an `Aborted`;
`SettlementLag`, `Unresolved`, `AdmissionCapacity` or `RangeQuiesced` (a written key or the record anchor lies in
a key range whose data is being moved to another partition; retry after a short backoff) on a `MustRetry`;
`UnknownTransaction` on an `Errored`.
The answer deconstructs to the `(type, anchor)` pair it answered before the class existed, so a caller that
only reads the outcome is unchanged; the client SDK puts the class and reason in the exception message of a
refused commit.

Admission and unresolved infrastructure failures are retryable rather than fabricated conflicts.
`MustRetry` does **not** prove that no record or prepare was replicated: a frozen durable finalize may
have initialized its record and prepared some participants. Retry its finalize with the same identity;
do not infer rollback from a missing acknowledgement. An `Aborted` carries the
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
`…outstanding`, `…resident_records`, `…admission_rejections`, `…range_quiesce_refusals`, `…late_commit_rejections`,
`…late_commit_conclusions{outcome}`, `…retries_past_deadline{outcome}`, `…deadline_extensions{outcome}`,
`…prepare_retry_loops{outcome}`,
`…replica_fence_lagging_replicas`, `…replica_fence_lag_transitions{state,reason}`, `…replica_fence_lagging_asks{kind}`.
The time of a commit call and its stages are in §6.8.

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
| Commit call timing | `KeyValues/Transactions/CommitCallTelemetry.cs`, `CommitCallTimeline.cs`, `CommitCallStage.cs` |
| Working-set release | `KeyValues/Transactions/TransactionCoordinator.WorkingSetRelease.cs`, `Handlers/TryReleaseExclusiveLockHandler.cs`, `Handlers/InvalidateOrApplyHandler.cs` |
| Durable stores | `Transactions/TransactionRecordStore.cs`, `PreparedIntentStore.cs` (+ their state machines) |
| Deferred-window visibility | `Handlers/DurableReadVisibility.cs`, `PreparedIntentVisibility.cs`, `ForeignIntentWriteResolver.cs` |
| Recovery | `Transactions/DurableTransactionRecovery.cs` |
| Aggregator | `KeyValues/Writes/` (`PartitionWriteAggregator`, `DurableProposalSubmission`, `IPartitionBatchExecutor`) |
| Replication | `Replication/` (`ReplicationTypes`, `ReplicationSerializer`, restorers) |
| Persistence | `Persistence/BackgroundWriterActor.cs`, `Persistence/Backend/` |
| Raft / WAL / HLC | Kommander (`/Users/andresgutierrez/kommander`, source-only reference) |

---

## 15. Mental model

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

For latest-read pins, historical-read fences and transaction lock behavior, see
[transaction reads and locks](transaction-read-and-lock-semantics-guide.md).
