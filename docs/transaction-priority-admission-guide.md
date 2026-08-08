# Kahuna transaction priority admission guide

This guide explains how Kahuna decides *which* transaction starts next when a node is saturated, how to
tag work with a priority, and the invariants a maintainer must preserve. It is written for two
audiences:

- **Developers using Kahuna** who want a latency-critical transaction to take precedence over bulk
  background work, and operators who need to size and observe the gate.
- **Developers maintaining Kahuna** who need the admission lifecycle, the capacity model, and the
  correctness invariants in one place.

No knowledge of the internals is required to start; the tuning and operations sections stand alone.

**The feature ships dark.** Both ceilings default to `0`, which means every transaction starts
immediately and priority is recorded but never gates. A default-configured node behaves exactly as it did
before this existed. Turning it on is a deliberate operator decision — start with §6.

---

## 1. The big picture

Without admission control, a transaction begins executing the moment it reaches `KeyValuesManager`.
Ordering between concurrent transactions is whatever falls out of HLC timestamps and lock-acquisition
races. That is fine on an idle node and bad on a busy one: a user-facing transaction competes on equal
footing with a compaction sweep, and the only lever a client has is to throttle its own submission rate.

Kahuna adds a **per-node priority admission gate** in front of transaction start. Each transaction
carries a `TransactionPriority`; when the node is at its configured concurrency ceiling, further
transactions wait, and the gate starts the **highest-priority eligible** one first.

Three properties are worth stating up front, because they bound what this feature is:

- **It governs *start*, not execution.** Once admitted, a transaction runs exactly as it would without
  the gate. Nothing here preempts or aborts running work.
- **It is per node.** Each node orders only the work it receives. There is no cross-node fairness.
- **It changes nothing about correctness.** 2PC, MVCC, locking, and commit semantics are untouched.
  Priority influences *when* a transaction starts and nothing else.

Below the ceiling the gate is transparent — admission completes synchronously, with no queueing and no
reordering. That is the overwhelmingly common case and it is why an unsaturated node pays nothing.

---

## 2. The priority scale

```
Background = 0   Bulk, deferrable work (compaction, analytics sweeps). Yields to everything.
Low        = 1   Below ordinary traffic but still latency-relevant.
Normal     = 2   Default for everything that does not say otherwise.
High       = 3   Latency-critical work that should start ahead of ordinary traffic.
Critical   = 4   Must not be deferred. Never aged, because it is already at the top.
```

A small fixed scale rather than an arbitrary integer: it bounds the number of queues, keeps aging cheap
to reason about, and can be widened later without a wire break.

**Do not tag ordinary application traffic `Critical`.** If everything is critical the ordering carries no
information, and `Critical` stops meaning what it is reserved for — a node's own maintenance work and
operations whose delay would compromise availability.

`Normal` is the default at every layer, so a caller that never sets a priority behaves exactly as before.

---

## 3. Setting a priority

### Interactive sessions

```csharp
await using KahunaTransactionSession session = await client.StartTransactionSession(new KahunaTransactionOptions
{
    Priority = TransactionPriority.High,
    Timeout  = 30_000
});
```

### Script transactions, from the client

Priority lives on dedicated overloads rather than as an extra optional parameter, so existing calls that
pass a `CancellationToken` positionally keep compiling:

```csharp
await client.ExecuteKeyValueTransactionScript(script, hash, parameters, TransactionPriority.Background);

// Compiled scripts have a matching overload:
await compiled.Run(TransactionPriority.High, parameters);
```

### Inline, in the script itself

An inline option **overrides** whatever the transport carried, so a script can state its own importance:

```sql
BEGIN (priority="high", locking="optimistic")
  LET row = GET `orders/42`
  SET `orders/42` row
  COMMIT
END
```

Accepted values are `background`, `low`, `normal`, `high`, `critical`. Anything else is rejected as a
malformed script, the same way an unrecognized `locking` value is.

A script may also state how long it is willing to *wait to start*, separately from how long it may run:

```sql
BEGIN (priority="high", timeout=5000, admissionWait=2000)
  ...
END
```

`timeout` is the execution deadline; `admissionWait` is the door-wait. The two are independent clocks —
see §6. A non-numeric `admissionWait` is rejected as a malformed script.

### What priority does *not* apply to

The gate governs **transactions**. A script that opens one — an explicit `BEGIN` block or a
multi-statement script — is admission-gated. A script consisting of a single standalone command (`SET`,
`GET`, `DELETE`, `EXTEND`, bucket/prefix reads, and their ephemeral forms) holds no transaction, runs
directly against the store, and is deliberately **not** gated.

Putting single-key reads and writes behind a concurrency ceiling would throttle ordinary traffic, which
is a much larger decision than this feature makes. Priority on those shapes is accepted and ignored
rather than rejected, so a caller can set it once for a mixed workload without special-casing.

---

## 4. The capacity model

This is the part worth understanding before tuning, because the interaction between the ceiling and the
reserve is the only genuinely subtle piece.

Two independent ceilings exist, because the two transaction shapes hold a slot for very different
durations:

| Gate | Ceiling | Slot held for |
|---|---|---|
| **script** | `MaxConcurrentTransactions` | the transaction's own bounded execution |
| **session** | `MaxConcurrentSessions` | as long as the client keeps the session open |

They are **separate pools on purpose**. A session is client-paced and may sit idle for minutes; sharing
one pool would let idle sessions occupy every slot and stall script transactions node-wide.

How to size `MaxConcurrentSessions` depends entirely on how the caller uses sessions, and the two shapes
pull in opposite directions:

- **One session per client session** — the shape this design assumes. The ceiling bounds *connections*,
  not work, so size it generously.
- **One session per transaction** — what a storage engine layered on Kahuna typically does. Here the
  ceiling bounds **all work on the node**, including the caller's own internal writes such as catalog
  updates and schema checkpoints. Size it as a work ceiling, and treat a reserve as effectively
  mandatory once you set one: without it, a burst of bulk transactions can occupy every slot and starve
  the internal work the caller needs to make progress at all.

If you are not sure which shape you have, check whether the caller opens a session per user request or
per transaction. Advice written for the first shape actively misleads for the second.

### Reserved slots

`TransactionPriorityReservedSlots` are slots that only `High` and `Critical` may occupy. The accounting
is deliberately **two-dimensional**:

```
total occupancy          <= MaxConcurrent...                 (everyone)
ordinary occupancy       <= MaxConcurrent... - Reserved      (Background / Low / Normal)
```

"Ordinary occupancy" is tracked as its own counter, not derived from the total. That distinction is the
whole point, and getting it wrong is subtle enough to be worth an example.

With `max=2, reserved=1` and two `High` transactions running, suppose one finishes:

- **Correct (what Kahuna does):** ordinary occupancy is 0, which is below its bound of 1, and total is 1,
  below 2 — so a waiting `Background` transaction starts. The node runs at 2/2. The remaining `High` can
  be considered to hold the reserved slot; the one it freed is ordinary capacity and is usable.
- **Wrong (deciding from the total):** the waiter sees `total (1) < max - reserved (1)` = false and stays
  parked while a slot sits idle. Under a *sustained* stream of high-priority work that slot never frees,
  so the background transaction starves forever — defeating the anti-starvation guarantee entirely.

This was a real bug caught in review. If you touch `HasCapacityFor`, keep the two counters separate.

The reserve invariant survives regardless: because ordinary work can never exceed
`MaxConcurrent - Reserved`, at least `Reserved` slots are always reachable by latency-critical work, no
matter how much bulk work is offered.

---

## 5. Anti-starvation aging

A strict priority queue starves the bottom of the scale. Kahuna bounds that with **aging**: a waiter's
*effective* priority rises by one level for every `TransactionPriorityAgingThreshold` it spends waiting,
compounding, capped just below `Critical`.

So a `Background` transaction queued behind an endless stream of `High` work climbs to `High` after
enough time and then wins on arrival order, because ties go to the earlier arrival. That is the bound:
worst-case wait is roughly `(High - Background) × AgingThreshold`, not infinity.

Two properties of aging that are easy to get wrong:

- **Aging moves a waiter's place in line, never its class.** Dispatch ordering uses the *effective*
  priority; capacity eligibility uses the *base* priority. An aged `Background` transaction can overtake
  ordinary work but can **never** consume a reserved slot. Without this split, the reserve would evaporate
  under load, which is precisely when it matters.
- **`Critical` is never aged.** It already sorts above everything, and letting it climb further would
  only let it overtake other `Critical` work out of arrival order.

### Why aging uses a monotonic clock, not the HLC

Elapsed waiting is measured with an injected monotonic `TimeProvider`. This is a deliberate exception to
the repo-wide rule that cross-node ordering uses the HLC and never a wall clock.

`HybridLogicalClock.ReceiveEvent` sets `L = max(local, remote, physical)`, and Kahuna feeds peer
transaction ids into the shared clock from several hot handlers. A single peer with a skewed-forward
clock could therefore drag the local HLC forward by seconds, instantly aging every queued waiter to the
front at once and collapsing the priority separation the gate exists to provide.

The rule's purpose is to stop wall-clock time from deciding *distributed* happened-before questions. How
long a caller has sat in one node's local queue is not such a question — it is a local duration, and a
monotonic source is the correct tool. Arrival *order* uses a monotonic sequence number, which is a strict
total order and so needs no timestamp tiebreak at all.

---

## 6. Configuration and tuning

Every knob is available on `KahunaConfiguration`, `KahunaCommandLineOptions`, and `EmbeddedKahunaOptions`.

| Setting | CLI flag | Default | Meaning |
|---|---|---|---|
| `MaxConcurrentTransactions` | `--max-concurrent-transactions` | `0` | Script transactions running at once. `0` = no gate. |
| `MaxConcurrentSessions` | `--max-concurrent-sessions` | `0` | Interactive sessions open at once. `0` = no gate. |
| `TransactionPriorityReservedSlots` | `--transaction-priority-reserved-slots` | `0` | Slots only `High`/`Critical` may occupy. |
| `TransactionPriorityAgingThreshold` | `--transaction-priority-aging-threshold` | `1000` | Milliseconds of waiting per effective priority level. `0` = no aging. |
| `TransactionPriorityMaxQueued` | `--transaction-priority-max-queued` | `4096` | Callers that may wait per gate before further ones are refused. `0` = unbounded. |
| `DefaultAdmissionWaitMs` | `--default-admission-wait` | `5000` | How long a caller queues for a slot when it does not request its own budget. |
| `MaxAdmissionWaitMs` | `--max-admission-wait` | `30000` | Hard ceiling on any admission wait. Caller-supplied budgets are clamped to it. |

A caller may also ask for its own budget per request: `AdmissionWaitMs` on the session options (and on
the REST/gRPC Begin request), or `admissionWait` inline in a script. A value `<= 0` means "use
`DefaultAdmissionWaitMs`"; anything larger than `MaxAdmissionWaitMs` is clamped down.

### Turning it on

1. **Start in pass-through** (the default) and watch `kahuna.tx_admission.admitted` by priority. This
   tells you the shape of your workload before you constrain it — how much of your traffic is genuinely
   background, and whether clients are tagging anything at all.
2. **Set a ceiling near observed healthy concurrency**, not below it. The gate is for shedding and
   ordering *surplus* load; a ceiling under normal concurrency converts a healthy node into a queueing
   one and adds latency for no benefit.
3. **Add a reserve only if you see high-priority work queueing** behind bulk work
   (`kahuna.tx_admission.queued` non-zero at `High`/`Critical`). A reserve of 1–2 is usually enough; it
   is subtracted from what ordinary traffic may use, so a large reserve throttles your common case.
4. **Leave aging on** unless you have a specific reason. Lower values favour fairness, higher values
   favour honouring stated priorities. `0` disables it and allows indefinite starvation of low priorities.

`MaxConcurrentSessions` should be considerably larger than `MaxConcurrentTransactions`: it bounds how
many clients may have a transaction *open*, which is a connection-scale number, not a work-scale one.

### Overload: the queue is bounded too

A ceiling alone bounds only how much work *runs*. The queue behind it would still grow with offered
load — each waiter retaining a continuation and a cancellation registration — consuming the very memory
the ceiling exists to protect, precisely during the overload it is meant to survive.

`TransactionPriorityMaxQueued` bounds the wait queue **depth**. Past it, admission is refused immediately
and the caller receives **`AdmissionRefused`**. Nothing was started, so a retry is always safe.

`MaxAdmissionWaitMs` bounds the wait queue **duration**, which depth alone does not: without it a single
patient caller can occupy a queue slot for as long as it likes while others are turned away.

#### The two retryable codes, and why they are not the same

Both are safe to retry. They ask for *opposite* client behaviour, which is the whole reason they are
distinct:

| Code | What happened | What the client should do |
|---|---|---|
| `AdmissionRefused` | The queue was full, or this caller's admission budget expired. The node is deliberately shedding load. | **Back off**, then retry. Retrying immediately re-queues against the very saturation the gate exists to relieve. |
| `MustRetry` | A transient condition — a partition leader still being elected, a replica catching up. | **Retry promptly.** It clears on its own, usually in milliseconds. |

A client that cannot tell these apart must pick one policy and be wrong half the time: a tight retry loop
makes an overloaded node worse, and a long back-off makes ordinary start-up look like an outage.

Both admission gates — sessions and script transactions — use `AdmissionRefused`, so a client never has
to know which gate it hit in order to know how to react.

> **Note for callers upgrading.** Both refusals previously arrived as other codes: the session gate
> returned `MustRetry` for both cases, and the script gate returned `MustRetry` for a full queue but
> `Aborted` for a timed-out wait — the last of these wrongly implying the transaction had started and
> failed. Any client branching on those codes for admission should move to `AdmissionRefused`.

---

## 7. Observability

All instruments are observable gauges on the `Kahuna` meter, tagged by `gate` (`script` / `session`) and
`priority`.

| Instrument | What it tells you |
|---|---|
| `kahuna.tx_admission.in_flight` | Transactions currently holding a slot. |
| `kahuna.tx_admission.queued` | Transactions currently waiting. **Non-zero means the gate is actually gating.** |
| `kahuna.tx_admission.max_queue_depth` | High-water mark of simultaneous waiters. |
| `kahuna.tx_admission.admitted` | Transactions admitted since start. |
| `kahuna.tx_admission.aged_promotions` | Waiters that aging promoted at least once. |
| `kahuna.tx_admission.abandoned_while_waiting` | Waiters whose admission budget expired before they ever started. |
| `kahuna.tx_admission.rejected_queue_full` | Requests refused outright because the queue was full. |

`abandoned_while_waiting` and `rejected_queue_full` remain **distinct** even though both now return the
same `AdmissionRefused` response code. They answer different questions: the counters say *why* the node
refused, the response code says *what the client should do about it*. The split is what tells you whether
to raise the ceiling (callers are giving up at the door) or to raise the queue bound / shed upstream (the
queue is overflowing).

Reading them:

- **`queued` is the headline.** Zero means the gate is transparent and your ceiling is not binding.
  Sustained non-zero means transactions are being deferred and your priorities are now deciding who waits.
- **`queued` at `High`/`Critical`** is the signal that the ceiling is too low for the offered load, or
  that a reserve is warranted. Bulk work queueing is the gate working as designed; latency-critical work
  queueing is not.
- **`abandoned_while_waiting` vs `rejected_queue_full`** are deliberately separate. The first is callers
  giving up (timeouts too short, or the queue too deep). The second is the node refusing work outright —
  a much stronger signal, and one worth alerting on.
- **`aged_promotions` rising** means low-priority work is only getting through because of aging. That is
  the anti-starvation mechanism doing its job, but sustained high values mean the node is over-subscribed.

---

## 8. Lifecycle, failure, and cluster behavior

**The gate is pure in-memory, per-node state** — like the lock table and write intents. It is not
replicated, is not part of Raft state, and needs no persistence. On restart the queue is empty and
in-flight counts reset. That is correct: queued-but-not-started transactions were never durable, and
clients retry.

**Slot release is the critical invariant.** A leaked slot permanently shrinks node capacity for the
process lifetime, so release is guaranteed on every path:

- *Script transactions* release in a `finally`, first and unconditionally, before anything that could
  throw. Lock computation sits inside the same `try` so even a malformed script returns its slot.
- *Interactive sessions* carry the slot on the session context. Every path that retires a session —
  commit, rollback, **and the reaper reclaiming an abandoned one** — routes through a single
  `FinalizeSession` helper. That last one matters: a crashed client that never commits must not cost the
  node a slot forever.
- *Admission that never becomes a session* (a clock fault or teardown between taking the slot and
  publishing the session) disposes the slot rather than orphaning it.

Lease release is idempotent, so overlapping completion paths cannot double-count and inflate the ceiling.

**The admission wait is bounded independently of the transaction's own timeout.** These are two clocks
that measure unrelated things, and conflating them is a bug rather than a simplification:

| Clock | Bounds | Enforced by |
|---|---|---|
| `Timeout` (session) | how long an admitted transaction may **live** | the reaper, which reclaims the session and its MVCC snapshots |
| `timeout` (script) | how long an admitted transaction may **run** | the execution deadline |
| `AdmissionWaitMs` / `admissionWait` | how long a caller waits **to start** | the admission gate |

A transaction that intends to run for a long time is not thereby willing to wait a long time to begin.
Reusing the session timeout as the door-wait meant a caller asking for a long-lived session was silently
also asking to park at the gate for that whole span, with no way to shorten one without shortening the
other — and shortening the reaper window would let live range locks be reclaimed out from under a healthy
transaction.

For script transactions the split has a second consequence: the execution deadline now starts **when the
transaction is admitted**, not when it was submitted, so time spent queued is no longer deducted from the
time the script has to do its work. This matches where the transaction's HLC identity is minted. A
script's `timeout` therefore means "wall time from start" rather than "wall time from submission".

> **Behaviour change on upgrade.** A caller that sets no admission budget now gets
> `DefaultAdmissionWaitMs` (5 s), which is *shorter* than the previous effective wait — the session
> timeout clamped to `MaxTransactionTimeout`, i.e. up to 5 minutes at stock settings. That is the point
> of the change, but anyone who was relying on a long queue wait should set `AdmissionWaitMs` explicitly.

**Leader changes.** Admission sits *after* leader routing: `KeyValueLocator` forwards to the
coordinator-partition leader before the gate runs, so followers never queue work they will not execute. A
transaction that loses leadership mid-flight follows the existing `MustRetry` semantics and its slot is
released on the failure path like any other.

**Node teardown** fails every parked waiter with cancellation, rather than leaving callers awaiting a slot
a dead node will never grant.

**Untrusted priority values.** An out-of-range ordinal can reach the server as a raw number in a REST
payload or as a cast enum on the embedded API, neither of which passes through the gRPC conversion.
Unknown values are **normalized to `Normal`** — never clamped, which would read a large value as
`Critical` and let untrusted input jump the queue and claim reserved capacity. Priority is normalized
rather than rejected because it only affects ordering; `Locking`, `ReadValidation`, and
`DecisionDurability` change what a transaction *means* and are still rejected when out of range.

On the wire, `GrpcTransactionPriority` is offset by one so its zero value is `UNSPECIFIED`. A peer built
before the field existed necessarily sends zero, and that must resolve to `Normal` rather than to
`Background` — otherwise upgrading the server would silently demote every old client's transactions.

---

## 9. Maintaining and extending

- The gate is `TransactionPriorityOrderer`, one instance per gate, owned by `KeyValuesManager`. All state
  is under a single lock. The lock is **never** held across an `await`, and waiter tasks are always
  completed *outside* it so a continuation cannot run inside the critical section.
- **Keep `inFlight` and `ordinaryInFlight` separate.** Collapsing them reintroduces the starvation bug in
  §4. `FreedCapacity_IsUsableByOrdinaryWorkEvenWhileHighPriorityHoldsTheReserve` guards the fix;
  `OrdinaryWork_NeverOccupiesMoreThanTheUnreservedCapacity` and
  `AgedWaiter_StillCannotConsumeTheReservedSlot` guard the invariant that must survive it.
- **Capacity eligibility reads base priority; dispatch ordering reads effective priority.** If you make
  aging influence eligibility, the reserve stops being a reserve.
- **Cancellation, dispatch, and disposal all settle a waiter, and exactly one may win.** `TrySettle` is
  the arbiter and is called under the lock. The cancellation registration is necessarily created *outside*
  the lock (an already-cancelled token would re-enter it) and then published under the lock only if the
  waiter has not already settled — otherwise it is disposed immediately. Getting this wrong leaks
  registrations that keep the orderer reachable.
- **Do not call `sessions.TryRemove` directly.** Any new session-removal path must go through
  `FinalizeSession` or it will leak an admission slot silently.
- Waiters live in a per-priority `LinkedList` with the node stored on the waiter, so a cancelled waiter
  is unlinked in constant time rather than waiting for a dispatch to reach it. That matters when the
  occupant is long-lived and no dispatch runs for minutes.
- Aging is evaluated lazily at dispatch rather than on a timer tick. This is the same compounding policy
  but exact rather than tick-quantized, needs no extra actor, and cannot lag a dispatch — promotion only
  matters when a slot frees, which is exactly what triggers dispatch.
- If you add a new transaction entry point, admit **before** minting the transaction id. A transaction
  that queued must carry the HLC of when it actually started; otherwise its reads anchor to a snapshot
  taken before it ran.
- **Arm the execution/lifetime deadline after admission, and give the wait its own token.** A single
  `CancellationTokenSource` spanning both phases silently charges the queue wait to the transaction's own
  budget. Build a short `using`-scoped source from the admission budget for the `AdmitAsync` call alone,
  and start the transaction's deadline once the slot is held. Note that `CancelAfter` *reschedules* an
  existing timer, so a second call after admission masks the bug — which also means a test can pass
  against both the broken and fixed shapes if the "revert" it is checked against keeps that second call.
  Verify against a single pre-admission `CancelAfter` and none after.
- **A refusal is not an abort.** Nothing is minted, locked, or written before a slot is held, so an
  admission failure must return `AdmissionRefused` — never `Aborted` (which claims the transaction ran
  and conflicted) and never `MustRetry` (which invites the tight retry the gate is shedding).
- Tests: `TestTransactionPriorityOrderer` covers the gate in isolation with a manually advanced
  `TimeProvider` (so aging assertions are exact rather than racing a real clock);
  `TestTransactionPriorityAdmission` drives the real script and interactive entry points against an
  embedded node.
