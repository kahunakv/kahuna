
using System.Collections.Concurrent;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The partition-scoped authority for durable prepared intents. Holds at most one live intent per logical key and
/// routes every prepare/resolve/remove transition through the deterministic
/// <see cref="PreparedIntentStateMachine"/>, so leader apply, follower apply, WAL replay, and state-transfer
/// replay converge. It is the source of truth for a key's pending mutation after its prepare commits — the
/// actor-local write intent is only a cache.
///
/// <para>Mutation happens on the single per-partition apply path (Raft apply / restore); reads (visibility
/// lookups and the recovery sweep) may run concurrently, which the concurrent map makes safe. This type owns no
/// replication or persistence; that plumbing serializes/dispatches these same transitions.</para>
/// </summary>
internal sealed class PreparedIntentStore
{
    private readonly ConcurrentDictionary<string, PreparedIntent> intents = new();

    /// <summary>One key's last committed durable-transaction write, as remembered by the staged-base fence:
    /// the revision/state the committed head reached when that commit's settlement applied, and the commit
    /// timestamp used for retention pruning.</summary>
    private readonly record struct CommittedHead(long Revision, KeyValueState State, HLCTimestamp CommittedAt);

    // The staged-base fence's committed-head memory: for each key, the last durable-transaction commit whose
    // settlement (or removal) this store applied, within the retention horizon. Fed by commit resolutions on
    // the same per-key ordered apply path as prepares, so at the moment a prepare applies, any competitor
    // commit of the same key that settled earlier in the log has already left its head here.
    //
    // ADVISORY, per-node, in-memory state: it shapes only the prepare ACKNOWLEDGEMENT returned to the local
    // producer (see <see cref="ApplyDeltaAckPrepares"/>), never the replicated state-machine transition — so
    // it needs no cross-replica determinism, no persistence, and no snapshot coupling. Bounded by
    // size-triggered retention pruning; the staleness gate in the fence makes pruning safe.
    private readonly ConcurrentDictionary<string, CommittedHead> committedHeads = new();

    // Highest commit timestamp among recorded heads — the fence's notion of "now" for pruning and the
    // staleness gate, always an HLC from replicated commits, never a local clock. Guarded by applyGate.
    private HLCTimestamp committedHeadWatermark = HLCTimestamp.Zero;

    /// <summary>Default staged-base fence retention (ms). Must comfortably exceed the longest possible
    /// transaction lifetime (begin → prepare), because the staleness gate refuses to acknowledge a
    /// validated-base prepare from a transaction older than this horizon — its base may predate every
    /// retained head. Ten minutes covers the decision-deadline ceiling and typical reaper horizons.</summary>
    internal const int DefaultStagedBaseFenceRetentionMs = 600_000;

    // Committed-head entries are pruned only when the map crosses this size, so quiet stores never scan.
    private const int CommittedHeadPruneTriggerCount = 32_768;

    private int stagedBaseFenceRetentionMs = DefaultStagedBaseFenceRetentionMs;

    // Monotonic tick source for the dirty stamps below: each mutation mints one tick, so stamps taken
    // from it order mutations against the pre-scan capture in <see cref="PersistSnapshot"/>.
    private long version;

    // Per-partition dirty stamp: the tick of the last mutation whose key routed to that partition. A
    // partition's checkpoint snapshot is skipped when neither its stamp, nor <see cref="allPartitionsVersion"/>,
    // nor the routing stamp moved since its last durable write — the file already holds exactly this content —
    // which turns the common quiet checkpoint from a full scan + serialize + rewrite into a counter comparison.
    // With the previous single global stamp, one intent change anywhere re-dirtied every partition, so a busy
    // checkpoint rescanned the whole set once per partition.
    private readonly ConcurrentDictionary<int, long> partitionVersion = new();

    // Tick of the last mutation that could not be attributed to one partition: no resolver attached yet
    // (load-time merges), or a bulk sweep that touched many keys. Dirties every partition; over-dirty is safe.
    private long allPartitionsVersion;

    // Stamps captured just before that partition's last successful snapshot write: the mutation tick it
    // covered and the routing stamp it routed with. Captured before the scan, so a mutation or a routing
    // change racing the scan leaves a stamp ahead and the next checkpoint rewrites the file. The routing
    // stamp is what keeps a skip from hiding an intent that silently moved partitions when the range map
    // changed: until every partition has re-persisted under the new routing, none of them compares equal.
    private readonly ConcurrentDictionary<int, PersistedStamp> persistedVersion = new();

    private readonly string? snapshotDirectory;

    private readonly string? snapshotPrefix;

    private readonly ILogger<IKahuna>? logger;

    private readonly object fileLock = new();

    // Serializes the read-decide-write of a single transition so the compare-and-set is atomic. The concurrent map
    // makes each dictionary operation safe, but not the read-then-write pair: Raft-ordered apply, follower apply,
    // the recovery sweep, and producer-side apply can all call Apply concurrently, and two interleaved transitions
    // on the same key would otherwise both observe the pre-state and clobber each other.
    private readonly object applyGate = new();

    // Resolves an intent's key to its current data partition, so a per-partition snapshot/transfer only covers
    // the intents this partition owns. Null in the pure/in-memory configuration used by unit tests.
    private Func<string, int>? resolvePartition;

    /// <summary>In-memory configuration (unit tests): no persistence.</summary>
    public PreparedIntentStore() { }

    /// <summary>Durable configuration: a per-partition on-disk snapshot under <paramref name="storagePath"/> is
    /// loaded on construction and rewritten by <see cref="PersistSnapshot"/> before the WAL checkpoint discards
    /// the log tail; a parse failure fails closed.</summary>
    public PreparedIntentStore(string? storagePath, string? storageRevision, ILogger<IKahuna>? logger)
    {
        this.logger = logger;

        if (!string.IsNullOrEmpty(storagePath))
        {
            snapshotDirectory = storagePath;
            snapshotPrefix = $"preparedintent_{storageRevision}";
            LoadFromDisk();
        }
    }

    /// <summary>Wires the key → data-partition resolver once the locator exists (manager construction).
    /// <paramref name="routingVersion"/> reports a stamp that changes whenever the resolver's routing may have
    /// changed (the range-map version); null means the routing is fixed for the store's lifetime.</summary>
    public void AttachPartitionResolver(Func<string, int> resolver, Func<long>? routingVersion = null)
    {
        resolvePartition = resolver;
        this.routingVersion = routingVersion;
    }

    // Monotonic stamp of the routing the resolver reads (RangeMapStore.MapVersion), or null when routing is
    // fixed for the store's lifetime (tests, memory-only configuration). Pulled by the checkpoint guard.
    private Func<long>? routingVersion;

    // Marks the partition owning <paramref name="key"/> dirty for the checkpoint guard. Mutators call this
    // after the dictionary write, so a stamp equal to a pre-scan capture implies the scan saw the mutation.
    // Without a resolver the mutation cannot be attributed, so every partition is marked instead.
    //
    // This runs on the replicated apply/restore path, so it must never throw: routing can legitimately fail
    // there (a restart replays data-partition entries before the meta partition has rebuilt the range map,
    // and the resolver throws on an uncovered key-range key). An unattributable mutation falls back to the
    // all-partitions stamp — over-dirty is safe, a failed apply is not.
    private void StampDirty(string key)
    {
        long tick = Interlocked.Increment(ref version);

        Func<string, int>? resolver = resolvePartition;
        if (resolver is not null)
        {
            try
            {
                partitionVersion.AddOrUpdate(resolver(key), static (_, t) => t, static (_, prev, t) => Math.Max(prev, t), tick);
                return;
            }
            catch
            {
                // Fall through to the all-partitions stamp.
            }
        }

        StampMax(ref allPartitionsVersion, tick);
    }

    // Marks every partition dirty: for bulk sweeps whose per-key attribution would cost more than the
    // over-inclusive rewrite it avoids.
    private void StampAllDirty() => StampMax(ref allPartitionsVersion, Interlocked.Increment(ref version));

    // Monotonic max-write: a stamp must never move backward, or two racing mutators could leave the stamp
    // equal to a checkpoint's pre-scan capture while the later mutation was missed by its scan.
    private static void StampMax(ref long location, long tick)
    {
        long observed = Interlocked.Read(ref location);
        while (tick > observed)
        {
            long prior = Interlocked.CompareExchange(ref location, tick, observed);
            if (prior == observed)
                return;
            observed = prior;
        }
    }

    // The pair of stamps a partition's snapshot file was written under: the mutation tick it covered and the
    // routing stamp it routed with. A checkpoint skips the rewrite only when both still match.
    private readonly record struct PersistedStamp(long Version, long RoutingVersion);

    // Invoked (outside the apply gate) with the committed intent whenever a commit settlement (resolve or
    // removal of a committed intent) applies on this node — the convergence hook: the same apply position
    // that advances the fence's committed head lets the wiring verify the mutation actually materialized
    // here and repair the visible entry when it did not. Must not re-enter this store.
    private Action<PreparedIntent>? onCommittedSettleApplied;

    /// <summary>Wires the commit-settlement convergence hook (manager construction). The callback runs on the
    /// apply path outside the gate, once per applied commit resolution/removal, and must not re-enter the
    /// store.</summary>
    public void AttachCommittedSettleObserver(Action<PreparedIntent> observer) => onCommittedSettleApplied = observer;

    // Invoked (outside the apply gate) with the key and the frozen (validated base, committed head) revision
    // pair when a fence-refusal streak indicates this node's visible entry stopped converging with its
    // committed head — the wiring re-drives convergence from the node's own durable state, and the head
    // revision lets it say plainly when even that state is missing the head commit. Must not block and must
    // not re-enter this store.
    private Action<string, long, long>? onFenceWedgeRepair;

    /// <summary>Wires the fence-wedge repair hook (manager construction): (key, validated base revision,
    /// committed head revision). The callback runs on the apply path outside the gate, rate-limited by the
    /// refusal streak, and must not block or re-enter the store.</summary>
    public void AttachFenceWedgeRepairer(Action<string, long, long> repairer) => onFenceWedgeRepair = repairer;

    // Invoked (outside the apply gate) with the refused prepare's intent and the remembered committed-head
    // revision whenever THIS node's fence flags a replicated validated-base prepare as stale. The verdict is
    // deterministically correct at the prepare's apply position (heads record only real commits, in log
    // order), even when the acknowledging leader's frozen memory admitted the prepare — so the wiring drives
    // a best-effort abort at the transaction's anchor. Must not block and must not re-enter the store.
    private Action<PreparedIntent, long>? onStaleBaseVeto;

    /// <summary>Wires the replica stale-base veto hook (manager construction): (refused intent, committed head
    /// revision). Runs on the apply path outside the gate, once per stale-flagged prepare apply on this node,
    /// and must not block or re-enter the store. See <see cref="ApplyDeltaAckPrepares"/> for the trigger.</summary>
    public void AttachStaleBaseVetoer(Action<PreparedIntent, long> vetoer) => onStaleBaseVeto = vetoer;

    /// <summary>Sets the staged-base fence retention horizon. The caller must pass a value comfortably above
    /// the longest possible transaction lifetime: the staleness gate refuses to acknowledge a validated-base
    /// prepare from a transaction older than this horizon, so a horizon below real transaction lifetimes turns
    /// long transactions into spurious aborts. Idempotent; safe to call on a shared store.</summary>
    public void ConfigureStagedBaseFence(int retentionMs) =>
        stagedBaseFenceRetentionMs = Math.Max(1, retentionMs);

    // ── Fence-verdict wait ───────────────────────────────────────────────────────
    //
    // A pre-decision fence verdict request routinely arrives before the transaction's prepare has applied on
    // this node: the coordinator releases the verdict request and the apply broadcast from the same
    // post-durability callback, so the request wins that race deterministically on the synchronous-fsync
    // commit path. Waiters therefore park on this pulse and are woken by the mutation paths the moment the
    // intent set changes, instead of sleeping on a poll timer whose full tick every read-modify-write commit
    // would pay as a latency floor.

    // Completed and replaced whenever the intent set changes while a waiter is parked. Waiters capture the
    // current instance BEFORE evaluating; a mutation that lands after their evaluation completes the instance
    // they hold (every replaced instance is completed first), so a wakeup can never be lost. Created with
    // RunContinuationsAsynchronously so the mutation paths never run waiter continuations inline.
    private TaskCompletionSource intentSetChangedPulse = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Number of parked fence waiters. While zero — the common case on every apply that no verdict request is
    // waiting on — the mutation paths skip the pulse swap and its allocation entirely.
    private int fenceWaiterCount;

    /// <summary>Wakes parked fence-verdict waiters after a mutation of the intent set. Called outside
    /// <see cref="applyGate"/>; a single volatile read when no waiter is parked.</summary>
    private void SignalFenceWaiters()
    {
        if (Volatile.Read(ref fenceWaiterCount) == 0)
            return;

        TaskCompletionSource pulse = Interlocked.Exchange(
            ref intentSetChangedPulse, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        pulse.TrySetResult();
    }

    /// <summary>Applies one transition to the intent at the command's key and reflects the result in the map:
    /// install/update on <see cref="TransactionApplyOutcome.Applied"/> with a record, delete when the applied
    /// record is null (removal), and leave the map unchanged on a no-op or rejection.</summary>
    public PreparedIntentApplyResult Apply(PreparedIntentCommand command)
    {
        string key = KeyOf(command);

        PreparedIntent? settledCommit = null;
        bool wedgeRepairDue = false;
        long wedgeBaseRevision = 0;
        long wedgeHeadRevision = 0;
        PreparedIntentApplyResult result;

        lock (applyGate)
        {
            intents.TryGetValue(key, out PreparedIntent? existing);

            result = PreparedIntentStateMachine.Apply(existing, command);

            if (result.Outcome == TransactionApplyOutcome.Applied)
            {
                if (result.Intent is null)
                {
                    intents.TryRemove(key, out _);
                    if (existing is not null)
                        Interlocked.Add(ref totalBytes, -IntentBytes(existing));
                }
                else
                {
                    intents[key] = result.Intent;
                    Interlocked.Add(ref totalBytes, IntentBytes(result.Intent) - (existing is null ? 0 : IntentBytes(existing)));
                }

                StampDirty(key);
            }

            // A commit resolution (or the removal of a committed intent — the import/replay orderings where the
            // resolve applied elsewhere) makes the intent's mutation the key's committed head; remember it for
            // the staged-base fence. Idempotent replays record the same values. A resolve over an absent intent
            // (already settled and garbage-collected, decision replaying) has no mutation left to record.
            if (existing is not null)
            {
                bool committedNow = command is ResolveIntentCommand { Commit: true }
                    && result.Outcome is TransactionApplyOutcome.Applied or TransactionApplyOutcome.IdempotentNoop;
                bool removedCommitted = command is RemoveIntentCommand
                    && existing.Resolution == PreparedIntentResolution.Committed;

                if (committedNow || removedCommitted)
                {
                    RecordCommittedHead(existing);
                    settledCommit = existing;
                }
            }

            // ── Staged-base fence, evaluated at the prepare's own apply position ──
            // A freshly installed validated-base prepare is checked against the committed-head memory. The
            // pre-propose validation cannot see a competitor that commits the same base between its probe and
            // this prepare landing (the competitor's intent settled and was removed, so single-live-intent sees
            // nothing) — the interleaving that silently discards the competitor's write (a lost update). Here
            // that competitor is always visible: either its intent is still live (the state machine already
            // rejected this prepare), or its settlement applied earlier on this key and left its head above.
            //
            // The verdict does NOT change the replicated transition — the intent installs identically on every
            // node, so replicas and replay never diverge. It only flags the result; the local producer's
            // acknowledgement path (<see cref="ApplyDeltaAckPrepares"/>) folds the flag into a refused prepare,
            // which drives the coordinator's standard truthful conflict abort, and the abort's resolution rolls
            // the installed intent back everywhere.
            //
            // Judged on a fresh install AND on the idempotent same-identity re-prepare of a still-pending
            // intent: the finalizer's prepare-retry loop re-proposes the same delta and reads the no-op as "already
            // prepared", so a verdict given only on the first apply would be washed away by the first retry.
            // Heads only move forward, so once a base is stale every re-ask answers stale, and the retry budget
            // exhausts into the truthful abort. A RESOLVED existing intent is left alone — that is a decision
            // replay, owned by the record, with no acknowledgement left to protect. A foreign holder is the state
            // machine's rejection to report.
            bool freshValidatedInstall = existing is null && result.Outcome == TransactionApplyOutcome.Applied;
            bool pendingSameIdentityReprepare = existing is { IsPending: true } && result.Outcome == TransactionApplyOutcome.IdempotentNoop;

            if ((freshValidatedInstall || pendingSameIdentityReprepare)
                && command is PrepareIntentCommand { Intent.HasValidatedBase: true } fencedPrepare)
            {
                string? fenceConflict = EvaluateStagedBaseFence(fencedPrepare.Intent);
                if (fenceConflict is not null)
                {
                    DurableTransactionMetrics.StagedBasePrepareRejections.Add(1);
                    wedgeRepairDue = TrackFenceRefusal(fencedPrepare.Intent, out wedgeHeadRevision);
                    wedgeBaseRevision = fencedPrepare.Intent.BaseRevision;

                    logger?.LogWarning(
                        "Staged base for {Key} moved before the prepare of transaction {TransactionId} applied; refusing the prepare acknowledgement to prevent a lost update: {Reason}",
                        fencedPrepare.Intent.Key, fencedPrepare.Intent.TransactionId, fenceConflict);

                    result = result with { StaleBase = true };
                }
                else
                    fenceRefusalStreaks.TryRemove(key, out _);
            }
        }

        // Outside the gate: the hooks may rent messages, schedule tasks, and enqueue actor work, none of which
        // may run under the apply lock. The hooks must not re-enter this store.
        if (result.Outcome == TransactionApplyOutcome.Applied)
            SignalFenceWaiters();

        if (settledCommit is not null)
            onCommittedSettleApplied?.Invoke(settledCommit);

        if (wedgeRepairDue)
            onFenceWedgeRepair?.Invoke(key, wedgeBaseRevision, wedgeHeadRevision);

        return result;
    }

    // ── Fence-wedge watchdog ─────────────────────────────────────────────────────
    //
    // A healthy fence refusal is transient: the client re-reads, validates the moved base, and the next
    // prepare passes. A key refusing over and over with the SAME frozen (validated base, committed head) pair
    // means this node's visible entry stopped converging with its committed head — the key is wedged and
    // effectively read-only, which previously ran for tens of minutes with nothing above Warning spam. The
    // streaks are advisory per-node observability, mutated under the apply gate, and pruned when the key's
    // fence passes or its head advances.

    private readonly record struct FenceRefusalStreak(long BaseRevision, long HeadRevision, long Count);

    private readonly ConcurrentDictionary<string, FenceRefusalStreak> fenceRefusalStreaks = new();

    // A storm reaches ~2,000-3,000 refusals/min on a wedged hot key; 50 consecutive identical refusals is far
    // beyond any healthy retry burst (the finalizer retries a prepare at most 8 times) while alarming within
    // seconds of wedge onset. Re-escalate periodically so a long-lived wedge stays visible in the log.
    private const int FenceWedgeAlarmThreshold = 50;

    private const int FenceWedgeReAlarmEvery = 10_000;

    // The self-healing trigger, deliberately far below the alarm: healthy contention never repeats the exact
    // same (validated base, committed head) pair this many times consecutively — a retrying client re-reads
    // the moved base, changing the pair and resetting the streak — while a wedged key repeats it forever. A
    // reconcile that heals re-arms nothing further; one that could not (or raced) fires again periodically.
    // The alarm at 50 therefore only ever fires when the repair itself is failing.
    private const int FenceWedgeRepairThreshold = 5;

    private const int FenceWedgeRepairRepeatEvery = 200;

    // Bounds the tracker: refusal streaks exist only for keys currently refusing, but a pathological workload
    // could touch many; past the cap new keys are simply not tracked (the metric still counts refusals).
    private const int FenceRefusalTrackerMaxKeys = 4_096;

    /// <summary>Records one fence refusal for the watchdog: escalates to the error log when the same key
    /// refuses repeatedly at an unchanged (validated base, committed head) pair, and returns whether the
    /// convergence repair hook is due for this refusal (rate-limited by the streak).
    /// <paramref name="headRevision"/> reports the remembered committed head the refusal was judged against.
    /// Caller holds <see cref="applyGate"/>.</summary>
    private bool TrackFenceRefusal(PreparedIntent intent, out long headRevision)
    {
        committedHeads.TryGetValue(intent.Key, out CommittedHead head);
        headRevision = head.Revision;

        if (!fenceRefusalStreaks.TryGetValue(intent.Key, out FenceRefusalStreak streak)
            || streak.BaseRevision != intent.BaseRevision
            || streak.HeadRevision != head.Revision)
        {
            if (fenceRefusalStreaks.Count >= FenceRefusalTrackerMaxKeys && !fenceRefusalStreaks.ContainsKey(intent.Key))
                return false;

            fenceRefusalStreaks[intent.Key] = new(intent.BaseRevision, head.Revision, 1);
            return false;
        }

        long count = streak.Count + 1;
        fenceRefusalStreaks[intent.Key] = streak with { Count = count };

        if (count == FenceWedgeAlarmThreshold || (count > FenceWedgeAlarmThreshold && count % FenceWedgeReAlarmEvery == 0))
        {
            DurableTransactionMetrics.StagedBaseFenceWedgedKeys.Add(1);

            logger?.LogError(
                "Key {Key} refused {Count} consecutive validated-base prepares at a frozen pair (validated revision {BaseRevision}, committed head {HeadRevision}): the node's visible entry has stopped converging with its committed head and the key is effectively read-only until it reconciles",
                intent.Key, count, intent.BaseRevision, head.Revision);
        }

        return count == FenceWedgeRepairThreshold
            || (count > FenceWedgeRepairThreshold && count % FenceWedgeRepairRepeatEvery == 0);
    }

    /// <summary>
    /// Decides whether a freshly installed validated-base prepare deserves its acknowledgement, against the
    /// committed-head memory. Returns null to acknowledge, or the conflict reason. Caller holds
    /// <see cref="applyGate"/>.
    /// </summary>
    private string? EvaluateStagedBaseFence(PreparedIntent intent) =>
        JudgeStagedBase(intent, countAbsentHeadAdmission: true, out _);

    /// <summary>The fence decision itself, shared by the apply-path acknowledgement and the replica verdict
    /// read (<see cref="EvaluateReplicaFenceVerdicts"/>). Returns null to admit, or the conflict reason.
    /// <paramref name="headRevision"/> reports the remembered head the verdict was judged against (-1 when the
    /// memory held nothing). Only the apply path counts the absent-head admission; a verdict read must not
    /// inflate that counter. Caller holds <see cref="applyGate"/>.</summary>
    private string? JudgeStagedBase(PreparedIntent intent, bool countAbsentHeadAdmission, out long headRevision)
    {
        headRevision = -1;

        // Staleness gate, the partner of retention pruning: a pruned head cannot be distinguished from "no
        // commit happened", so a prepare from a transaction that BEGAN before the retention horizon (its reads,
        // and therefore its base, may predate every retained head) must not be acknowledged on absence of
        // evidence. The transaction id is the begin HLC, so it lower-bounds every read the base came from.
        // Measured against the head watermark — an HLC from replicated commits — never a local clock.
        if (committedHeadWatermark != HLCTimestamp.Zero
            && intent.TransactionId.L + stagedBaseFenceRetentionMs < committedHeadWatermark.L)
            return $"transaction began before the staged-base fence retention horizon; its validated base for key {intent.Key} cannot be verified";

        if (!committedHeads.TryGetValue(intent.Key, out CommittedHead head))
        {
            // The only silent path around the fence: nothing to check is indistinguishable from "no commit
            // ever happened" — count it so a loss investigation can tell proof-of-currency from absence.
            if (countAbsentHeadAdmission)
                DurableTransactionMetrics.FenceAdmissionsAbsentHead.Add(1);
            return null;
        }

        headRevision = head.Revision;

        // BaseState says whether the validated base was an existing value (the finalize-input builder maps an
        // observed non-existent base to Undefined). PreparedIntent.UnknownBaseRevision (no base at all) never
        // reaches this method. A transactional delete recorded as the head keeps the key absent, so it is a
        // valid base for a validated-absent insert.
        if (intent.BaseState != KeyValueState.Set)
        {
            return head.State == KeyValueState.Set
                ? $"committed base for key {intent.Key} changed after validation: validated absent, committed head is now revision {head.Revision}"
                : null;
        }

        // Only a head that moved PAST the validated base is a conflict. A head at the base is exactly the
        // commit the read observed; a head behind the base means non-transactional writes advanced the key
        // after that commit — this memory can attest nothing newer there, and refusing would wedge every later
        // read-modify-write of the key until the entry ages out.
        return head.Revision > intent.BaseRevision
            ? $"committed base for key {intent.Key} changed after validation: validated revision {intent.BaseRevision}, committed head is now revision {head.Revision}"
            : null;
    }

    /// <summary>
    /// Reads the fence's committed-head memory for <paramref name="key"/>: the revision and state of the last
    /// durable-transaction commit whose settlement this node applied within retention. Lock-free (the map is
    /// concurrent and heads are monotonic per key), so read paths may probe it per operation. False means the
    /// memory holds nothing for the key — which can mean "no commit ever happened" or "the memory was pruned
    /// or never fed"; callers must not treat absence as proof of no history.
    /// </summary>
    internal bool TryGetCommittedHead(string key, out long revision, out KeyValueState state)
    {
        if (committedHeads.TryGetValue(key, out CommittedHead head))
        {
            revision = head.Revision;
            state = head.State;
            return true;
        }

        revision = -1;
        state = KeyValueState.Undefined;
        return false;
    }

    /// <summary>Number of keys currently held in the committed-head memory. Observability only — logged in the
    /// leadership-change fingerprint so a promotion with an empty memory is visible in the node log.</summary>
    internal int CommittedHeadCount => committedHeads.Count;

    /// <summary>Number of live prepared intents currently held. Observability only — logged in the
    /// leadership-change fingerprint alongside <see cref="CommittedHeadCount"/>.</summary>
    internal int LiveIntentCount => intents.Count;

    /// <summary>
    /// Requests the convergence repair for a key whose locally visible durable row was observed strictly below
    /// the committed-head memory — the same wiring the fence-wedge watchdog drives (re-drive the parked head
    /// mutation, then reconcile from local revision history). A no-op when the memory does not actually exceed
    /// <paramref name="observedRevision"/> (a benign race with a concurrent settle) or when no hook is wired.
    /// Never blocks: the wired hook only schedules detached work. Returns whether a repair was requested.
    /// </summary>
    internal bool RequestConvergenceRepair(string key, long observedRevision)
    {
        if (!committedHeads.TryGetValue(key, out CommittedHead head) || head.Revision <= observedRevision)
            return false;

        onFenceWedgeRepair?.Invoke(key, observedRevision, head.Revision);
        return true;
    }

    /// <summary>
    /// Reads this node's staged-base fence verdict for each of one transaction's validated-base prepares —
    /// the replica half of the pre-decision fence confirmation. A key whose still-pending intent this store
    /// holds under the given identity is judged with the same fence the apply path runs; while that intent is
    /// live, the single-live-intent rule freezes the key's committed head, so the answer equals the verdict at
    /// the prepare's own apply position. A key without that intent answers
    /// <see cref="KeyValueStagedBaseVerdict.NotApplied"/> (this node cannot attest), and a resolved intent or
    /// a blind write answers <see cref="KeyValueStagedBaseVerdict.Clear"/> (nothing left for the fence to
    /// judge). Evaluated under the apply gate so the (intent, head, watermark) read is consistent.
    /// </summary>
    internal KeyValueStagedBaseVerdictEntry[] EvaluateReplicaFenceVerdicts(
        HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys)
    {
        KeyValueStagedBaseVerdictEntry[] verdicts = new KeyValueStagedBaseVerdictEntry[keys.Count];

        lock (applyGate)
        {
            for (int i = 0; i < keys.Count; i++)
            {
                if (!intents.TryGetValue(keys[i], out PreparedIntent? intent)
                    || intent.TransactionId != transactionId
                    || intent.Epoch != epoch)
                {
                    verdicts[i] = new(
                        KeyValueStagedBaseVerdict.NotApplied,
                        committedHeads.TryGetValue(keys[i], out CommittedHead head) ? head.Revision : -1);
                    continue;
                }

                if (!intent.IsPending || !intent.HasValidatedBase)
                {
                    verdicts[i] = new(KeyValueStagedBaseVerdict.Clear, -1);
                    continue;
                }

                string? conflict = JudgeStagedBase(intent, countAbsentHeadAdmission: false, out long headRevision);
                verdicts[i] = new(
                    conflict is null ? KeyValueStagedBaseVerdict.Clear : KeyValueStagedBaseVerdict.StaleBase,
                    headRevision);
            }
        }

        return verdicts;
    }

    /// <summary>
    /// The waiting form of <see cref="EvaluateReplicaFenceVerdicts"/>: while any key answers
    /// <see cref="KeyValueStagedBaseVerdict.NotApplied"/>, parks on the intent-set pulse and re-evaluates
    /// when the set changes, until every key is attested or <paramref name="waitMs"/> runs out. Event-driven:
    /// a prepare that applies moments after the request wakes the waiter immediately, so a healthy in-flight
    /// apply costs the wake latency, never a poll tick. Returns the latest evaluation; cancellation and an
    /// exhausted budget return it as it stands (<c>NotApplied</c> is never an objection to the caller).
    /// </summary>
    internal async Task<KeyValueStagedBaseVerdictEntry[]> EvaluateReplicaFenceVerdictsAsync(
        HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys, int waitMs, CancellationToken cancellationToken)
    {
        long deadline = Environment.TickCount64 + waitMs;

        KeyValueStagedBaseVerdictEntry[] verdicts = EvaluateReplicaFenceVerdicts(transactionId, epoch, keys);
        if (!AnyNotApplied(verdicts))
            return verdicts;

        Interlocked.Increment(ref fenceWaiterCount);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Capture the pulse BEFORE evaluating: a mutation that applies after this evaluation then
                // completes the captured instance, so satisfaction can never slip past a parked waiter.
                Task changed = Volatile.Read(ref intentSetChangedPulse).Task;

                verdicts = EvaluateReplicaFenceVerdicts(transactionId, epoch, keys);
                if (!AnyNotApplied(verdicts))
                    return verdicts;

                long remainingMs = deadline - Environment.TickCount64;
                if (remainingMs <= 0)
                    return verdicts;

                try
                {
                    await changed.WaitAsync(TimeSpan.FromMilliseconds(remainingMs), cancellationToken).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    // Budget exhausted; the loop runs one final fresh evaluation and returns it.
                }
                catch (OperationCanceledException)
                {
                    return verdicts;
                }
            }

            return verdicts;
        }
        finally
        {
            Interlocked.Decrement(ref fenceWaiterCount);
        }
    }

    private static bool AnyNotApplied(KeyValueStagedBaseVerdictEntry[] verdicts)
    {
        foreach (KeyValueStagedBaseVerdictEntry verdict in verdicts)
        {
            if (verdict.Verdict == KeyValueStagedBaseVerdict.NotApplied)
                return true;
        }

        return false;
    }

    /// <summary>Empties the committed-head memory, reproducing the state a process restart leaves: the memory
    /// is in-memory only, so every restart opens a window where the fence has no head to judge against. Test
    /// seam for driving that window deterministically; never called by production code.</summary>
    internal void ForgetCommittedHeadsForTesting()
    {
        lock (applyGate)
        {
            committedHeads.Clear();
            fenceRefusalStreaks.Clear();
            committedHeadWatermark = HLCTimestamp.Zero;
        }
    }

    /// <summary>Records a committed intent's mutation as its key's committed head and advances the pruning
    /// watermark. Monotonic per key: an older revision never overwrites a newer one. Deliberately does not
    /// touch the snapshot change stamp — the memory is advisory and never persisted. Caller holds
    /// <see cref="applyGate"/>.</summary>
    private void RecordCommittedHead(PreparedIntent intent)
    {
        if (committedHeads.TryGetValue(intent.Key, out CommittedHead current) && current.Revision >= intent.Revision)
            return;

        committedHeads[intent.Key] = new(intent.Revision, intent.State, intent.CommitTimestamp);

        // The head moved: whatever refusal streak the key held was measured against the old pair.
        fenceRefusalStreaks.TryRemove(intent.Key, out _);

        if (intent.CommitTimestamp > committedHeadWatermark)
            committedHeadWatermark = intent.CommitTimestamp;

        if (committedHeads.Count > CommittedHeadPruneTriggerCount)
            PruneCommittedHeads();
    }

    /// <summary>Drops committed-head entries older than the retention horizon. Size-triggered rather than
    /// timer-driven, so quiet stores never scan. Safe because the staleness gate refuses any prepare whose
    /// transaction is old enough to have depended on a pruned entry. Caller holds <see cref="applyGate"/>.</summary>
    private void PruneCommittedHeads()
    {
        long cutoff = committedHeadWatermark.L - stagedBaseFenceRetentionMs;

        List<string>? expired = null;
        foreach (KeyValuePair<string, CommittedHead> entry in committedHeads)
        {
            if (entry.Value.CommittedAt.L < cutoff)
                (expired ??= []).Add(entry.Key);
        }

        if (expired is null)
            return;

        foreach (string key in expired)
            committedHeads.TryRemove(key, out _);
    }

    /// <summary>The current intent at <paramref name="key"/>, or null. The emptiness pre-check is
    /// deliberate: this runs on every point read/write in the actor hot path, and on workloads with
    /// no durable transactions it skips hashing the key into the shared map entirely (an empty map
    /// can only ever answer null).</summary>
    public PreparedIntent? Get(string key) =>
        !intents.IsEmpty && intents.TryGetValue(key, out PreparedIntent? intent) ? intent : null;

    /// <summary>The intent at <paramref name="key"/> only when it belongs to the given transaction attempt.</summary>
    public PreparedIntent? GetByIdentity(HLCTimestamp transactionId, long epoch, string key) =>
        !intents.IsEmpty
        && intents.TryGetValue(key, out PreparedIntent? intent)
        && intent.TransactionId == transactionId
        && intent.Epoch == epoch
            ? intent
            : null;

    /// <summary>Every intent currently held on this partition — the input to the recovery sweep.</summary>
    public IReadOnlyCollection<PreparedIntent> Snapshot() => intents.Values.ToArray();

    /// <summary>Pending (undecided) intents whose recovery deadline is at or before <paramref name="now"/> —
    /// candidates for a recovery decision lookup. Deadline comparison is by HLC, never a local clock.</summary>
    public IReadOnlyList<PreparedIntent> DueForRecovery(HLCTimestamp now)
    {
        List<PreparedIntent> due = [];
        foreach (PreparedIntent intent in intents.Values)
        {
            if (intent.IsPending && intent.RecoveryDeadline != HLCTimestamp.Zero && intent.RecoveryDeadline <= now)
                due.Add(intent);
        }

        return due;
    }

    /// <summary>Due pending intents whose key routes to <paramref name="partitionId"/>. When no partition resolver
    /// is attached (the in-memory/test configuration), every due intent is returned.</summary>
    public IReadOnlyList<PreparedIntent> DueForRecovery(HLCTimestamp now, int partitionId)
    {
        Func<string, int>? resolver = resolvePartition;

        List<PreparedIntent> due = [];

        foreach (PreparedIntent intent in intents.Values)
        {
            if (!intent.IsPending || intent.RecoveryDeadline == HLCTimestamp.Zero || intent.RecoveryDeadline > now)
                continue;

            if (resolver is not null && resolver(intent.Key) != partitionId)
                continue;

            due.Add(intent);
        }

        return due;
    }

    public int Count => intents.Count;

    // Running sum of resident intent value bytes, maintained on every install/update/remove across Apply and the
    // load/import merge, so durable admission can bound resident prepared-intent memory without an O(n) scan.
    private long totalBytes;

    /// <summary>Resident prepared-intent value bytes across this node (durable admission bound / observability).</summary>
    public long TotalBytes => Interlocked.Read(ref totalBytes);

    private static long IntentBytes(PreparedIntent intent) => intent.Value?.Length ?? 0;

    private static string KeyOf(PreparedIntentCommand command) => command switch
    {
        PrepareIntentCommand prepare => prepare.Intent.Key,
        ResolveIntentCommand resolve => resolve.Key,
        RemoveIntentCommand remove => remove.Key,
        _ => throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown prepared-intent command")
    };

    // ── replication ─────────────────────────────────────────────────────────────

    public bool Restore(int partitionId, RaftLog log) => ApplyLog(log);

    public bool Replicate(int partitionId, RaftLog log) => ApplyLog(log);

    // The proposer's decoded commands, keyed by the exact byte array handed to Raft, budgeted for one
    // take per co-hosted node. See ProposedDeltaCache for the reuse and lifetime contract; reusing the
    // producer's instances is safe only because commands and their intents are immutable.
    private static readonly ProposedDeltaCache<PreparedIntentCommand> locallyProposedDeltas = new();

    /// <summary>Applies a delta and reports whether every PREPARE command in it took ownership of its key. Returns
    /// <see langword="false"/> when any prepare is rejected by the state machine — another transaction already
    /// holds the key, or the same identity re-prepared a divergent mutation — or when the staged-base fence
    /// flagged an applied prepare's validated base as moved (<see cref="PreparedIntentApplyResult.StaleBase"/>).
    /// Neither is an acknowledged prepare: the producer must abort rather than commit a mutation whose
    /// recoverable intent it never owned, or whose base a competitor already re-committed (a lost update).
    /// Resolve/remove deltas carry no prepares, so they always report true.
    ///
    /// <para>A stale-flagged prepare additionally fires the replica veto hook: the acknowledgement that folds
    /// this method's return value is the LEADER's alone, so on any other node the flag would otherwise be a
    /// verdict with no effect — and a leader whose own memory is frozen admits exactly the prepares the healthy
    /// replicas refuse (the fsync-gate fork producer). The hook runs after the applies, outside the gate. The
    /// restore path (<see cref="Restore"/>) deliberately bypasses this method, so replayed history never
    /// vetoes; ordered live catch-up cannot produce a false flag, because heads advance in the same log order
    /// the prepares apply in.</para></summary>
    public bool ApplyDeltaAckPrepares(RaftLog log)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        if (!locallyProposedDeltas.TryTake(log.LogData, out PreparedIntentCommand[]? commands))
            commands = DecodeDelta(log.LogData);

        bool allPreparesAccepted = true;
        List<PreparedIntent>? staleFlagged = null;

        foreach (PreparedIntentCommand command in commands)
        {
            PreparedIntentApplyResult result = Apply(command);
            if (command is PrepareIntentCommand prepare && (result.Outcome == TransactionApplyOutcome.Rejected || result.StaleBase))
            {
                allPreparesAccepted = false;
                if (result.StaleBase)
                    (staleFlagged ??= []).Add(prepare.Intent);
            }
        }

        FireStaleBaseVetoes(staleFlagged);
        return allPreparesAccepted;
    }

    /// <summary>Fires the veto hook for each stale-flagged prepare of one delta apply, with the head revision
    /// re-read at invocation (a benign race with a concurrent settle can only raise it). No gate is held here;
    /// the wiring schedules detached work.</summary>
    private void FireStaleBaseVetoes(List<PreparedIntent>? staleFlagged)
    {
        if (staleFlagged is null || onStaleBaseVeto is null)
            return;

        foreach (PreparedIntent intent in staleFlagged)
        {
            committedHeads.TryGetValue(intent.Key, out CommittedHead head);
            onStaleBaseVeto(intent, head.Revision);
        }
    }

    private bool ApplyLog(RaftLog log)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        if (!locallyProposedDeltas.TryTake(log.LogData, out PreparedIntentCommand[]? commands))
            commands = DecodeDelta(log.LogData);

        foreach (PreparedIntentCommand command in commands)
            Apply(command);

        return true;
    }

    // Wire tags of the delta envelope (field number << 3 | wire type 2, length-delimited), matching
    // prepared_intent_message.proto: field 1 is the repeated command, field 2 the shared header.
    private const uint DeltaCommandEntryTag = 1 << 3 | 2;
    private const uint DeltaHeaderTag = 2 << 3 | 2;

    // Scratch slice list for DecodeDelta on this thread. Decode is synchronous, so entries never
    // survive a call; the backing array persists across deltas.
    [ThreadStatic]
    private static List<(int Offset, int Length)>? scratchCommandSlices;

    /// <summary>
    /// Decodes a prepared-intent delta's transitions without applying them — the single codec every reader
    /// of these bytes must use (live apply, WAL replay, point-in-time restore), so a second decoder can
    /// never drift from the apply path.
    ///
    /// <para>Reads the wire directly instead of materializing the generated proto messages: a delta decode
    /// on this path allocates only the commands themselves plus their strings and value bytes, not a
    /// parallel message graph, and this is the hottest decode in the replication pipeline — every node
    /// decodes every delta it did not itself propose. The shared header is written after the commands, so
    /// the envelope walk records each command's slice first and the commands are built afterward against
    /// the header. When a proto field is added, update <see cref="ReadCommand"/> and the reference decoder
    /// <see cref="ToCommand"/> together — the encoding test decodes every delta both ways and fails on any
    /// disagreement.</para>
    /// </summary>
    internal static PreparedIntentCommand[] DecodeDelta(byte[] data)
    {
        ReadOnlySpan<byte> bytes = data;

        List<(int Offset, int Length)> slices = scratchCommandSlices ??= [];
        slices.Clear();

        int headerOffset = -1;
        int headerLength = 0;

        int pos = 0;
        while (pos < bytes.Length)
        {
            ulong tag = ReadWireTag(bytes, ref pos);
            if (tag == DeltaCommandEntryTag)
            {
                int length = ReadLengthPrefix(bytes, ref pos);
                slices.Add((pos, length));
                pos += length;
            }
            else if (tag == DeltaHeaderTag)
            {
                headerLength = ReadLengthPrefix(bytes, ref pos);
                headerOffset = pos;
                pos += headerLength;
            }
            else
                SkipField(bytes, ref pos, tag);
        }

        SharedHeaderFields header = default;
        bool hasHeader = headerOffset >= 0;
        if (hasHeader)
            ReadSharedHeader(bytes.Slice(headerOffset, headerLength), ref header);

        PreparedIntentCommand[] commands = new PreparedIntentCommand[slices.Count];
        for (int i = 0; i < commands.Length; i++)
        {
            (int offset, int length) = slices[i];
            commands[i] = ReadCommand(bytes.Slice(offset, length), in header, hasHeader);
        }

        return commands;
    }

    // ── wire primitives for the direct delta reader ───────────────────────────────

    /// <summary>Reads one base-128 varint (up to ten bytes, matching the protobuf limit). Callers narrow
    /// the result to the field's declared width, which reproduces protobuf's truncation semantics for
    /// int32-family fields encoded as 64-bit varints.</summary>
    private static ulong ReadVarint(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong result = 0;
        for (int shift = 0; shift < 70 && pos < data.Length; shift += 7)
        {
            byte b = data[pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
        }

        throw new InvalidDataException("Malformed varint in a prepared-intent delta.");
    }

    private static ulong ReadWireTag(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong tag = ReadVarint(data, ref pos);
        if (tag >> 3 == 0)
            throw new InvalidDataException("Zero field number in a prepared-intent delta.");
        return tag;
    }

    private static int ReadLengthPrefix(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong length = ReadVarint(data, ref pos);
        if (length > (ulong)(data.Length - pos))
            throw new InvalidDataException("Length prefix overruns the prepared-intent delta.");
        return (int)length;
    }

    private static string ReadLengthDelimitedString(ReadOnlySpan<byte> data, ref int pos)
    {
        int length = ReadLengthPrefix(data, ref pos);
        string result = System.Text.Encoding.UTF8.GetString(data.Slice(pos, length));
        pos += length;
        return result;
    }

    private static byte[] ReadLengthDelimitedBytes(ReadOnlySpan<byte> data, ref int pos)
    {
        int length = ReadLengthPrefix(data, ref pos);
        byte[] result = data.Slice(pos, length).ToArray();
        pos += length;
        return result;
    }

    /// <summary>Skips one unknown field by its wire type, so a delta written by a newer schema still
    /// decodes (the unknown data is dropped, never re-serialized — decoded commands are applied, not
    /// forwarded). Groups are rejected: proto3 never writes them.</summary>
    private static void SkipField(ReadOnlySpan<byte> data, ref int pos, ulong tag)
    {
        switch ((int)(tag & 7))
        {
            case 0: ReadVarint(data, ref pos); break;
            case 1:
                if (data.Length - pos < 8)
                    throw new InvalidDataException("Truncated fixed64 field in a prepared-intent delta.");
                pos += 8;
                break;
            case 2: pos += ReadLengthPrefix(data, ref pos); break;
            case 5:
                if (data.Length - pos < 4)
                    throw new InvalidDataException("Truncated fixed32 field in a prepared-intent delta.");
                pos += 4;
                break;
            default:
                throw new InvalidDataException($"Unsupported wire type {tag & 7} in a prepared-intent delta.");
        }
    }

    /// <summary>Raw fields of one delta's shared header, exactly as read from the wire. Absent fields keep
    /// their proto3 defaults, which is what the generated reader would also report.</summary>
    private struct SharedHeaderFields
    {
        internal int TxNode; internal long TxPhysical; internal uint TxCounter;
        internal long Epoch;
        internal long ManifestHash;
        internal string? AnchorKey;
        internal int CommitNode; internal long CommitPhysical; internal uint CommitCounter;
        internal int DeadlineNode; internal long DeadlinePhysical; internal uint DeadlineCounter;
    }

    // Field numbers follow PreparedIntentDeltaHeaderMessage in prepared_intent_message.proto.
    private static void ReadSharedHeader(ReadOnlySpan<byte> data, ref SharedHeaderFields header)
    {
        const int Varint = 0, Len = 2;

        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadWireTag(data, ref pos);
            switch (tag)
            {
                case 1 << 3 | Varint: header.TxNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 2 << 3 | Varint: header.TxPhysical = (long)ReadVarint(data, ref pos); break;
                case 3 << 3 | Varint: header.TxCounter = (uint)ReadVarint(data, ref pos); break;
                case 4 << 3 | Varint: header.Epoch = (long)ReadVarint(data, ref pos); break;
                case 5 << 3 | Varint: header.ManifestHash = (long)ReadVarint(data, ref pos); break;
                case 6 << 3 | Len: header.AnchorKey = ReadLengthDelimitedString(data, ref pos); break;
                case 7 << 3 | Varint: header.CommitNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 8 << 3 | Varint: header.CommitPhysical = (long)ReadVarint(data, ref pos); break;
                case 9 << 3 | Varint: header.CommitCounter = (uint)ReadVarint(data, ref pos); break;
                case 10 << 3 | Varint: header.DeadlineNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 11 << 3 | Varint: header.DeadlinePhysical = (long)ReadVarint(data, ref pos); break;
                case 12 << 3 | Varint: header.DeadlineCounter = (uint)ReadVarint(data, ref pos); break;
                default: SkipField(data, ref pos, tag); break;
            }
        }
    }

    /// <summary>Reads one command from its wire slice and builds the final command object, binding the
    /// header-hoisted fields (transaction identity, epoch, manifest hash, anchor key, commit timestamp,
    /// recovery deadline) from the shared header when the delta carries one. Field numbers follow
    /// PreparedIntentCommandMessage in prepared_intent_message.proto, and the header-fallback semantics
    /// mirror <see cref="ToCommand"/> exactly; the encoding test compares the two decoders so they cannot
    /// drift apart.</summary>
    private static PreparedIntentCommand ReadCommand(ReadOnlySpan<byte> data, in SharedHeaderFields header, bool hasHeader)
    {
        const int Varint = 0, Len = 2;

        PreparedIntentCommandKindMessage kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare;
        int txNode = 0; long txPhysical = 0; uint txCounter = 0;
        long epoch = 0;
        string key = string.Empty;
        bool commit = false;
        long manifestHash = 0;
        string anchorKey = string.Empty;
        int commitNode = 0; long commitPhysical = 0; uint commitCounter = 0;
        int state = 0;
        byte[] value = [];
        bool valueNull = false;
        string bucket = string.Empty;
        bool bucketNull = false;
        long revision = 0;
        int expiresNode = 0; long expiresPhysical = 0; uint expiresCounter = 0;
        bool noRevision = false;
        long baseRevision = 0;
        int baseState = 0;
        int deadlineNode = 0; long deadlinePhysical = 0; uint deadlineCounter = 0;
        int resolution = 0;

        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadWireTag(data, ref pos);
            switch (tag)
            {
                case 1 << 3 | Varint: kind = (PreparedIntentCommandKindMessage)(int)(uint)ReadVarint(data, ref pos); break;
                case 2 << 3 | Varint: txNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 3 << 3 | Varint: txPhysical = (long)ReadVarint(data, ref pos); break;
                case 4 << 3 | Varint: txCounter = (uint)ReadVarint(data, ref pos); break;
                case 5 << 3 | Varint: epoch = (long)ReadVarint(data, ref pos); break;
                case 6 << 3 | Len: key = ReadLengthDelimitedString(data, ref pos); break;
                case 7 << 3 | Varint: commit = ReadVarint(data, ref pos) != 0; break;
                case 8 << 3 | Varint: manifestHash = (long)ReadVarint(data, ref pos); break;
                case 9 << 3 | Len: anchorKey = ReadLengthDelimitedString(data, ref pos); break;
                case 10 << 3 | Varint: commitNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 11 << 3 | Varint: commitPhysical = (long)ReadVarint(data, ref pos); break;
                case 12 << 3 | Varint: commitCounter = (uint)ReadVarint(data, ref pos); break;
                case 13 << 3 | Varint: state = (int)(uint)ReadVarint(data, ref pos); break;
                case 14 << 3 | Len: value = ReadLengthDelimitedBytes(data, ref pos); break;
                case 15 << 3 | Varint: valueNull = ReadVarint(data, ref pos) != 0; break;
                case 16 << 3 | Len: bucket = ReadLengthDelimitedString(data, ref pos); break;
                case 17 << 3 | Varint: bucketNull = ReadVarint(data, ref pos) != 0; break;
                case 18 << 3 | Varint: revision = (long)ReadVarint(data, ref pos); break;
                case 19 << 3 | Varint: expiresNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 20 << 3 | Varint: expiresPhysical = (long)ReadVarint(data, ref pos); break;
                case 21 << 3 | Varint: expiresCounter = (uint)ReadVarint(data, ref pos); break;
                case 22 << 3 | Varint: noRevision = ReadVarint(data, ref pos) != 0; break;
                case 23 << 3 | Varint: baseRevision = (long)ReadVarint(data, ref pos); break;
                case 24 << 3 | Varint: baseState = (int)(uint)ReadVarint(data, ref pos); break;
                case 25 << 3 | Varint: deadlineNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 26 << 3 | Varint: deadlinePhysical = (long)ReadVarint(data, ref pos); break;
                case 27 << 3 | Varint: deadlineCounter = (uint)ReadVarint(data, ref pos); break;
                case 28 << 3 | Varint: resolution = (int)(uint)ReadVarint(data, ref pos); break;
                default: SkipField(data, ref pos, tag); break;
            }
        }

        HLCTimestamp txId = hasHeader
            ? new(header.TxNode, header.TxPhysical, header.TxCounter)
            : new(txNode, txPhysical, txCounter);
        long effectiveEpoch = hasHeader ? header.Epoch : epoch;

        switch (kind)
        {
            case PreparedIntentCommandKindMessage.PreparedIntentPrepare:
                return new PrepareIntentCommand(new PreparedIntent(
                    txId, effectiveEpoch, key,
                    hasHeader ? header.ManifestHash : manifestHash,
                    hasHeader ? header.AnchorKey ?? string.Empty : anchorKey,
                    hasHeader
                        ? new HLCTimestamp(header.CommitNode, header.CommitPhysical, header.CommitCounter)
                        : new HLCTimestamp(commitNode, commitPhysical, commitCounter),
                    (KeyValueState)state,
                    valueNull ? null : value,
                    bucketNull ? null : bucket,
                    revision,
                    new HLCTimestamp(expiresNode, expiresPhysical, expiresCounter),
                    noRevision,
                    baseRevision, (KeyValueState)baseState,
                    hasHeader
                        ? new HLCTimestamp(header.DeadlineNode, header.DeadlinePhysical, header.DeadlineCounter)
                        : new HLCTimestamp(deadlineNode, deadlinePhysical, deadlineCounter),
                    (PreparedIntentResolution)resolution));

            case PreparedIntentCommandKindMessage.PreparedIntentResolve:
                return new ResolveIntentCommand(txId, effectiveEpoch, key, commit);

            case PreparedIntentCommandKindMessage.PreparedIntentRemove:
                return new RemoveIntentCommand(txId, effectiveEpoch, key);

            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown prepared-intent command kind");
        }
    }

    /// <summary>Serializes a batch of transitions for one atomic data-partition log entry. The produced bytes
    /// remember their decoded commands so the local apply of this same entry can skip re-parsing them.</summary>
    public static byte[] SerializeDelta(IEnumerable<PreparedIntentCommand> commands)
    {
        PreparedIntentCommand[] batch = commands as PreparedIntentCommand[] ?? [.. commands];

        PreparedIntentDeltaMessage delta = scratchDelta ??= new();

        // Entry-side clear rather than trusting the previous call's cleanup: if an earlier serialization threw
        // mid-batch, its half-built commands are still parked here and must never leak into this delta. Discarded
        // (not pooled) because only messages that went through ResetCommandMessage may re-enter the pool.
        delta.Commands.Clear();
        delta.Header = null;

        foreach (PreparedIntentCommand command in batch)
            delta.Commands.Add(ToProto(command));

        HoistSharedHeader(delta);

        byte[] data = ReplicationSerializer.Serialize(delta);
        locallyProposedDeltas.Register(data, batch);

        // The proto layer is scaffolding: the bytes are final and the delta cache above holds the decoded
        // commands, never these messages, so they can be recycled for the next serialization on this thread.
        ReturnCommandMessages(delta);

        return data;
    }

    /// <summary>
    /// Upper bound on proto command messages retained per thread; a burst beyond the cap is dropped for the GC.
    /// Rent and return both happen inside the synchronous <see cref="SerializeDelta"/>, so unlike request pooling
    /// the pool has perfect thread locality.
    /// </summary>
    private const int MaxPooledCommandMessages = 1024;

    [ThreadStatic]
    private static Stack<PreparedIntentCommandMessage>? pooledCommandMessages;

    // Reused delta envelope for this thread's serializations; its repeated field keeps its backing array across
    // calls. Safe because SerializeDelta is synchronous with no awaits between first touch and last use.
    [ThreadStatic]
    private static PreparedIntentDeltaMessage? scratchDelta;

    private static PreparedIntentCommandMessage RentCommandMessage()
    {
        Stack<PreparedIntentCommandMessage>? pool = pooledCommandMessages;
        return pool is { Count: > 0 } ? pool.Pop() : new PreparedIntentCommandMessage();
    }

    private static void ReturnCommandMessages(PreparedIntentDeltaMessage delta)
    {
        Stack<PreparedIntentCommandMessage> pool = pooledCommandMessages ??= new();

        foreach (PreparedIntentCommandMessage message in delta.Commands)
        {
            if (pool.Count >= MaxPooledCommandMessages)
                break;

            ResetCommandMessage(message);
            pool.Push(message);
        }

        delta.Commands.Clear();
        delta.Header = null;
    }

    /// <summary>
    /// Returns every field to its proto3 default so a recycled message is indistinguishable from a fresh one.
    /// The fill paths (<see cref="FillPrepareProto"/> and the resolve/remove arms of <see cref="ToProto"/>) rely
    /// on this: each sets only the fields its kind carries, so a stale field surviving here would leak one
    /// command's payload into the next delta on this thread. Every field of
    /// <see cref="PreparedIntentCommandMessage"/> must be listed; the encoding test's descriptor sweep fails if a
    /// newly added proto field is missed. Internal for that test.
    /// </summary>
    internal static void ResetCommandMessage(PreparedIntentCommandMessage m)
    {
        m.Kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare;
        m.TransactionIdNode = 0; m.TransactionIdPhysical = 0; m.TransactionIdCounter = 0;
        m.Epoch = 0;
        m.Key = string.Empty;
        m.Commit = false;
        m.ManifestHash = 0;
        m.RecordAnchorKey = string.Empty;
        m.CommitTimestampNode = 0; m.CommitTimestampPhysical = 0; m.CommitTimestampCounter = 0;
        m.State = 0;
        m.Value = ByteString.Empty; m.ValueNull = false;
        m.Bucket = string.Empty; m.BucketNull = false;
        m.Revision = 0;
        m.ExpiresNode = 0; m.ExpiresPhysical = 0; m.ExpiresCounter = 0;
        m.NoRevision = false;
        m.BaseRevision = 0; m.BaseState = 0;
        m.RecoveryDeadlineNode = 0; m.RecoveryDeadlinePhysical = 0; m.RecoveryDeadlineCounter = 0;
        m.Resolution = 0;
    }

    /// <summary>
    /// Moves the fields every transition in the delta agrees on into one delta-level header and clears them from the
    /// commands, so the transaction identity, manifest hash, commit timestamp, recovery deadline and record-anchor
    /// key are encoded once for the batch instead of once per key. A cleared field is at its proto3 default and so is
    /// not written at all. Does nothing when the commands disagree, which keeps the per-command form correct for any
    /// delta this can't compact.
    /// </summary>
    private static void HoistSharedHeader(PreparedIntentDeltaMessage delta)
    {
        if (delta.Commands.Count < 2)
            return;

        PreparedIntentCommandMessage first = delta.Commands[0];

        // Every command's identity must agree. The mutation fields only exist on PREPARE commands, so they need only
        // agree among those — a resolve/remove never reads them back.
        PreparedIntentCommandMessage? firstPrepare = null;

        foreach (PreparedIntentCommandMessage command in delta.Commands)
        {
            if (command.TransactionIdNode != first.TransactionIdNode
                || command.TransactionIdPhysical != first.TransactionIdPhysical
                || command.TransactionIdCounter != first.TransactionIdCounter
                || command.Epoch != first.Epoch)
                return;

            if (command.Kind != PreparedIntentCommandKindMessage.PreparedIntentPrepare)
                continue;

            if (firstPrepare is null)
            {
                firstPrepare = command;
                continue;
            }

            if (command.ManifestHash != firstPrepare.ManifestHash
                || !string.Equals(command.RecordAnchorKey, firstPrepare.RecordAnchorKey, StringComparison.Ordinal)
                || command.CommitTimestampNode != firstPrepare.CommitTimestampNode
                || command.CommitTimestampPhysical != firstPrepare.CommitTimestampPhysical
                || command.CommitTimestampCounter != firstPrepare.CommitTimestampCounter
                || command.RecoveryDeadlineNode != firstPrepare.RecoveryDeadlineNode
                || command.RecoveryDeadlinePhysical != firstPrepare.RecoveryDeadlinePhysical
                || command.RecoveryDeadlineCounter != firstPrepare.RecoveryDeadlineCounter)
                return;
        }

        delta.Header = new()
        {
            TransactionIdNode = first.TransactionIdNode,
            TransactionIdPhysical = first.TransactionIdPhysical,
            TransactionIdCounter = first.TransactionIdCounter,
            Epoch = first.Epoch,
            ManifestHash = firstPrepare?.ManifestHash ?? 0,
            RecordAnchorKey = firstPrepare?.RecordAnchorKey ?? string.Empty,
            CommitTimestampNode = firstPrepare?.CommitTimestampNode ?? 0,
            CommitTimestampPhysical = firstPrepare?.CommitTimestampPhysical ?? 0,
            CommitTimestampCounter = firstPrepare?.CommitTimestampCounter ?? 0,
            RecoveryDeadlineNode = firstPrepare?.RecoveryDeadlineNode ?? 0,
            RecoveryDeadlinePhysical = firstPrepare?.RecoveryDeadlinePhysical ?? 0,
            RecoveryDeadlineCounter = firstPrepare?.RecoveryDeadlineCounter ?? 0
        };

        foreach (PreparedIntentCommandMessage command in delta.Commands)
        {
            command.TransactionIdNode = 0;
            command.TransactionIdPhysical = 0;
            command.TransactionIdCounter = 0;
            command.Epoch = 0;

            if (command.Kind != PreparedIntentCommandKindMessage.PreparedIntentPrepare)
                continue;

            command.ManifestHash = 0;
            command.RecordAnchorKey = string.Empty;
            command.CommitTimestampNode = 0;
            command.CommitTimestampPhysical = 0;
            command.CommitTimestampCounter = 0;
            command.RecoveryDeadlineNode = 0;
            command.RecoveryDeadlinePhysical = 0;
            command.RecoveryDeadlineCounter = 0;
        }
    }

    // Fills a pooled message; every rented message starts at proto3 defaults (see ResetCommandMessage), so each
    // arm only sets the fields its kind carries.
    private static PreparedIntentCommandMessage ToProto(PreparedIntentCommand command)
    {
        PreparedIntentCommandMessage m = RentCommandMessage();

        switch (command)
        {
            case PrepareIntentCommand prepare:
                return FillPrepareProto(m, prepare.Intent);

            case ResolveIntentCommand resolve:
                m.Kind = PreparedIntentCommandKindMessage.PreparedIntentResolve;
                m.TransactionIdNode = resolve.TransactionId.N; m.TransactionIdPhysical = resolve.TransactionId.L; m.TransactionIdCounter = resolve.TransactionId.C;
                m.Epoch = resolve.Epoch; m.Key = resolve.Key; m.Commit = resolve.Commit;
                return m;

            case RemoveIntentCommand remove:
                m.Kind = PreparedIntentCommandKindMessage.PreparedIntentRemove;
                m.TransactionIdNode = remove.TransactionId.N; m.TransactionIdPhysical = remove.TransactionId.L; m.TransactionIdCounter = remove.TransactionId.C;
                m.Epoch = remove.Epoch; m.Key = remove.Key;
                return m;

            default:
                throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown prepared-intent command");
        }
    }

    /// <summary>Rebuilds one transition, taking the fields the batch shares from <paramref name="header"/> when the
    /// writer hoisted them there and from the command itself when it did not.</summary>
    /// <summary>Reference decoder over the generated proto messages, kept as the readable statement of the
    /// wire semantics. Production decoding is <see cref="DecodeDelta"/>, which reads the same fields directly
    /// off the wire without the message graph; the encoding test decodes every delta through both and fails
    /// on any disagreement, so a change here without the matching <see cref="ReadCommand"/> change (or the
    /// reverse) cannot land silently. Internal for that test.</summary>
    internal static PreparedIntentCommand ToCommand(PreparedIntentCommandMessage m, PreparedIntentDeltaHeaderMessage? header)
    {
        HLCTimestamp txId = header is null
            ? new(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter)
            : new(header.TransactionIdNode, header.TransactionIdPhysical, header.TransactionIdCounter);

        long epoch = header?.Epoch ?? m.Epoch;

        switch (m.Kind)
        {
            case PreparedIntentCommandKindMessage.PreparedIntentPrepare:
                return new PrepareIntentCommand(IntentOf(m, header));

            case PreparedIntentCommandKindMessage.PreparedIntentResolve:
                return new ResolveIntentCommand(txId, epoch, m.Key, m.Commit);

            case PreparedIntentCommandKindMessage.PreparedIntentRemove:
                return new RemoveIntentCommand(txId, epoch, m.Key);

            default:
                throw new ArgumentOutOfRangeException(nameof(m), m.Kind, "unknown prepared-intent command kind");
        }
    }

    // Fresh-message form for the snapshot/state-transfer paths, which run at checkpoint cadence and don't pool.
    private static PreparedIntentCommandMessage PrepareProtoOf(PreparedIntent i) => FillPrepareProto(new PreparedIntentCommandMessage(), i);

    // A prepared intent maps to (and from) a PREPARE-kind command message, which carries every intent field plus
    // its resolution — reused for both delta commands (pooled messages) and full snapshot entries (fresh
    // messages). Sets every prepare-carried field, so the target may be a recycled message.
    private static PreparedIntentCommandMessage FillPrepareProto(PreparedIntentCommandMessage m, PreparedIntent i)
    {
        m.Kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare;
        m.TransactionIdNode = i.TransactionId.N; m.TransactionIdPhysical = i.TransactionId.L; m.TransactionIdCounter = i.TransactionId.C;
        m.Epoch = i.Epoch; m.Key = i.Key;
        m.ManifestHash = i.ManifestHash; m.RecordAnchorKey = i.RecordAnchorKey;
        m.CommitTimestampNode = i.CommitTimestamp.N; m.CommitTimestampPhysical = i.CommitTimestamp.L; m.CommitTimestampCounter = i.CommitTimestamp.C;
        m.State = (int)i.State;
        // Wrap the intent's value array without copying: the committed value is immutable and the message is
        // serialized synchronously by the caller before the array could change, so aliasing it here is safe and
        // avoids a full value copy per intent on every prepare/settle serialization.
        m.Value = i.Value is null ? ByteString.Empty : UnsafeByteOperations.UnsafeWrap(i.Value); m.ValueNull = i.Value is null;
        m.Bucket = i.Bucket ?? string.Empty; m.BucketNull = i.Bucket is null;
        m.Revision = i.Revision;
        m.ExpiresNode = i.Expires.N; m.ExpiresPhysical = i.Expires.L; m.ExpiresCounter = i.Expires.C;
        m.NoRevision = i.NoRevision;
        m.BaseRevision = i.BaseRevision; m.BaseState = (int)i.BaseState;
        m.RecoveryDeadlineNode = i.RecoveryDeadline.N; m.RecoveryDeadlinePhysical = i.RecoveryDeadline.L; m.RecoveryDeadlineCounter = i.RecoveryDeadline.C;
        m.Resolution = (int)i.Resolution;
        return m;
    }

    private static PreparedIntent IntentOf(PreparedIntentCommandMessage m, PreparedIntentDeltaHeaderMessage? header = null) => new(
        header is null
            ? new HLCTimestamp(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter)
            : new HLCTimestamp(header.TransactionIdNode, header.TransactionIdPhysical, header.TransactionIdCounter),
        header?.Epoch ?? m.Epoch, m.Key, header?.ManifestHash ?? m.ManifestHash, header?.RecordAnchorKey ?? m.RecordAnchorKey,
        header is null
            ? new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter)
            : new HLCTimestamp(header.CommitTimestampNode, header.CommitTimestampPhysical, header.CommitTimestampCounter),
        (KeyValueState)m.State,
        m.ValueNull ? null : m.Value.ToByteArray(),
        m.BucketNull ? null : m.Bucket,
        m.Revision,
        new HLCTimestamp(m.ExpiresNode, m.ExpiresPhysical, m.ExpiresCounter),
        m.NoRevision,
        m.BaseRevision, (KeyValueState)m.BaseState,
        header is null
            ? new HLCTimestamp(m.RecoveryDeadlineNode, m.RecoveryDeadlinePhysical, m.RecoveryDeadlineCounter)
            : new HLCTimestamp(header.RecoveryDeadlineNode, header.RecoveryDeadlinePhysical, header.RecoveryDeadlineCounter),
        (PreparedIntentResolution)m.Resolution);

    // ── durable snapshot ──────────────────────────────────────────────────────────

    /// <summary>Atomically rewrites this partition's on-disk intent snapshot. Returns true (durable) so the WAL
    /// checkpoint may discard the covered tail; false on write failure gates the checkpoint. No-op (true) when
    /// persistence or the resolver is not configured.</summary>
    public bool PersistSnapshot(int partitionId)
    {
        if (snapshotDirectory is null || snapshotPrefix is null || resolvePartition is null)
            return true;

        // Unchanged since this partition's last durable write: the file already holds exactly this content, so
        // the checkpoint may proceed without scanning or rewriting anything. The stamp is captured before the
        // scan and recorded only after a successful write, so a failed write or a mutation racing the scan
        // always leaves the partition due for a rewrite.
        long observedVersion = Math.Max(
            Interlocked.Read(ref allPartitionsVersion),
            partitionVersion.TryGetValue(partitionId, out long dirtyTick) ? dirtyTick : 0);
        long observedRouting = routingVersion?.Invoke() ?? 0;

        if (persistedVersion.TryGetValue(partitionId, out PersistedStamp last)
            && last.Version == observedVersion && last.RoutingVersion == observedRouting)
            return true;

        string path = Path.Combine(snapshotDirectory, $"{snapshotPrefix}_p{partitionId}.snapshot");

        try
        {
            // Entries stream straight into the temp file through one reused PREPARE-kind message, producing
            // the same bytes as serializing a whole PreparedIntentSnapshotMessage: each entry is written
            // length-delimited under the repeated field's tag. Materializing one protobuf object per retained
            // intent plus one byte[] for the whole set made every checkpoint's allocation proportional to the
            // store size, which dominated the node's allocation profile whenever the retained set was large.
            lock (fileLock)
            {
                string tmp = path + ".tmp";

                using (FileStream file = new(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024))
                using (CodedOutputStream output = new(file))
                {
                    PreparedIntentCommandMessage entry = new();

                    foreach (PreparedIntent intent in intents.Values)
                    {
                        if (resolvePartition(intent.Key) != partitionId)
                            continue;

                        FillPrepareProto(entry, intent);
                        output.WriteTag(PreparedIntentSnapshotMessage.IntentsFieldNumber, WireFormat.WireType.LengthDelimited);
                        output.WriteMessage(entry);
                    }
                }

                File.Move(tmp, path, overwrite: true);
            }

            persistedVersion[partitionId] = new PersistedStamp(observedVersion, observedRouting);
            return true;
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "Failed to persist prepared-intent snapshot to {Path}", path);
            return false;
        }
    }

    private void LoadFromDisk()
    {
        if (snapshotDirectory is null || snapshotPrefix is null || !Directory.Exists(snapshotDirectory))
            return;

        string[] files;
        lock (fileLock)
            files = Directory.GetFiles(snapshotDirectory, $"{snapshotPrefix}_p*.snapshot");

        foreach (string path in files)
        {
            byte[] data;
            try
            {
                lock (fileLock)
                    data = File.ReadAllBytes(path);
            }
            catch (Exception ex)
            {
                throw new IOException($"Failed to read prepared-intent snapshot {path}; refusing to start with a possibly incomplete intent set", ex);
            }

            PreparedIntentSnapshotMessage message;

            try
            {
                message = ReplicationSerializer.UnserializePreparedIntentSnapshotMessage(data);
            }
            catch (Exception ex)
            {
                throw new InvalidDataException($"Corrupt prepared-intent snapshot {path}; refusing to start empty and lose a prepared intent", ex);
            }

            foreach (PreparedIntentCommandMessage entry in message.Intents)
                MergeLoad(IntentOf(entry));
        }
    }

    // Load-time merge across (possibly overlapping) per-partition files: at most one live intent per key. A more
    // resolved intent for the same identity is authoritative; a different transaction on the same key is a
    // conflict that keeps the existing intent and is logged rather than silently overwritten.
    private void MergeLoad(PreparedIntent incoming)
    {
        if (!intents.TryGetValue(incoming.Key, out PreparedIntent? existing))
        {
            intents[incoming.Key] = incoming;
            Interlocked.Add(ref totalBytes, IntentBytes(incoming));
            StampDirty(incoming.Key);
            return;
        }

        if (existing.TransactionId != incoming.TransactionId || existing.Epoch != incoming.Epoch)
        {
            logger?.LogError("Conflicting prepared intents on load for key {Key}: {A} vs {B}",
                incoming.Key, existing.TransactionId, incoming.TransactionId);
            return;
        }

        if (existing.IsPending && incoming.IsResolved)
        {
            intents[incoming.Key] = incoming;
            Interlocked.Add(ref totalBytes, IntentBytes(incoming) - IntentBytes(existing));
            StampDirty(incoming.Key);
        }
    }

    // ── state transfer (split/merge) ────────────────────────────────────────────────

    /// <summary>Intents whose key routes into <c>[startKey, endKey)</c> (ordinal, half-open) — the set a range
    /// split/merge hands to the destination partition.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotRange(string? startKey, string? endKey)
    {
        List<PreparedIntent> result = [];

        foreach (PreparedIntent intent in intents.Values)
        {
            if (startKey is not null && string.CompareOrdinal(intent.Key, startKey) < 0)
                continue;

            if (endKey is not null && string.CompareOrdinal(intent.Key, endKey) >= 0)
                continue;

            result.Add(intent);
        }

        return result;
    }

    /// <summary>Intents whose key belongs to <paramref name="bucket"/> (its parent prefix) — the set a bucket scan
    /// (<c>GetByBucket</c>) reconciles against. Uses the intent's own bucket, which the freeze sources from the key
    /// (its parent prefix), so an intent-only committed key is included/overridden/excluded in a bucket scan exactly
    /// as it would be in the equivalent range scan.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotBucket(string? bucket)
    {
        List<PreparedIntent> result = [];

        foreach (PreparedIntent intent in intents.Values)
        {
            if (string.Equals(intent.Bucket, bucket, StringComparison.Ordinal))
                result.Add(intent);
        }

        return result;
    }

    /// <summary>Intents covering a scan page's window with the scan's own boundary semantics: start-exclusivity for a
    /// continuation cursor (<paramref name="startInclusive"/> false skips an intent exactly at <paramref name="startKey"/>,
    /// which the prior page already emitted) and end-inclusivity (<paramref name="endInclusive"/> true keeps an intent
    /// exactly at <paramref name="endKey"/>). This is distinct from <see cref="SnapshotRange"/>'s fixed half-open
    /// <c>[start, end)</c> so the overlaid intent set matches exactly the window the scan's KV rows were drawn from —
    /// without it a boundary intent is re-emitted across pages or an inclusive-end intent is missed.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotScanWindow(string? startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        List<PreparedIntent> result = [];

        foreach (PreparedIntent intent in intents.Values)
        {
            if (startKey is not null)
            {
                int cmpStart = string.CompareOrdinal(intent.Key, startKey);
                if (cmpStart < 0 || (!startInclusive && cmpStart == 0))
                    continue;
            }

            if (endKey is not null)
            {
                int cmpEnd = string.CompareOrdinal(intent.Key, endKey);
                if (cmpEnd > 0 || (!endInclusive && cmpEnd == 0))
                    continue;
            }

            result.Add(intent);
        }

        return result;
    }

    /// <summary>
    /// Drops every intent whose key satisfies <paramref name="shouldRemove"/> and returns how many were
    /// removed. This is the un-host purge: when this node stops being a replica of the keys' partition, the
    /// intents' resolution lives with the partition's replicas (and returns in a seeding snapshot on any
    /// re-gain), so the local copies are dead retention. Byte accounting is adjusted and the change stamp
    /// bumped so the next per-partition snapshot rewrites the emptied slice.
    /// </summary>
    public int PurgeWhere(Func<string, bool> shouldRemove)
    {
        int removedCount;

        lock (applyGate)
        {
            List<string>? toRemove = null;

            foreach (KeyValuePair<string, PreparedIntent> kv in intents)
            {
                if (shouldRemove(kv.Key))
                    (toRemove ??= []).Add(kv.Key);
            }

            if (toRemove is null)
                return 0;

            foreach (string key in toRemove)
            {
                if (intents.TryRemove(key, out PreparedIntent? removed))
                    Interlocked.Add(ref totalBytes, -IntentBytes(removed));
            }

            StampAllDirty();
            removedCount = toRemove.Count;
        }

        SignalFenceWaiters();
        return removedCount;
    }

    /// <summary>Folds transferred intents into this partition's set (idempotent by key + resolution authority).</summary>
    public void ImportIntents(IEnumerable<PreparedIntent> incoming)
    {
        foreach (PreparedIntent intent in incoming)
            MergeLoad(intent);

        SignalFenceWaiters();
    }

    public static byte[] SerializeIntents(IEnumerable<PreparedIntent> intents)
    {
        PreparedIntentSnapshotMessage message = new();
        foreach (PreparedIntent intent in intents)
            message.Intents.Add(PrepareProtoOf(intent));

        return ReplicationSerializer.Serialize(message);
    }

    public static IReadOnlyList<PreparedIntent> DeserializeIntents(byte[] data)
    {
        PreparedIntentSnapshotMessage message = ReplicationSerializer.UnserializePreparedIntentSnapshotMessage(data);
        List<PreparedIntent> result = new(message.Intents.Count);
        foreach (PreparedIntentCommandMessage entry in message.Intents)
            result.Add(IntentOf(entry));

        return result;
    }
}
