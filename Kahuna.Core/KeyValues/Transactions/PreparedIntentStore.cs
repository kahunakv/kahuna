
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
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

    // Monotonic change stamp for the intent set, bumped whenever an intent is installed, updated, or removed. A
    // partition's checkpoint snapshot is skipped when the set hasn't changed since that partition's last durable
    // write — the file on disk already reflects exactly this content — which turns the common quiet checkpoint
    // from a full scan + serialize + rewrite into a counter comparison. Intents only change partitions through
    // replicated split/merge transfer deltas, which apply here and bump the stamp, so a skip can never hide a
    // moved intent.
    private long version;

    // Per-partition value of <see cref="version"/> captured just before that partition's last successful
    // snapshot write. Captured before the scan, so a mutation racing the scan leaves the stamp ahead and the
    // next checkpoint rewrites the file.
    private readonly ConcurrentDictionary<int, long> persistedVersion = new();

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

    /// <summary>Wires the key → data-partition resolver once the locator exists (manager construction).</summary>
    public void AttachPartitionResolver(Func<string, int> resolver) => resolvePartition = resolver;

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

    /// <summary>Sets the staged-base fence retention horizon. The caller must pass a value comfortably above
    /// the longest possible transaction lifetime: the staleness gate refuses to acknowledge a validated-base
    /// prepare from a transaction older than this horizon, so a horizon below real transaction lifetimes turns
    /// long transactions into spurious aborts. Idempotent; safe to call on a shared store.</summary>
    public void ConfigureStagedBaseFence(int retentionMs) =>
        stagedBaseFenceRetentionMs = Math.Max(1, retentionMs);

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

                Interlocked.Increment(ref version);
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
    private string? EvaluateStagedBaseFence(PreparedIntent intent)
    {
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
            DurableTransactionMetrics.FenceAdmissionsAbsentHead.Add(1);
            return null;
        }

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

    // The node that proposes a delta serialized it from live command objects moments before Raft hands the very
    // same byte array back to the local apply path, so the decoded form is registered against the produced bytes
    // and the local apply reuses it instead of parsing and rebuilding every command. The table is weak on the
    // byte array, so an entry vanishes with the proposal bytes; any reader that misses — a follower, WAL replay
    // on restart, state transfer, all of which see freshly materialized arrays — parses as before, so
    // correctness never depends on a hit. Commands and their intents are immutable, which is what makes reusing
    // the producer's instances safe even when an in-process transport shares the array across nodes.
    private static readonly ConditionalWeakTable<byte[], PreparedIntentCommand[]> locallyProposedDeltas = new();

    // A hit is taken single-shot: with the redundant-apply ledger only one local apply runs per committed entry,
    // and clearing on take keeps the WAL's in-memory retention of the bytes from pinning the decoded commands.
    // A concurrent second taker simply misses and parses, which is the same double work it did before.
    private static bool TryTakeLocallyProposed(byte[] data, [NotNullWhen(true)] out PreparedIntentCommand[]? commands)
    {
        if (!locallyProposedDeltas.TryGetValue(data, out commands))
            return false;

        locallyProposedDeltas.Remove(data);
        return true;
    }

    /// <summary>Applies a delta and reports whether every PREPARE command in it took ownership of its key. Returns
    /// <see langword="false"/> when any prepare is rejected by the state machine — another transaction already
    /// holds the key, or the same identity re-prepared a divergent mutation — or when the staged-base fence
    /// flagged an applied prepare's validated base as moved (<see cref="PreparedIntentApplyResult.StaleBase"/>).
    /// Neither is an acknowledged prepare: the producer must abort rather than commit a mutation whose
    /// recoverable intent it never owned, or whose base a competitor already re-committed (a lost update).
    /// Resolve/remove deltas carry no prepares, so they always report true.</summary>
    public bool ApplyDeltaAckPrepares(RaftLog log)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        bool allPreparesAccepted = true;

        if (TryTakeLocallyProposed(log.LogData, out PreparedIntentCommand[]? proposed))
        {
            foreach (PreparedIntentCommand command in proposed)
            {
                PreparedIntentApplyResult result = Apply(command);
                if (command is PrepareIntentCommand && (result.Outcome == TransactionApplyOutcome.Rejected || result.StaleBase))
                    allPreparesAccepted = false;
            }

            return allPreparesAccepted;
        }

        PreparedIntentDeltaMessage delta = ReplicationSerializer.UnserializePreparedIntentDeltaMessage(log.LogData);

        foreach (PreparedIntentCommandMessage message in delta.Commands)
        {
            PreparedIntentCommand command = ToCommand(message, delta.Header);
            PreparedIntentApplyResult result = Apply(command);
            if (command is PrepareIntentCommand && (result.Outcome == TransactionApplyOutcome.Rejected || result.StaleBase))
                allPreparesAccepted = false;
        }

        return allPreparesAccepted;
    }

    private bool ApplyLog(RaftLog log)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        if (TryTakeLocallyProposed(log.LogData, out PreparedIntentCommand[]? proposed))
        {
            foreach (PreparedIntentCommand command in proposed)
                Apply(command);

            return true;
        }

        PreparedIntentDeltaMessage delta = ReplicationSerializer.UnserializePreparedIntentDeltaMessage(log.LogData);

        foreach (PreparedIntentCommandMessage message in delta.Commands)
            Apply(ToCommand(message, delta.Header));

        return true;
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
        locallyProposedDeltas.AddOrUpdate(data, batch);

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
    private static PreparedIntentCommand ToCommand(PreparedIntentCommandMessage m, PreparedIntentDeltaHeaderMessage? header)
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
        long observedVersion = Interlocked.Read(ref version);
        if (persistedVersion.TryGetValue(partitionId, out long lastPersisted) && lastPersisted == observedVersion)
            return true;

        string path = Path.Combine(snapshotDirectory, $"{snapshotPrefix}_p{partitionId}.snapshot");

        try
        {
            PreparedIntentSnapshotMessage message = new();
            foreach (PreparedIntent intent in intents.Values)
            {
                if (resolvePartition(intent.Key) == partitionId)
                    message.Intents.Add(PrepareProtoOf(intent));
            }

            byte[] data = ReplicationSerializer.Serialize(message);
            lock (fileLock)
            {
                string tmp = path + ".tmp";
                File.WriteAllBytes(tmp, data);
                File.Move(tmp, path, overwrite: true);
            }

            persistedVersion[partitionId] = observedVersion;
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
            Interlocked.Increment(ref version);
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
            Interlocked.Increment(ref version);
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

            Interlocked.Increment(ref version);
            return toRemove.Count;
        }
    }

    /// <summary>Folds transferred intents into this partition's set (idempotent by key + resolution authority).</summary>
    public void ImportIntents(IEnumerable<PreparedIntent> incoming)
    {
        foreach (PreparedIntent intent in incoming)
            MergeLoad(intent);
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
