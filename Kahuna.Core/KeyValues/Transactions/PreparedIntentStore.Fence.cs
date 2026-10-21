using System.Collections.Concurrent;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>Stale-base fence judgement, wedge watchdog, and the one-phase bundled commit verdict of <see cref="PreparedIntentStore"/>.</summary>
internal sealed partial class PreparedIntentStore
{
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
    /// refuses repeatedly at an unchanged (base, committed head) pair, and returns whether the convergence
    /// repair hook is due for this refusal (rate-limited by the streak). <paramref name="baseRevision"/> is the
    /// revision the refused write was built on: its validated base, or the entry a colliding revision was
    /// staged over. <paramref name="headRevision"/> reports the remembered committed head the refusal was
    /// judged against. Caller holds <see cref="applyGate"/>.</summary>
    private bool TrackFenceRefusal(string key, long baseRevision, out long headRevision)
    {
        TryFindHead(key, out CommittedHead head);
        headRevision = head.Revision;

        if (!fenceRefusalStreaks.TryGetValue(key, out FenceRefusalStreak streak)
            || streak.BaseRevision != baseRevision
            || streak.HeadRevision != head.Revision)
        {
            if (fenceRefusalStreaks.Count >= FenceRefusalTrackerMaxKeys && !fenceRefusalStreaks.ContainsKey(key))
                return false;

            fenceRefusalStreaks[key] = new(baseRevision, head.Revision, 1);
            return false;
        }

        long count = streak.Count + 1;
        fenceRefusalStreaks[key] = streak with { Count = count };

        if (count == FenceWedgeAlarmThreshold || (count > FenceWedgeAlarmThreshold && count % FenceWedgeReAlarmEvery == 0))
        {
            DurableTransactionMetrics.StagedBaseFenceWedgedKeys.Add(1);

            logger?.LogError(
                "Key {Key} refused {Count} consecutive prepares at a frozen pair (base revision {BaseRevision}, committed head {HeadRevision}): the node's visible entry has stopped converging with its committed head and the key is effectively read-only until it reconciles",
                key, count, baseRevision, head.Revision);
        }

        return count == FenceWedgeRepairThreshold
            || (count > FenceWedgeRepairThreshold && count % FenceWedgeRepairRepeatEvery == 0);
    }

    /// <summary>Whether the staged-base fence judges a prepare at all: a write with a validated base, or any
    /// write that carries a revision. Only a validated-free revision-free write (<c>SET NOREV</c>, blind) has
    /// nothing the committed-head memory can check.</summary>
    private static bool IsFenceJudged(PreparedIntent intent) => intent.HasValidatedBase || !intent.NoRevision;

    /// <summary>
    /// Decides whether a freshly installed prepare deserves its acknowledgement, against the committed-head
    /// memory. Returns null to acknowledge, or the conflict reason; <paramref name="revisionCollision"/> tells
    /// the staged-revision rule refused it rather than the validated-base rule. Caller holds
    /// <see cref="applyGate"/>.
    /// </summary>
    private string? EvaluateStagedBaseFence(PreparedIntent intent, out bool revisionCollision) =>
        JudgeStagedBase(intent, countAbsentHeadAdmission: true, out _, out revisionCollision);

    /// <summary>The ADVISORY fence decision, shared by the apply-path acknowledgement and the replica verdict
    /// read (<see cref="EvaluateReplicaFenceVerdicts"/>). Judges by key across every ledger slice this node
    /// holds — the two-phase acknowledgement is per-node advice, so the widest memory is the most useful —
    /// against the node-wide watermark. Returns null to admit, or the conflict reason.
    /// <paramref name="headRevision"/> reports the remembered head the verdict was judged against (-1 when the
    /// memory held nothing); <paramref name="revisionCollision"/> tells the staged-revision rule refused it.
    /// Only the apply path counts the absent-head admission; a verdict read must not inflate that counter.
    /// Caller holds <see cref="applyGate"/>.</summary>
    private string? JudgeStagedBase(PreparedIntent intent, bool countAbsentHeadAdmission, out long headRevision, out bool revisionCollision)
    {
        headRevision = -1;
        revisionCollision = false;

        // Staleness gate, the partner of retention pruning: a pruned head cannot be distinguished from "no
        // commit happened", so a prepare from a transaction that BEGAN before the retention horizon (its reads,
        // and therefore its base, may predate every retained head) must not be acknowledged on absence of
        // evidence. The transaction id is the begin HLC, so it lower-bounds every read the base came from.
        // Measured against the head watermark — an HLC from replicated commits — never a local clock. A blind
        // write has no base to verify, so the gate does not apply to it.
        if (intent.HasValidatedBase && IsOlderThanRetention(intent.TransactionId, ledgerWatermark))
            return $"transaction began before the staged-base fence retention horizon; its validated base for key {intent.Key} cannot be verified";

        if (!TryFindHead(intent.Key, out CommittedHead head))
        {
            // The only silent path around the fence: nothing to check is indistinguishable from "no commit
            // ever happened" — count it so a loss investigation can tell proof-of-currency from absence.
            if (countAbsentHeadAdmission && intent.HasValidatedBase)
                DurableTransactionMetrics.FenceAdmissionsAbsentHead.Add(1);
            return null;
        }

        headRevision = head.Revision;

        if (intent.HasValidatedBase)
        {
            string? baseConflict = JudgeBaseAgainstHead(intent.Key, intent.BaseRevision, intent.BaseState, head);
            if (baseConflict is not null)
                return baseConflict;
        }

        string? revisionConflict = JudgeRevisionAgainstHead(intent, head);
        revisionCollision = revisionConflict is not null;
        return revisionConflict;
    }

    /// <summary>
    /// The staged-revision rule shared by the advisory fence and the deterministic bundled commit gate: a
    /// revision-carrying write must stage a revision strictly above the key's committed head. Returns null to
    /// admit, or the conflict reason.
    ///
    /// <para>A revision identifies one committed mutation of a key, and every staging site allocates the next
    /// one from the entry it holds exclusively (a set and a delete both stage base+1), so a key's revisions
    /// only move forward and a head at or above the staged revision means another transaction already
    /// committed it. That is only reachable when the writer's exclusion was lost between staging and commit —
    /// its point lock and write intent lived in a deposed leader's memory — and committing it would put two
    /// records at one revision, the later silently replacing the earlier. A head below the staged revision is
    /// the ordinary case (non-transactional writes never feed the memory, so the head may lag the key).
    /// Revision-free writes allocate nothing and are not judged.</para>
    /// </summary>
    private static string? JudgeRevisionAgainstHead(PreparedIntent intent, CommittedHead head)
    {
        if (intent.NoRevision || head.Revision < intent.Revision)
            return null;

        return $"staged revision {intent.Revision} for key {intent.Key} is already committed: committed head is revision {head.Revision}";
    }

    /// <summary>The base-versus-head rule shared by the advisory fence and the deterministic bundled commit
    /// gate. Returns null to admit, or the conflict reason.</summary>
    private static string? JudgeBaseAgainstHead(string key, long baseRevision, KeyValueState baseState, CommittedHead head)
    {
        // BaseState says whether the validated base was an existing value (the finalize-input builder maps an
        // observed non-existent base to Undefined). PreparedIntent.UnknownBaseRevision (no base at all) never
        // reaches this method. A transactional delete recorded as the head keeps the key absent, so it is a
        // valid base for a validated-absent insert.
        if (baseState != KeyValueState.Set)
        {
            return head.State == KeyValueState.Set
                ? $"committed base for key {key} changed after validation: validated absent, committed head is now revision {head.Revision}"
                : null;
        }

        // Only a head that moved PAST the validated base is a conflict. A head at the base is exactly the
        // commit the read observed; a head behind the base means non-transactional writes advanced the key
        // after that commit — this memory can attest nothing newer there, and refusing would wedge every later
        // read-modify-write of the key until the entry ages out.
        return head.Revision > baseRevision
            ? $"committed base for key {key} changed after validation: validated revision {baseRevision}, committed head is now revision {head.Revision}"
            : null;
    }

    /// <summary>Whether a transaction that began at <paramref name="transactionId"/> predates the retention
    /// horizon measured from <paramref name="watermark"/>: its base or read may depend on a head the window no
    /// longer retains, so nothing about it can be verified. Never true while the watermark is unset.</summary>
    private bool IsOlderThanRetention(HLCTimestamp transactionId, HLCTimestamp watermark) =>
        watermark != HLCTimestamp.Zero && transactionId.L + stagedBaseFenceRetentionMs < watermark.L;

    /// <summary>Whether a head is inside the retention window measured from its slice's watermark — the
    /// logical presence the bundled commit gate judges by, independent of physical pruning.</summary>
    private bool IsWithinRetention(CommittedHead head, HLCTimestamp watermark) =>
        head.CommittedAt.L >= watermark.L - stagedBaseFenceRetentionMs;

    /// <summary>Finds a key's committed head across every ledger slice (the highest revision when several
    /// slices remember the key — a range that moved between partitions leaves its history behind). Lock-free;
    /// the advisory reads probe it per operation.</summary>
    private bool TryFindHead(string key, out CommittedHead head)
    {
        head = default;
        bool found = false;

        // Enumerated directly: ConcurrentDictionary.Values takes every bucket lock and copies a
        // snapshot list per call, while the enumerator is lock-free — this probe runs per prepare
        // apply under the apply gate.
        foreach (KeyValuePair<int, PartitionLedger> slice in ledgers)
        {
            if (slice.Value.Heads.TryGetValue(key, out CommittedHead candidate) && (!found || candidate.Revision > head.Revision))
            {
                head = candidate;
                found = true;
            }
        }

        return found;
    }

    /// <summary>
    /// The deterministic verdict on a one-phase bundled commit at its apply position on
    /// <paramref name="partitionId"/>'s log, consumed by the transaction-record store's bundled commit gate.
    /// Every bundled key must be held by this transaction's live intent (the prepare applied earlier in the
    /// same atomic batch). When the command asks for apply-time validation, each co-bundled intent's validated
    /// base and staged revision and each carried read-only dependency are additionally judged against the
    /// partition's ledger slice: a foreign undecided or committed intent on a read key, a head above the
    /// validated base or the observed revision, a head at or above the staged revision, a value where absence
    /// was validated, or a transaction older than the slice's retention horizon rejects. Only replicated state is consulted — the intent map, the slice's heads and its
    /// watermark — so every replica of the partition reaches the same verdict for the same log entry.
    /// Evaluated under the apply gate so the (intents, heads, watermark) read is one consistent state.
    /// </summary>
    internal BundledCommitJudgement JudgeBundledCommit(int partitionId, CommitTransactionCommand commit)
    {
        lock (applyGate)
        {
            ledgers.TryGetValue(partitionId, out PartitionLedger? ledger);
            HLCTimestamp watermark = ledger?.Watermark ?? HLCTimestamp.Zero;

            if (commit.BundledPrepareKeys is not null)
            {
                foreach (string bundledKey in commit.BundledPrepareKeys)
                {
                    if (!intents.TryGetValue(bundledKey, out PreparedIntent? intent)
                        || intent.TransactionId != commit.TransactionId
                        || intent.Epoch != commit.Epoch)
                        return new(BundledCommitVerdict.PrepareMissing, $"bundled prepare for key {bundledKey} is not held by transaction {commit.TransactionId}");

                    if (!commit.ApplyTimeValidation || !IsFenceJudged(intent))
                        continue;

                    if (intent.HasValidatedBase && IsOlderThanRetention(commit.TransactionId, watermark))
                        return new(BundledCommitVerdict.StaleBase, $"transaction began before the ledger retention horizon; its validated base for key {bundledKey} cannot be verified");

                    if (ledger is null
                        || !ledger.Heads.TryGetValue(bundledKey, out CommittedHead head)
                        || !IsWithinRetention(head, watermark))
                        continue;

                    if (intent.HasValidatedBase)
                    {
                        string? conflict = JudgeBaseAgainstHead(bundledKey, intent.BaseRevision, intent.BaseState, head);
                        if (conflict is not null)
                            return new(BundledCommitVerdict.StaleBase, conflict);
                    }

                    // A blind write has no base, but its staged revision must still be new: a head already at it
                    // is another transaction's commit of that revision, and committing this bundle would put a
                    // second record at it that silently replaces the first. Final like a moved base — heads only
                    // advance — so it is reported as one and drives the same truthful conflict abort.
                    string? revisionConflict = JudgeRevisionAgainstHead(intent, head);
                    if (revisionConflict is not null)
                    {
                        DurableTransactionMetrics.StagedRevisionCollisions.Add(1);
                        return new(BundledCommitVerdict.StaleBase, revisionConflict);
                    }
                }
            }

            if (!commit.ApplyTimeValidation || commit.BundledReadDependencies is not { Count: > 0 } reads)
                return BundledCommitJudgement.Admitted;

            if (IsOlderThanRetention(commit.TransactionId, watermark))
                return new(BundledCommitVerdict.StaleRead, "transaction began before the ledger retention horizon; its read-set cannot be verified");

            foreach (BundledReadDependency read in reads)
            {
                // A foreign intent on a read key is a competitor's write that this transaction's validation
                // never saw: undecided, it may still commit over the read; committed, it already did. An
                // aborted holder never materializes and is no objection.
                if (intents.TryGetValue(read.Key, out PreparedIntent? holder)
                    && (holder.TransactionId != commit.TransactionId || holder.Epoch != commit.Epoch)
                    && holder.Resolution != PreparedIntentResolution.Aborted)
                    return new(BundledCommitVerdict.StaleRead, $"read key {read.Key} is held by a live prepared intent of transaction {holder.TransactionId}");

                if (ledger is null
                    || !ledger.Heads.TryGetValue(read.Key, out CommittedHead head)
                    || !IsWithinRetention(head, watermark))
                    continue;

                // The same rule as a validated base: an observed value is stale once a head moved past its
                // revision; an observed absence is stale once a head made the key a value.
                string? conflict = JudgeBaseAgainstHead(
                    read.Key, read.ObservedRevision, read.ObservedExists ? KeyValueState.Set : KeyValueState.Undefined, head);
                if (conflict is not null)
                    return new(BundledCommitVerdict.StaleRead, conflict.Replace("committed base", "read dependency"));
            }

            return BundledCommitJudgement.Admitted;
        }
    }

}
