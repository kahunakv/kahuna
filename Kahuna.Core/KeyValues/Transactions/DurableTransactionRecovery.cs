using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The participant-side recovery sweep of the durable-intent 2PC model. A partition leader periodically resolves
/// its own unresolved prepared intents whose recovery deadline has passed: it looks up each transaction's
/// canonical record, and applies its terminal decision (commit → materialize; abort → discard). For an intent
/// whose record is still <see cref="TransactionDecision.Undecided"/> past its decision deadline — or has no
/// record at all (an orphan prepare that outlived a failed anchor initialization) — it drives an idempotent
/// presumed-abort at the anchor and then resolves the intent to whatever the canonical record actually became
/// (a concurrent in-flight commit can still win the race). It never guesses an outcome and never resolves an
/// intent whose record is undecided but still within its deadline.
///
/// <para>Record absence is treated as an orphan only while the intent is younger than the record retention
/// horizon. Past that age the absent record may equally be a COMMITTED record the retention GC reclaimed while
/// this leg's settlement was still failing (the GC's settlement guard sees only intents this node holds, and a
/// completion receipt exists only once a leg materializes — an unmaterialized committed leg blocks neither), so
/// presuming abort would mint a tombstone over a committed transaction's history and discard the only durable
/// copy of its value: a silent lost write, observed downstream as a transfer with one leg missing. Such an
/// intent is held (and surfaced through <see cref="DurableTransactionMetrics.RecordlessIntentHolds"/>) instead
/// of resolved; a failed-init orphan cannot ordinarily reach that age, because the sweep aborts it within its
/// decision deadline — far inside the retention window.</para>
/// </summary>
internal sealed class DurableTransactionRecovery
{
    /// <summary>Reads the canonical transaction record from its anchor partition (local store or a remote lookup).</summary>
    public delegate Task<TransactionRecord?> LookupRecordDelegate(HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken);

    /// <summary>Drives an idempotent abort transition at the anchor and returns the record as it stands after it
    /// (the winner — a concurrent commit is not overwritten).</summary>
    public delegate Task<TransactionRecord?> DriveAbortDelegate(AbortTransactionCommand abort, string anchorKey, CancellationToken cancellationToken);

    private readonly PreparedIntentStore intentStore;

    private readonly DurableTransactionFinalizer.ReplicateDelegate replicate;

    private readonly LookupRecordDelegate lookupRecord;

    private readonly DriveAbortDelegate driveAbort;

    private readonly DurableTransactionFinalizer.ApplyCommitLocally? applyCommitLocally;

    // A locally visible terminal Abort is definitive (an abort never overwrites a commit, and terminal
    // records replicate only through the canonical log). Checked immediately before a commit-direction
    // materialization is proposed, so a settle racing a decision it read moments earlier can never push
    // an aborted transaction's value into the log. Null disables the fence (protocol tests).
    private readonly Func<HLCTimestamp, long, bool>? locallyAborted;

    // The record retention horizon (ms). An absent record for an intent older than this is ambiguous — orphan
    // or reclaimed-after-commit — and must not be presumed aborted. Matches the retention GC's TTL.
    private readonly long recordRetentionMs;

    private readonly ILogger<IKahuna>? logger;

    // Emits the value-free by-reference materialization record instead of copying the committed value into the
    // log a second time. Off unless every node in the cluster applies that record (see the configuration flag's
    // upgrade order); an older node skips an unknown message type, which loses the write on that node.
    private readonly bool materializeByReference;

    // Materialization window caps, the same the finalizer's resolution uses: one window coalesces into one capped
    // scheduler proposal, and a large group advances window by window instead of admitting everything at once.
    private readonly int maxMaterializationBatchItems;

    private readonly long maxMaterializationBatchBytes;

    // Bounds concurrent leader-local applies; the node's shared gate in production, a private one otherwise.
    private readonly SemaphoreSlim localApplyGate;

    // Blocker groups of one helping pass resolve concurrently up to this degree: independent transactions, so
    // their materializations coalesce into the same proposals instead of serializing one durable round each.
    private const int MaxConcurrentBlockerGroups = 4;

    public DurableTransactionRecovery(
        PreparedIntentStore intentStore,
        DurableTransactionFinalizer.ReplicateDelegate replicate,
        LookupRecordDelegate lookupRecord,
        DriveAbortDelegate driveAbort,
        DurableTransactionFinalizer.ApplyCommitLocally? applyCommitLocally = null,
        TimeSpan? recordRetentionTtl = null,
        ILogger<IKahuna>? logger = null,
        Func<HLCTimestamp, long, bool>? locallyAborted = null,
        bool materializeByReference = false,
        int maxMaterializationBatchItems = 512,
        long maxMaterializationBatchBytes = 4 * 1024 * 1024,
        SemaphoreSlim? localApplyGate = null)
    {
        this.materializeByReference = materializeByReference;
        this.maxMaterializationBatchItems = Math.Max(1, maxMaterializationBatchItems);
        this.maxMaterializationBatchBytes = Math.Max(1, maxMaterializationBatchBytes);
        this.localApplyGate = localApplyGate ?? new SemaphoreSlim(DurableTransactionFinalizer.MaxConcurrentLocalApplies);
        this.intentStore = intentStore;
        this.replicate = replicate;
        this.lookupRecord = lookupRecord;
        this.driveAbort = driveAbort;
        this.applyCommitLocally = applyCommitLocally;
        this.locallyAborted = locallyAborted;
        this.logger = logger;

        // Default mirrors KahunaConfiguration.TransactionOutcomeRetentionTtl; a non-positive TTL means age-based
        // record GC is disabled, so absence can never mean reclaimed-after-commit and no hold is needed.
        recordRetentionMs = (long)(recordRetentionTtl ?? TimeSpan.FromMinutes(5)).TotalMilliseconds;
    }

    /// <summary>Resolves every eligible unresolved intent on <paramref name="partitionId"/> and returns how many
    /// intents were confirmed settled (their settle delta replicated). An intent whose materialization, local
    /// apply or settle did not land is not counted; it stays for the next sweep. Bounded by the current due set;
    /// safe to run repeatedly (idempotent).</summary>
    public async Task<int> SweepAsync(int partitionId, HLCTimestamp now, CancellationToken cancellationToken)
    {
        int resolved = 0;

        IEnumerable<IGrouping<(HLCTimestamp, long, string), PreparedIntent>> groups = intentStore
            .DueForRecovery(now, partitionId)
            .GroupBy(i => (i.TransactionId, i.Epoch, i.RecordAnchorKey));

        foreach (IGrouping<(HLCTimestamp, long, string), PreparedIntent> group in groups)
        {
            PreparedIntent representative = group.First();
            bool? commit = await DecideAsync(representative, now, cancellationToken).ConfigureAwait(false);
            if (commit is null)
                continue; // still within the decision window, or the abort drive did not land — retry next sweep.

            ResolveGroupResult result = await ResolveGroupAsync(partitionId, group, commit.Value, ResolutionSource.Recovery, cancellationToken).ConfigureAwait(false);
            resolved += result.Settled;
        }

        return resolved;
    }

    /// <summary>
    /// Targeted "helping" resolution for a blocked finalize: given the intents a transaction failed to prepare on
    /// <paramref name="partitionId"/>, finds the foreign intents currently holding those keys whose canonical
    /// record is already terminal — committed- or aborted-but-unsettled, i.e. only waiting on deferred settlement —
    /// and settles them now. Returns how many blocking intents were confirmed settled — their settle delta
    /// replicated — so the caller re-prepares immediately only on real progress and otherwise sleeps through its
    /// backoff; a pass whose materializations, local applies or settle did not land reports zero, never the
    /// size of the group it attempted.
    ///
    /// <para>A blocker whose record is still <c>Undecided</c> is never touched: helping must not presume-abort a
    /// live coordinator inside its decision window. That case stays with the caller's bounded retry (and, past the
    /// deadline, with the periodic recovery sweep, which owns the presumed-abort protocol). Settlement reuses the
    /// sweep's resolution path, so it is idempotent under races with the deferred-settlement task, the sweep, or
    /// another helper — whoever loses applies no-ops in Raft order.</para>
    /// </summary>
    public async Task<int> TryResolveDecidedBlockersAsync(
        int partitionId,
        IReadOnlyList<PreparedIntent> blockedIntents,
        HLCTimestamp requestingTransactionId,
        long requestingEpoch,
        CancellationToken cancellationToken)
    {
        // Group the live foreign holders of our keys by owning transaction, so one record lookup and one settle
        // delta covers every key a given blocker holds. The store read is leader-local and in-memory; when nothing
        // foreign holds our keys this whole pass costs no I/O.
        Dictionary<(HLCTimestamp TransactionId, long Epoch), List<PreparedIntent>>? byBlocker = null;

        foreach (PreparedIntent blocked in blockedIntents)
        {
            PreparedIntent? holder = intentStore.Get(blocked.Key);
            if (holder is null)
                continue;

            if (holder.TransactionId == requestingTransactionId && holder.Epoch == requestingEpoch)
                continue;

            byBlocker ??= [];
            (HLCTimestamp, long) identity = (holder.TransactionId, holder.Epoch);
            if (!byBlocker.TryGetValue(identity, out List<PreparedIntent>? group))
                byBlocker[identity] = group = [];

            // The same holder instance can appear once per blocked key it owns; Get returns the live intent per
            // key, so duplicates cannot arise for a single key and each entry here is a distinct blocked key.
            group.Add(holder);
        }

        if (byBlocker is null)
            return 0;

        // Independent blockers resolve concurrently, a few at a time: each is a distinct decided transaction, so
        // their materializations and settles coalesce into shared proposals instead of queueing one durable round
        // behind another. The degree is small and fixed so one helping pass cannot flood the scheduler.
        List<PreparedIntent>[] groups = [.. byBlocker.Values];
        int[] settledPerGroup = new int[groups.Length];

        await Parallel.ForEachAsync(
            Enumerable.Range(0, groups.Length),
            new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentBlockerGroups, CancellationToken = cancellationToken },
            async (index, ct) =>
            {
                settledPerGroup[index] = await HelpBlockerGroupAsync(partitionId, groups[index], ct).ConfigureAwait(false);
            }).ConfigureAwait(false);

        int settled = 0;
        foreach (int count in settledPerGroup)
            settled += count;

        if (settled > 0)
            DurableTransactionMetrics.PrepareConflictBlockersSettled.Add(settled);

        return settled;
    }

    /// <summary>Settles one blocker's intents when its record is terminal; returns the confirmed count. An
    /// undecided blocker (or one with no record yet) is a live conflict, not settlement lag, and is left alone.</summary>
    private async Task<int> HelpBlockerGroupAsync(int partitionId, List<PreparedIntent> group, CancellationToken cancellationToken)
    {
        PreparedIntent representative = group[0];
        TransactionRecord? record = await lookupRecord(
            representative.TransactionId, representative.Epoch, representative.RecordAnchorKey, cancellationToken).ConfigureAwait(false);

        bool? commit = record?.Decision switch
        {
            TransactionDecision.Commit => true,
            TransactionDecision.Abort => false,
            _ => null,
        };

        if (commit is null)
            return 0;

        ResolveGroupResult result = await ResolveGroupAsync(partitionId, group, commit.Value, ResolutionSource.Helping, cancellationToken).ConfigureAwait(false);

        // A decided blocker the helper could not settle at all: the finalize backs off for this round, and the
        // cause says whether the blocker's partition is refusing materializations, applies or settles.
        if (result.Settled == 0)
            DurableTransactionMetrics.HelpingSettledNone(result.Cause);

        return result.Settled;
    }

    /// <summary>
    /// Settles a supplied set of prepared intents — the pre-cutover barrier of a range split/merge, run over
    /// the intents gathered from the moving range's partition leader. Each intent whose canonical decision is
    /// terminal (or whose own resolution already is) is resolved through the sweep's idempotent path:
    /// materialize + leader apply + settle for a commit, clear + settle for an abort. An intent that is still
    /// undecided <b>inside</b> its decision window is left alone — a live coordinator must not be
    /// presumed-aborted by a data move — and counts as unsettled; one undecided <b>past</b> its recovery
    /// deadline is driven through the ordinary presumed-abort protocol, exactly as the periodic sweep would.
    /// Returns how many intents could not be settled — undecided ones, and decided ones whose materialization,
    /// local apply or settle delta did not land: zero means the range carries no unsettled durable state and the
    /// caller may proceed to copy and cut over.
    /// </summary>
    public async Task<int> SettleSuppliedIntentsAsync(
        int partitionId, IReadOnlyList<PreparedIntent> intents, HLCTimestamp now, CancellationToken cancellationToken)
    {
        int unsettled = 0;

        foreach (IGrouping<(HLCTimestamp, long, string), PreparedIntent> group in
            intents.GroupBy(i => (i.TransactionId, i.Epoch, i.RecordAnchorKey)))
        {
            PreparedIntent representative = group.First();

            // An intent already resolved (deferred removal lag) settles by its own terminal resolution; a
            // pending one defers to the canonical record — and, past its recovery deadline, to the
            // presumed-abort drive DecideAsync owns.
            bool? commit;
            switch (representative.Resolution)
            {
                case PreparedIntentResolution.Committed:
                    commit = true;
                    break;

                case PreparedIntentResolution.Aborted:
                    commit = false;
                    break;

                default:
                {
                    TransactionRecord? record = await lookupRecord(
                        representative.TransactionId, representative.Epoch, representative.RecordAnchorKey, cancellationToken).ConfigureAwait(false);

                    commit = record?.Decision switch
                    {
                        TransactionDecision.Commit => true,
                        TransactionDecision.Abort => false,
                        // Undecided or not-yet-initialized: inside the intent's recovery window the
                        // coordinator may still be deciding (a non-anchor prepare can even be durable
                        // before the record init commits) — never presume-abort it for a data move.
                        // Past the window, DecideAsync drives the ordinary presumed-abort protocol.
                        _ => representative.RecoveryDeadline != HLCTimestamp.Zero && representative.RecoveryDeadline <= now
                            ? await DecideAsync(representative, now, cancellationToken).ConfigureAwait(false)
                            : null
                    };
                    break;
                }
            }

            int groupSize = group.Count();
            if (commit is null)
            {
                unsettled += groupSize;
                continue;
            }

            // A decided intent whose settle did not land is still unsettled durable state on the moving range;
            // reporting it settled would let the cutover copy a range that still carries it.
            ResolveGroupResult result = await ResolveGroupAsync(partitionId, group, commit.Value, ResolutionSource.RangeMove, cancellationToken).ConfigureAwait(false);
            unsettled += groupSize - result.Settled;
        }

        return unsettled;
    }

    private async Task<bool?> DecideAsync(PreparedIntent intent, HLCTimestamp now, CancellationToken cancellationToken)
    {
        TransactionRecord? record = await lookupRecord(intent.TransactionId, intent.Epoch, intent.RecordAnchorKey, cancellationToken).ConfigureAwait(false);

        switch (record?.Decision)
        {
            case TransactionDecision.Commit:
                return true;

            case TransactionDecision.Abort:
                return false;

            case TransactionDecision.Undecided when now <= record.DecisionDeadline:
                // The coordinator may still be finalizing; do not presume-abort inside the window.
                return null;
        }

        // No record at all, and the intent is old enough that the retention GC could already have reclaimed a
        // TERMINAL record for this transaction: absence no longer distinguishes "never initialized" from
        // "committed, then aged out while this leg's settlement kept failing". Presuming abort on the latter
        // discards the only durable copy of a committed value (a silent lost write), so the intent is held for
        // a later pass — and surfaced loudly, because it can no longer resolve without the record. A genuine
        // failed-init orphan cannot ordinarily reach this age: the sweep aborts it within its decision
        // deadline, far inside the retention window.
        if (record is null && recordRetentionMs > 0 && now.L - intent.CommitTimestamp.L > recordRetentionMs)
        {
            DurableTransactionMetrics.RecordlessIntentHolds.Add(1);

            logger?.LogError(
                "Prepared intent for key {Key} of transaction {TransactionId} has no canonical record and is older than the record retention horizon; holding it instead of presuming abort — the record may have been a reclaimed commit",
                intent.Key, intent.TransactionId);

            return null;
        }

        // Undecided past its deadline, or no record at all (orphan prepare): drive a presumed abort and take the
        // outcome that actually won at the canonical record — never assume the abort won.
        bool deadlineExpiry = record is { Decision: TransactionDecision.Undecided };

        AbortTransactionCommand abort = new(
            intent.TransactionId, intent.Epoch, intent.ManifestHash, TransactionAbortClass.PresumedAbort,
            OpId: now, AttemptHlc: now,
            intent.RecordAnchorKey,
            CommitTimestamp: record?.CommitTimestamp ?? intent.CommitTimestamp,
            DecisionDeadline: record?.DecisionDeadline ?? intent.RecoveryDeadline,
            CreatedAt: record?.CreatedAt ?? now);

        TransactionRecord? after = await driveAbort(abort, intent.RecordAnchorKey, cancellationToken).ConfigureAwait(false);

        // Count only aborts that actually won for a record left Undecided past its deadline — the signal that the
        // deadline expired before a healthy coordinator could decide. Orphan-prepare aborts (no record) are a
        // different cause and excluded; a concurrent commit that won the race is not a deadline-expiry abort.
        if (deadlineExpiry && after is { Decision: TransactionDecision.Abort })
            DurableTransactionMetrics.DeadlineExpiryAborts.Add(1);

        return after?.Decision switch
        {
            TransactionDecision.Commit => true,
            TransactionDecision.Abort => false,
            _ => null
        };
    }

    /// <summary>
    /// Resolves one transaction's intents on a partition to its terminal decision and reports what was
    /// <b>confirmed</b> settled: the number of intents whose settle delta replicated, and — when that number is
    /// zero — the step that stopped the pass. A caller must never infer progress from the size of the group it
    /// handed in: materialization can be refused, the leader-local apply can fail to confirm, and the settle
    /// delta itself can be rejected by the scheduler, each leaving every intent exactly where it was.
    /// </summary>
    private async Task<ResolveGroupResult> ResolveGroupAsync(int partitionId, IEnumerable<PreparedIntent> intents, bool commit, ResolutionSource source, CancellationToken cancellationToken)
    {
        List<PreparedIntent> group = intents.ToList();

        // Only settle (resolve + remove) an intent whose terminal effect is durably applied. On commit that means
        // its value materialized: removing an intent whose materialization did not commit would delete the only
        // durable copy of an already-committed value, so a false/thrown materialization leaves the intent for a
        // later sweep. On abort there is no value to lose, so every intent is settled.
        List<PreparedIntent> settleable;

        if (commit)
        {
            // Abort fence, re-checked at the last moment before any value reaches the log: the commit
            // direction was read moments ago, but a locally visible terminal Abort is definitive and a
            // materialization proposed past it would durably apply an aborted transaction's leg on every
            // replica. Leave the whole group unsettled; the next sweep re-reads the canonical record.
            PreparedIntent fenceProbe = group[0];
            if (locallyAborted is not null && locallyAborted(fenceProbe.TransactionId, fenceProbe.Epoch))
            {
                DurableTransactionMetrics.AbortFencedCommitApplies.Add(group.Count);
                logger?.LogError(
                    "Refusing commit-direction settle for transaction {TransactionId} epoch {Epoch} ({Count} intents): a terminal Abort is locally visible",
                    fenceProbe.TransactionId, fenceProbe.Epoch, group.Count);
                return new ResolveGroupResult(0, ResolveFailureCause.Fenced);
            }

            // Materialize the whole group in scheduler-sized windows (every record of a window submitted before
            // the window is awaited), exactly as the finalizer's own resolution does, so a blocked successor
            // waits one coalesced round for a predecessor's keys instead of one durable round per key.
            bool[] materialized = await DurableMaterializationWindow.MaterializeAsync(
                partitionId, group, materializeByReference, maxMaterializationBatchItems, maxMaterializationBatchBytes,
                replicate, cancellationToken).ConfigureAwait(false);

            // Replication makes the value durable and converges followers, but the leader applies a key/value
            // materialization to its own in-memory KV state through its dedicated apply path, not the generic
            // commit apply — so without this the recovered value is durable in the log yet invisible on the
            // recovering leader until a restart replays it. Mirror the finalizer's resolution: apply the
            // committed value locally before settling, under the node's shared bound. If the local apply does not
            // confirm (e.g. leadership lost mid-sweep), leave the intent for a later sweep rather than settling an
            // unapplied commit.
            bool[] applied = applyCommitLocally is null
                ? materialized
                : await DurableTransactionFinalizer.ApplyLocallyAsync(
                    partitionId, group, materialized, (p, intent) => applyCommitLocally(p, intent), localApplyGate, cancellationToken).ConfigureAwait(false);

            settleable = new(group.Count);
            bool anyMaterializeFailed = false;
            bool anyApplyFailed = false;

            for (int i = 0; i < group.Count; i++)
            {
                if (!materialized[i])
                    anyMaterializeFailed = true;
                else if (!applied[i])
                    anyApplyFailed = true;
                else
                    settleable.Add(group[i]);
            }

            DurableTransactionMetrics.Materialized(source, settleable.Count);

            if (settleable.Count == 0)
                return new ResolveGroupResult(0, anyMaterializeFailed ? ResolveFailureCause.MaterializeFailed : anyApplyFailed ? ResolveFailureCause.ApplyFailed : ResolveFailureCause.None);
        }
        else
        {
            settleable = group;
        }

        if (settleable.Count == 0)
            return new ResolveGroupResult(0, ResolveFailureCause.None);

        // Resolve and remove each intent atomically (Pending -> resolved -> deleted), so recovery leaves no
        // lingering resolved intent. Idempotent under replay.
        List<PreparedIntentCommand> settle = new(settleable.Count * 2);
        foreach (PreparedIntent intent in settleable)
        {
            settle.Add(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, commit));
            settle.Add(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key));
        }

        // The replicate seam is the single ordered apply owner: on the partition leader it applies this settle
        // delta through the scheduler's Raft-ordered completion, in the same order as any concurrent finalizer
        // decision for the same record — so recovery and the live coordinator can never apply out of log order.
        // A settle that does not replicate settles nothing: the materialized values are durable and idempotent to
        // re-materialize, and every intent stays for the next pass.
        byte[] resolveDelta = PreparedIntentStore.SerializeDelta(settle);
        bool settled;
        try
        {
            settled = await replicate(partitionId, ReplicationTypes.PreparedIntent, resolveDelta, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            settled = false;
        }

        if (!settled)
            return new ResolveGroupResult(0, ResolveFailureCause.SettleFailed);

        DurableTransactionMetrics.Settled(source, settleable.Count);
        return new ResolveGroupResult(settleable.Count, ResolveFailureCause.None);
    }
}

/// <summary>What one resolution pass over a transaction's intents confirmed: <paramref name="Settled"/> is the
/// number of intents whose settle delta replicated; <paramref name="Cause"/> names the step that stopped the pass
/// when nothing was settled, and is <see cref="ResolveFailureCause.None"/> otherwise.</summary>
internal readonly record struct ResolveGroupResult(int Settled, ResolveFailureCause Cause);
