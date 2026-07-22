using System.Diagnostics;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The per-partition prepared-intent group of one transaction. The anchor partition additionally carries
/// the canonical record's initialization (see <see cref="DurableFinalizeInput.AnchorPartitionId"/>). The
/// <paramref name="Generation"/> is the range-descriptor generation this partition was resolved against at freeze,
/// used to re-fence the prepare submission at dispatch so a split between freeze and dispatch releases it retryably
/// instead of appending to a retired partition.</summary>
internal sealed record DurablePartitionPrepare(int PartitionId, long Generation, IReadOnlyList<PreparedIntent> Intents);

/// <summary>The frozen, immutable inputs of one finalize attempt: identity, the canonical commit timestamp and
/// decision deadline, the participant manifest, and the per-partition prepared intents. Freezing happens in the
/// coordinator before finalize; this type is what the finalizer drives to a durable outcome.</summary>
internal sealed record DurableFinalizeInput(
    HLCTimestamp TransactionId,
    long Epoch,
    string CoordinatorKey,
    string RecordAnchorKey,
    int AnchorPartitionId,
    long AnchorGeneration,
    HLCTimestamp CommitTimestamp,
    HLCTimestamp DecisionDeadline,
    long ManifestHash,
    IReadOnlyList<TransactionParticipantRef> Manifest,
    IReadOnlyList<DurablePartitionPrepare> Partitions,
    HLCTimestamp CreatedAt);

internal enum DurableFinalizeResult
{
    Committed,
    Aborted,
    MustRetry
}

/// <summary>The outcome of a finalize attempt, already mapped to the MustRetry/Aborted result contract: only a
/// conflict-class abort is <see cref="DurableFinalizeResult.Aborted"/>; every other abort and every
/// infrastructural failure is <see cref="DurableFinalizeResult.MustRetry"/>.</summary>
internal readonly record struct DurableFinalizeOutcome(DurableFinalizeResult Result, TransactionAbortClass AbortClass);

/// <summary>
/// Drives one transaction's finalize under the durable-intent 2PC model: initialize the canonical
/// record, prepare every participant's durable intent (prepare barrier), validate the read-set, decide the
/// canonical outcome by compare-and-set, then resolve each intent. It owns the protocol sequencing and outcome
/// mapping, behind a replicate seam so the same logic runs against real Raft in production and synchronously in
/// tests. It never truncates a log: abort is a canonical decision, not a rollback.
/// </summary>
internal sealed class DurableTransactionFinalizer : IDisposable
{
    // Local apply is actor work, not Raft I/O. This gate is shared by every finalize using this finalizer, so
    // concurrent transactions cannot multiply their individual fan-out into an unbounded actor-inbox flood.
    private const int MaxConcurrentLocalApplies = 32;

    private readonly SemaphoreSlim localApplyGate = new(MaxConcurrentLocalApplies);

    /// <summary>Replicates a partition's serialized delta of the given log type and returns whether it committed
    /// durably. In production this is an auto-commit Raft round trip; the finalizer applies the delta to the
    /// local store on success (idempotent with the replication-callback apply on every replica). The
    /// <paramref name="admissionClass"/> tells the shared write scheduler whether this is ordinary work
    /// (record init, prepare) or terminal work that finishes an already-prepared transaction (decision, settle,
    /// materialize) — terminal work draws on reserve capacity so an ordinary-write burst cannot starve it.</summary>
    public delegate Task<bool> ReplicateDelegate(int partitionId, string logType, byte[] logData, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken);

    /// <summary>Like <see cref="ReplicateDelegate"/> but re-fences the submission at dispatch against the range
    /// descriptor <paramref name="fenceKey"/> was resolved to at freeze (<paramref name="fenceGeneration"/>): a
    /// split/merge between freeze and dispatch releases it retryably instead of appending to a retired partition.
    /// Used for the pre-decision record initialization, prepare, and decision; null falls back to the unfenced
    /// replicate (protocol tests, and the post-decision settle/materialize which recovery backstops).</summary>
    public delegate Task<bool> ReplicateFencedDelegate(int partitionId, string logType, byte[] logData, string fenceKey, long fenceGeneration, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken);

    /// <summary>Runs the post-decision resolution (materialize committed values, settle intents) on a background
    /// task, off the commit critical path (deferred settlement). When <see langword="null"/>, resolution is instead
    /// awaited inline so it completes before <see cref="FinalizeAsync"/> returns (synchronous settlement). Either
    /// way the canonical decision is already durable, so recovery finishes resolution if a deferred run is lost.</summary>
    public delegate void ResolutionScheduler(Func<CancellationToken, Task> resolution);

    /// <summary>Applies a committed intent's value on the leader's live KV state (clears the committing
    /// transaction's staged write intent + MVCC snapshot and applies the value to the base entry). The replicated
    /// key/value record makes followers converge, but the leader does not apply it through the replication callback,
    /// so this is how the committed value becomes visible on the leader. Null in bare protocol tests (no actor).</summary>
    public delegate Task<bool> ApplyCommitLocally(int partitionId, PreparedIntent intent);

    /// <summary>Clears an aborted transaction's staged write intent + MVCC snapshot on the owning actor (the durable
    /// analog of ApplyConfirmedRollback), so the key is not blocked until the intent lease expires. Null in bare
    /// protocol tests (no actor).</summary>
    public delegate Task<bool> ApplyRollbackLocally(int partitionId, PreparedIntent intent);

    private readonly TransactionRecordStore recordStore;

    private readonly ReplicateDelegate replicate;

    // Fenced replicate for the pre-decision path; null falls back to the unfenced replicate.
    private readonly ReplicateFencedDelegate? replicateFenced;

    // Null = synchronous settlement: resolution is awaited inline in FinalizeAsync. Non-null = deferred settlement.
    private readonly ResolutionScheduler? scheduleResolution;

    private readonly ApplyCommitLocally? applyCommitLocally;

    private readonly ApplyRollbackLocally? applyRollbackLocally;

    // Mints a fresh HLC immediately before the terminal transition, used as the attempt's AttemptHlc so elapsed
    // prepare/validate time can actually trip the frozen decision deadline. Null keeps the deprecated behaviour of
    // reusing the operation id as the attempt HLC — used only by protocol tests that supply an explicit late opId.
    private readonly Func<HLCTimestamp>? attemptClock;

    // Records the finalize latency measured up to (and including) the canonical decision — deliberately excluding
    // post-decision resolution/settlement — so the p99 that sizes future decision deadlines is not inflated by
    // work that happens after the decision. Null in tests that do not observe latency.
    private readonly Action<double>? recordDecisionLatencyMs;

    private readonly int maxMaterializationBatchItems;

    private readonly long maxMaterializationBatchBytes;

    public DurableTransactionFinalizer(
        TransactionRecordStore recordStore,
        // The prepared-intent store is applied by the ordered scheduler-completion path (the single apply owner),
        // not by the finalizer, so this parameter is retained only for call-site compatibility.
        PreparedIntentStore intentStore,
        ReplicateDelegate replicate,
        ResolutionScheduler? resolutionScheduler = null,
        ApplyCommitLocally? applyCommitLocally = null,
        ApplyRollbackLocally? applyRollbackLocally = null,
        Func<HLCTimestamp>? attemptClock = null,
        Action<double>? recordDecisionLatencyMs = null,
        ReplicateFencedDelegate? replicateFenced = null,
        int maxMaterializationBatchItems = 512,
        long maxMaterializationBatchBytes = 4 * 1024 * 1024)
    {
        this.recordStore = recordStore;
        this.replicate = replicate;
        this.replicateFenced = replicateFenced;
        this.applyCommitLocally = applyCommitLocally;
        this.applyRollbackLocally = applyRollbackLocally;
        // A null scheduler means synchronous settlement (FinalizeAsync awaits resolution inline).
        this.scheduleResolution = resolutionScheduler;
        this.attemptClock = attemptClock;
        this.recordDecisionLatencyMs = recordDecisionLatencyMs;
        this.maxMaterializationBatchItems = Math.Max(1, maxMaterializationBatchItems);
        this.maxMaterializationBatchBytes = Math.Max(1, maxMaterializationBatchBytes);
    }

    /// <param name="validateReadSet">Runs the optimistic read-set conflict check after every prepare is durable;
    /// true means no conflict. Only invoked when every prepare committed.</param>
    /// <param name="opId">This attempt's unique operation id, also used as the transition's attempt HLC (for the
    /// deadline check and the recorded winner). Must be less than or equal to the frozen decision deadline for a
    /// commit to be authorized.</param>
    public async Task<DurableFinalizeOutcome> FinalizeAsync(
        DurableFinalizeInput input,
        Func<CancellationToken, Task<bool>> validateReadSet,
        HLCTimestamp opId,
        CancellationToken cancellationToken)
    {
        long startTicks = Stopwatch.GetTimestamp();

        // ── Initialize the canonical record (Undecided) on the anchor partition ──
        // Nothing is durable yet if this fails, so it is a clean retry.
        byte[] initDelta = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(
            input.TransactionId, input.Epoch, input.CoordinatorKey, input.RecordAnchorKey,
            input.CommitTimestamp, input.DecisionDeadline, input.ManifestHash, input.Manifest, opId, input.CreatedAt)]);

        if (!await ReplicateRecordAsync(input.AnchorPartitionId, initDelta, input.RecordAnchorKey, input.AnchorGeneration, Writes.WriteAdmissionClass.Ordinary, cancellationToken).ConfigureAwait(false))
            return Retry();

        // ── Prepare barrier: prepare every partition, waiting for all (never abandon a submission on the first
        // failure — its outcome is needed to drive a truthful abort). A prepared-then-aborted intent is cleaned
        // up by resolution/recovery; a failed prepare forces the transaction to abort. ──
        Task<bool>[] prepareTasks = input.Partitions.Select(partition =>
        {
            byte[] prepareDelta = PreparedIntentStore.SerializeDelta(partition.Intents.Select(i => (PreparedIntentCommand)new PrepareIntentCommand(i)));
            return ReplicatePrepareAsync(partition.PartitionId, prepareDelta, partition.Intents[0].Key, partition.Generation, cancellationToken);
        }).ToArray();
        bool[] prepareResults = await Task.WhenAll(prepareTasks).ConfigureAwait(false);
        bool allPrepared = prepareResults.All(static prepared => prepared);

        // ── Post-prepare validation, only meaningful when everything is durable ──
        bool validated = allPrepared && await validateReadSet(cancellationToken).ConfigureAwait(false);

        // ── Decision barrier: a commit only when every prepare is durable and validation passed; otherwise a
        // conflict abort (validation failed) or a retryable abort (a prepare did not commit). ──
        DurableFinalizeOutcome outcome;
        if (allPrepared && validated)
            outcome = await DecideAsync(input, commit: true, TransactionAbortClass.None, opId, cancellationToken).ConfigureAwait(false);
        else
        {
            TransactionAbortClass abortClass = !allPrepared ? TransactionAbortClass.RetryableFailure : TransactionAbortClass.Conflict;
            outcome = await DecideAsync(input, commit: false, abortClass, opId, cancellationToken).ConfigureAwait(false);
        }

        // Record the latency up to the decision only — resolution below is excluded so it never inflates the
        // deadline window that a future finalize's prepare must fit inside.
        recordDecisionLatencyMs?.Invoke(Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds);

        // ── Resolution: apply the terminal decision to every prepared intent — on commit, materialize each intent
        // into visible KV state, then settle (resolve + remove) the intent. With synchronous settlement (no
        // scheduler) it is awaited here so the committed value is materialized before the caller returns — required
        // for correct cross-node read-your-writes until the cross-node anchor decision lookup exists. A scheduler
        // runs it in the background (deferred settlement); the decision is already durable and recovery finishes any
        // lost run.
        //
        // Resolution is best-effort and MUST NOT change the returned outcome: the canonical decision is already
        // durable, so an exception here (a materialization/apply/settle failure after the commit) is swallowed and
        // left to the recovery sweep — never allowed to escape and be reported to the caller as a conflict abort. ──
        if (scheduleResolution is null)
        {
            try
            {
                await ResolveAsync(input, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // The decision is durable; recovery completes the resolution. Do not let it turn a commit into an abort.
            }
        }
        else
        {
            scheduleResolution(ct => ResolveAsync(input, ct));
        }

        return outcome;
    }

    private async Task<DurableFinalizeOutcome> DecideAsync(
        DurableFinalizeInput input, bool commit, TransactionAbortClass abortClass, HLCTimestamp opId, CancellationToken cancellationToken)
    {
        // Mint a fresh attempt HLC immediately before the transition so a slow prepare/validate can push the
        // attempt past the frozen decision deadline (the gate that yields a late commit to presumed-abort
        // recovery). The operation id stays stable for idempotency; only the attempt HLC advances.
        HLCTimestamp attemptHlc = attemptClock?.Invoke() ?? opId;

        TransactionRecordCommand decision = commit
            ? new CommitTransactionCommand(input.TransactionId, input.Epoch, input.ManifestHash, opId, attemptHlc)
            : new AbortTransactionCommand(input.TransactionId, input.Epoch, input.ManifestHash, abortClass, opId, attemptHlc,
                input.RecordAnchorKey, input.CommitTimestamp, input.DecisionDeadline, input.CreatedAt);

        byte[] delta = TransactionRecordStore.SerializeDelta([decision]);

        // The decision is terminal work finishing an already-prepared transaction: admit it as Terminal so an
        // ordinary-write burst saturating the anchor partition can never reject it.
        if (!await ReplicateRecordAsync(input.AnchorPartitionId, delta, input.RecordAnchorKey, input.AnchorGeneration, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false))
            return Retry();

        // The winner is whatever the canonical record actually reflects after apply, not what we requested — a
        // concurrent recovery abort may have won the race in the log.
        TransactionRecord? record = recordStore.Get(input.TransactionId, input.Epoch);
        if (record is null)
            return Retry();

        // A commit we requested that left the record Undecided was rejected by the state machine's deadline gate
        // (the only transition that keeps an initialized record Undecided): the attempt's HLC passed the frozen
        // decision deadline, so the transaction yields to presumed-abort recovery. Surface it — a rising rate means
        // the deadline is too tight for the current finalize latency and healthy commits are being aborted.
        if (commit && record.Decision == TransactionDecision.Undecided)
            DurableTransactionMetrics.LateCommitRejections.Add(1);

        return record.Decision switch
        {
            TransactionDecision.Commit => new DurableFinalizeOutcome(DurableFinalizeResult.Committed, TransactionAbortClass.None),
            TransactionDecision.Abort when record.AbortClass == TransactionAbortClass.Conflict
                => new DurableFinalizeOutcome(DurableFinalizeResult.Aborted, TransactionAbortClass.Conflict),
            TransactionDecision.Abort
                => new DurableFinalizeOutcome(DurableFinalizeResult.MustRetry, record.AbortClass),
            _ => Retry()
        };
    }

    private async Task ResolveAsync(DurableFinalizeInput input, CancellationToken cancellationToken)
    {
        TransactionRecord? record = recordStore.Get(input.TransactionId, input.Epoch);
        if (record is null || !record.IsTerminal)
            return;

        bool commit = record.Decision == TransactionDecision.Commit;

        await Task.WhenAll(input.Partitions.Select(partition => ResolvePartitionAsync(partition, commit, localApplyGate, cancellationToken))).ConfigureAwait(false);
    }

    private async Task ResolvePartitionAsync(
        DurablePartitionPrepare partition,
        bool commit,
        SemaphoreSlim localApplyGate,
        CancellationToken cancellationToken)
    {
        // Only an intent whose terminal effect is durably applied may be settled (resolved + removed). On
        // commit that means its value is materialized: settling an intent whose materialization did not commit
        // would delete the only durable copy of an already-committed value, so a false/thrown materialization
        // leaves the intent for the recovery sweep to retry. On abort the actor must still positively clear
        // staged state before settlement, otherwise the intent remains for recovery.
        List<PreparedIntent> settleable;
        if (commit)
        {
            // Fill scheduler-sized windows before awaiting them. Each window can coalesce into a capped proposal,
            // while a transaction larger than the scheduler's admission capacity advances without allocating or
            // admitting its whole working set at once.
            bool[] materialized = await MaterializePartitionAsync(partition, cancellationToken).ConfigureAwait(false);

            if (applyCommitLocally is not null)
                materialized = await ApplyLocallyAsync(
                    partition.PartitionId,
                    partition.Intents,
                    materialized,
                    (partitionId, intent) => applyCommitLocally(partitionId, intent),
                    localApplyGate,
                    cancellationToken).ConfigureAwait(false);

            settleable = partition.Intents.Where((_, index) => materialized[index]).ToList();
        }
        else
        {
            bool[] rolledBack = applyRollbackLocally is null
                ? Enumerable.Repeat(true, partition.Intents.Count).ToArray()
                : await ApplyLocallyAsync(
                    partition.PartitionId,
                    partition.Intents,
                    Enumerable.Repeat(true, partition.Intents.Count).ToArray(),
                    (partitionId, intent) => applyRollbackLocally(partitionId, intent),
                    localApplyGate,
                    cancellationToken).ConfigureAwait(false);

            settleable = partition.Intents.Where((_, index) => rolledBack[index]).ToList();
        }

        if (settleable.Count > 0)
            await SettleIntentsAsync(partition.PartitionId, settleable, commit, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool[]> MaterializePartitionAsync(DurablePartitionPrepare partition, CancellationToken cancellationToken)
    {
        bool[] materialized = new bool[partition.Intents.Count];
        int next = 0;

        while (next < partition.Intents.Count)
        {
            List<(int Index, byte[] Record)> window = new(Math.Min(maxMaterializationBatchItems, partition.Intents.Count - next));
            long windowBytes = 0;

            while (next < partition.Intents.Count && window.Count < maxMaterializationBatchItems)
            {
                byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(partition.Intents[next]);
                if (window.Count > 0 && windowBytes + record.Length > maxMaterializationBatchBytes)
                    break;

                window.Add((next, record));
                windowBytes += record.Length;
                next++;

                if (windowBytes >= maxMaterializationBatchBytes)
                    break;
            }

            Task<bool>[] tasks = window
                .Select(item => MaterializeAsync(partition.PartitionId, item.Record, cancellationToken))
                .ToArray();
            bool[] results = await Task.WhenAll(tasks).ConfigureAwait(false);
            for (int i = 0; i < results.Length; i++)
                materialized[window[i].Index] = results[i];
        }

        return materialized;
    }

    private async Task<bool> MaterializeAsync(int partitionId, byte[] kvRecord, CancellationToken cancellationToken)
    {
        try
        {
            // Replicate the committed value as an ordinary key/value record so followers converge. The leader
            // applies it separately through its owning actor after the durable record is acknowledged. This is
            // post-decision materialization — terminal work — so it draws on reserve capacity and is never
            // starved by an ordinary-write burst.
            return await replicate(partitionId, ReplicationTypes.KeyValues, kvRecord, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            return false;
        }
    }

    private static async Task<bool[]> ApplyLocallyAsync(
        int partitionId,
        IReadOnlyList<PreparedIntent> intents,
        IReadOnlyList<bool> durableEffects,
        Func<int, PreparedIntent, Task<bool>> apply,
        SemaphoreSlim gate,
        CancellationToken cancellationToken)
    {
        bool[] applied = new bool[intents.Count];

        await Parallel.ForEachAsync(
            Enumerable.Range(0, intents.Count),
            new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentLocalApplies },
            async (index, _) =>
            {
                if (!durableEffects[index])
                    return;

                try
                {
                    await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        applied[index] = await apply(partitionId, intents[index]).ConfigureAwait(false);
                    }
                    finally
                    {
                        gate.Release();
                    }
                }
                catch
                {
                    applied[index] = false;
                }
            }).ConfigureAwait(false);

        return applied;
    }

    public void Dispose() => localApplyGate.Dispose();

    // Resolves and removes each intent in one atomic delta (applied in order Pending -> resolved -> deleted), so
    // no "resolved-but-not-removed" state can linger to block a later write to the key or serve a stale value.
    // Idempotent: a replay of [Resolve, Remove] over an already-removed intent is a pair of no-ops.
    private async Task SettleIntentsAsync(int partitionId, IReadOnlyList<PreparedIntent> intents, bool commit, CancellationToken cancellationToken)
    {
        List<PreparedIntentCommand> settle = new(intents.Count * 2);
        foreach (PreparedIntent intent in intents)
        {
            settle.Add(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, commit));
            settle.Add(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key));
        }

        byte[] delta = PreparedIntentStore.SerializeDelta(settle);
        await ReplicateIntentsAsync(partitionId, delta, cancellationToken).ConfigureAwait(false);
    }

    // Both replicate helpers return whatever the replicate seam reports. The seam is the single apply owner: on a
    // leader it applies the delta through the scheduler's Raft-ordered completion; forwarded to a remote leader it
    // returns that leader's applied outcome. For a prepared-intent delta the returned boolean already folds in
    // prepare acknowledgement, so a rejected prepare (another transaction owns the key) surfaces here as a failed
    // replicate and drives an abort rather than a commit of a mutation recovery could not complete.
    // Pre-decision record (initialize / decision): fenced against the anchor's frozen descriptor so a split
    // between freeze and dispatch releases it retryably instead of landing on a retired partition.
    // The record initialize is ordinary work; the decision is terminal work that finishes an already-prepared
    // transaction. The caller passes the class so the decision draws on reserve capacity and can never be
    // rejected by an ordinary-write burst on the anchor partition.
    private Task<bool> ReplicateRecordAsync(int partitionId, byte[] delta, string fenceKey, long fenceGeneration, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken) =>
        replicateFenced is not null
            ? replicateFenced(partitionId, ReplicationTypes.TransactionRecord, delta, fenceKey, fenceGeneration, admissionClass, cancellationToken)
            : replicate(partitionId, ReplicationTypes.TransactionRecord, delta, admissionClass, cancellationToken);

    // Pre-decision prepare: fenced against the partition group's frozen descriptor. A rejected prepare surfaces as
    // a failed replicate (the seam folds in prepare acknowledgement) and drives an abort. Ordinary work.
    private Task<bool> ReplicatePrepareAsync(int partitionId, byte[] delta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken) =>
        replicateFenced is not null
            ? replicateFenced(partitionId, ReplicationTypes.PreparedIntent, delta, fenceKey, fenceGeneration, Writes.WriteAdmissionClass.Ordinary, cancellationToken)
            : replicate(partitionId, ReplicationTypes.PreparedIntent, delta, Writes.WriteAdmissionClass.Ordinary, cancellationToken);

    // Post-decision settle: unfenced and terminal. The decision is already durable; a split at this point is
    // resolved by the recovery sweep, and re-fencing would only strand the settle.
    private Task<bool> ReplicateIntentsAsync(int partitionId, byte[] delta, CancellationToken cancellationToken) =>
        replicate(partitionId, ReplicationTypes.PreparedIntent, delta, Writes.WriteAdmissionClass.Terminal, cancellationToken);

    private static DurableFinalizeOutcome Retry() => new(DurableFinalizeResult.MustRetry, TransactionAbortClass.RetryableFailure);
}
