using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The per-partition prepared-intent group of one transaction. The anchor partition additionally carries
/// the canonical record's initialization (see <see cref="DurableFinalizeInput.AnchorPartitionId"/>).</summary>
internal sealed record DurablePartitionPrepare(int PartitionId, IReadOnlyList<PreparedIntent> Intents);

/// <summary>The frozen, immutable inputs of one finalize attempt: identity, the canonical commit timestamp and
/// decision deadline, the participant manifest, and the per-partition prepared intents. Freezing happens in the
/// coordinator before finalize; this type is what the finalizer drives to a durable outcome.</summary>
internal sealed record DurableFinalizeInput(
    HLCTimestamp TransactionId,
    long Epoch,
    string CoordinatorKey,
    string RecordAnchorKey,
    int AnchorPartitionId,
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
/// canonical outcome by compare-and-set, then resolve each intent. This slice does one Raft request per
/// operation (cross-transaction batching is a later slice); it owns the protocol sequencing and the outcome
/// mapping, behind a replicate seam so the same logic runs against real Raft in production and synchronously in
/// tests. It never truncates a log: abort is a canonical decision, not a rollback.
/// </summary>
internal sealed class DurableTransactionFinalizer
{
    /// <summary>Replicates a partition's serialized delta of the given log type and returns whether it committed
    /// durably. In production this is an auto-commit Raft round trip; the finalizer applies the delta to the
    /// local store on success (idempotent with the replication-callback apply on every replica).</summary>
    public delegate Task<bool> ReplicateDelegate(int partitionId, string logType, byte[] logData, CancellationToken cancellationToken);

    /// <summary>Runs the post-decision resolution (materialize committed values, settle intents). The default
    /// runs it inline so it completes before <see cref="FinalizeAsync"/> returns; production injects a scheduler
    /// that runs it on a background task, off the commit critical path (deferred settlement). Either way the
    /// canonical decision is already durable, so recovery finishes resolution if a deferred run is lost.</summary>
    public delegate void ResolutionScheduler(Func<CancellationToken, Task> resolution);

    /// <summary>Applies a committed intent's value on the leader's live KV state (clears the committing
    /// transaction's staged write intent + MVCC snapshot and applies the value to the base entry). The replicated
    /// key/value record makes followers converge, but the leader does not apply it through the replication callback,
    /// so this is how the committed value becomes visible on the leader. Null in bare protocol tests (no actor).</summary>
    public delegate Task ApplyCommitLocally(int partitionId, PreparedIntent intent);

    /// <summary>Clears an aborted transaction's staged write intent + MVCC snapshot on the owning actor (the durable
    /// analog of ApplyConfirmedRollback), so the key is not blocked until the intent lease expires. Null in bare
    /// protocol tests (no actor).</summary>
    public delegate Task ApplyRollbackLocally(int partitionId, PreparedIntent intent);

    private readonly TransactionRecordStore recordStore;

    private readonly PreparedIntentStore intentStore;

    private readonly ReplicateDelegate replicate;

    private readonly ResolutionScheduler scheduleResolution;

    private readonly ApplyCommitLocally? applyCommitLocally;

    private readonly ApplyRollbackLocally? applyRollbackLocally;

    public DurableTransactionFinalizer(
        TransactionRecordStore recordStore,
        PreparedIntentStore intentStore,
        ReplicateDelegate replicate,
        ResolutionScheduler? resolutionScheduler = null,
        ApplyCommitLocally? applyCommitLocally = null,
        ApplyRollbackLocally? applyRollbackLocally = null)
    {
        this.recordStore = recordStore;
        this.intentStore = intentStore;
        this.replicate = replicate;
        this.applyCommitLocally = applyCommitLocally;
        this.applyRollbackLocally = applyRollbackLocally;
        // Inline default: resolution completes before FinalizeAsync returns (the pre-deferral behavior).
        this.scheduleResolution = resolutionScheduler
            ?? (static resolution => resolution(CancellationToken.None).GetAwaiter().GetResult());
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
        // ── Initialize the canonical record (Undecided) on the anchor partition ──
        // Nothing is durable yet if this fails, so it is a clean retry.
        byte[] initDelta = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(
            input.TransactionId, input.Epoch, input.CoordinatorKey, input.RecordAnchorKey,
            input.CommitTimestamp, input.DecisionDeadline, input.ManifestHash, input.Manifest, opId, input.CreatedAt)]);

        if (!await ReplicateRecordAsync(input.AnchorPartitionId, initDelta, cancellationToken).ConfigureAwait(false))
            return Retry();

        // ── Prepare barrier: prepare every partition, waiting for all (never abandon a submission on the first
        // failure — its outcome is needed to drive a truthful abort). A prepared-then-aborted intent is cleaned
        // up by resolution/recovery; a failed prepare forces the transaction to abort. ──
        bool allPrepared = true;
        foreach (DurablePartitionPrepare partition in input.Partitions)
        {
            byte[] prepareDelta = PreparedIntentStore.SerializeDelta(partition.Intents.Select(i => (PreparedIntentCommand)new PrepareIntentCommand(i)));
            if (!await ReplicateIntentsAsync(partition.PartitionId, prepareDelta, cancellationToken).ConfigureAwait(false))
                allPrepared = false;
        }

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

        // ── Resolution: apply the terminal decision to every prepared intent — on commit, materialize each intent
        // into visible KV state, then settle (resolve + remove) the intent. Under deferred settlement this runs off
        // the commit critical path: the decision is already durable, reads/writers observe the committed intent via
        // the §6 visibility contract until materialization lands, and recovery finishes it if a deferred run is
        // lost. The scheduler decides whether it runs inline (default) or in the background (production). ──
        scheduleResolution(ct => ResolveAsync(input, ct));

        return outcome;
    }

    private async Task<DurableFinalizeOutcome> DecideAsync(
        DurableFinalizeInput input, bool commit, TransactionAbortClass abortClass, HLCTimestamp opId, CancellationToken cancellationToken)
    {
        TransactionRecordCommand decision = commit
            ? new CommitTransactionCommand(input.TransactionId, input.Epoch, input.ManifestHash, opId, opId)
            : new AbortTransactionCommand(input.TransactionId, input.Epoch, input.ManifestHash, abortClass, opId, opId,
                input.RecordAnchorKey, input.CommitTimestamp, input.DecisionDeadline, input.CreatedAt);

        byte[] delta = TransactionRecordStore.SerializeDelta([decision]);

        if (!await ReplicateRecordAsync(input.AnchorPartitionId, delta, cancellationToken).ConfigureAwait(false))
            return Retry();

        // The winner is whatever the canonical record actually reflects after apply, not what we requested — a
        // concurrent recovery abort may have won the race in the log.
        TransactionRecord? record = recordStore.Get(input.TransactionId, input.Epoch);
        if (record is null)
            return Retry();

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

        foreach (DurablePartitionPrepare partition in input.Partitions)
        {
            // On commit, materialize each intent's value as an ordinary key/value record so the existing
            // replicator applies it to visible MVCC/persistence — durably, BEFORE the intent is removed below.
            // On abort nothing materializes.
            if (commit)
            {
                foreach (PreparedIntent intent in partition.Intents)
                {
                    // Replicate the committed value as an ordinary key/value record so followers converge, then
                    // apply it on the leader (the leader does not apply it through the replication callback).
                    byte[] kvRecord = PreparedIntentMaterializer.ToKeyValueRecord(intent);
                    bool applied = await replicate(partition.PartitionId, ReplicationTypes.KeyValues, kvRecord, cancellationToken).ConfigureAwait(false);
                    if (applied && applyCommitLocally is not null)
                        await applyCommitLocally(partition.PartitionId, intent).ConfigureAwait(false);
                }
            }
            else
            {
                // Abort: nothing materializes, but clear each participant's staged write intent + MVCC on the leader
                // so the key is not blocked until the intent lease expires.
                foreach (PreparedIntent intent in partition.Intents)
                    if (applyRollbackLocally is not null)
                        await applyRollbackLocally(partition.PartitionId, intent).ConfigureAwait(false);
            }

            await SettleIntentsAsync(partition.PartitionId, partition.Intents, commit, cancellationToken).ConfigureAwait(false);
        }
    }

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

    private async Task<bool> ReplicateRecordAsync(int partitionId, byte[] delta, CancellationToken cancellationToken)
    {
        bool ok = await replicate(partitionId, ReplicationTypes.TransactionRecord, delta, cancellationToken).ConfigureAwait(false);
        if (ok)
            recordStore.Replicate(partitionId, new RaftLog { LogType = ReplicationTypes.TransactionRecord, LogData = delta });

        return ok;
    }

    private async Task<bool> ReplicateIntentsAsync(int partitionId, byte[] delta, CancellationToken cancellationToken)
    {
        bool ok = await replicate(partitionId, ReplicationTypes.PreparedIntent, delta, cancellationToken).ConfigureAwait(false);
        if (ok)
            intentStore.Replicate(partitionId, new RaftLog { LogType = ReplicationTypes.PreparedIntent, LogData = delta });

        return ok;
    }

    private static DurableFinalizeOutcome Retry() => new(DurableFinalizeResult.MustRetry, TransactionAbortClass.RetryableFailure);
}
