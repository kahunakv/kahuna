using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kommander.Data;
using Kommander.Time;

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

    public DurableTransactionRecovery(
        PreparedIntentStore intentStore,
        DurableTransactionFinalizer.ReplicateDelegate replicate,
        LookupRecordDelegate lookupRecord,
        DriveAbortDelegate driveAbort)
    {
        this.intentStore = intentStore;
        this.replicate = replicate;
        this.lookupRecord = lookupRecord;
        this.driveAbort = driveAbort;
    }

    /// <summary>Resolves every eligible unresolved intent on <paramref name="partitionId"/> and returns how many
    /// intents were resolved. Bounded by the current due set; safe to run repeatedly (idempotent).</summary>
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

            await ResolveGroupAsync(partitionId, group, commit.Value, cancellationToken).ConfigureAwait(false);
            resolved += group.Count();
        }

        return resolved;
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

        // Undecided past its deadline, or no record at all (orphan prepare): drive a presumed abort and take the
        // outcome that actually won at the canonical record — never assume the abort won.
        AbortTransactionCommand abort = new(
            intent.TransactionId, intent.Epoch, intent.ManifestHash, TransactionAbortClass.PresumedAbort,
            OpId: now, AttemptHlc: now,
            intent.RecordAnchorKey,
            CommitTimestamp: record?.CommitTimestamp ?? intent.CommitTimestamp,
            DecisionDeadline: record?.DecisionDeadline ?? intent.RecoveryDeadline,
            CreatedAt: record?.CreatedAt ?? now);

        TransactionRecord? after = await driveAbort(abort, intent.RecordAnchorKey, cancellationToken).ConfigureAwait(false);

        return after?.Decision switch
        {
            TransactionDecision.Commit => true,
            TransactionDecision.Abort => false,
            _ => null
        };
    }

    private async Task ResolveGroupAsync(int partitionId, IEnumerable<PreparedIntent> intents, bool commit, CancellationToken cancellationToken)
    {
        List<PreparedIntent> group = intents.ToList();

        if (commit)
        {
            foreach (PreparedIntent intent in group)
            {
                byte[] kvRecord = PreparedIntentMaterializer.ToKeyValueRecord(intent);
                await replicate(partitionId, ReplicationTypes.KeyValues, kvRecord, cancellationToken).ConfigureAwait(false);
            }
        }

        byte[] resolveDelta = PreparedIntentStore.SerializeDelta(group.Select(i =>
            (PreparedIntentCommand)new ResolveIntentCommand(i.TransactionId, i.Epoch, i.Key, commit)));

        if (await replicate(partitionId, ReplicationTypes.PreparedIntent, resolveDelta, cancellationToken).ConfigureAwait(false))
            intentStore.Replicate(partitionId, new RaftLog { LogType = ReplicationTypes.PreparedIntent, LogData = resolveDelta });
    }
}
