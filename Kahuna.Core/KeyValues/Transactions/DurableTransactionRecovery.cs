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

    private async Task ResolveGroupAsync(int partitionId, IEnumerable<PreparedIntent> intents, bool commit, CancellationToken cancellationToken)
    {
        List<PreparedIntent> group = intents.ToList();

        // Only settle (resolve + remove) an intent whose terminal effect is durably applied. On commit that means
        // its value materialized: removing an intent whose materialization did not commit would delete the only
        // durable copy of an already-committed value, so a false/thrown materialization leaves the intent for a
        // later sweep. On abort there is no value to lose, so every intent is settled.
        List<PreparedIntent> settleable;

        if (commit)
        {
            settleable = new(group.Count);
            foreach (PreparedIntent intent in group)
            {
                bool materialized;
                try
                {
                    byte[] kvRecord = PreparedIntentMaterializer.ToKeyValueRecord(intent);
                    materialized = await replicate(partitionId, ReplicationTypes.KeyValues, kvRecord, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    materialized = false;
                }

                if (materialized)
                    settleable.Add(intent);
            }
        }
        else
        {
            settleable = group;
        }

        if (settleable.Count == 0)
            return;

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
        byte[] resolveDelta = PreparedIntentStore.SerializeDelta(settle);
        await replicate(partitionId, ReplicationTypes.PreparedIntent, resolveDelta, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false);
    }
}
