
using System.Collections.Concurrent;
using System.Diagnostics;
using DotNext;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Replication;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Communication.Grpc;
using TransactionHandle = Kahuna.Shared.KeyValue.TransactionHandle;
using Kahuna.Server.KeyValues.Handlers;

namespace Kahuna.Server.KeyValues.Transactions;

// Commit timestamp minting and single-actor-turn finalize.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// Mints a durable commit timestamp on the coordinator's clock, strictly above the transaction id, every
    /// staged write's participant stamp, and <paramref name="commitFloor"/> (the highest snapshot timestamp at
    /// which a read was served the committed head of a staged key; Zero if none).
    /// </summary>
    internal static HLCTimestamp MintDurableCommitTimestamp(
        HybridLogicalClock clock, int nodeId, HLCTimestamp transactionId, Dictionary<string, StagedValue> staged, HLCTimestamp commitFloor = default)
    {
        HLCTimestamp highest = transactionId;

        foreach (StagedValue value in staged.Values)
        {
            if (value.StagedAt > highest)
                highest = value.StagedAt;
        }

        if (commitFloor > highest)
            highest = commitFloor;

        return clock.ReceiveEvent(nodeId, highest);
    }

    /// <summary>
    /// Commit id for the manual (ephemeral) prepare: a fresh HLC event past the highest LastModified the
    /// transaction's confirmed writes observed, so the prepare's "modified after the commit id" check cannot
    /// trip on the transaction's own staged write.
    /// </summary>
    private HLCTimestamp MintManualCommitId(TransactionContext context)
    {
        HLCTimestamp highestModifiedTime = context.TransactionId;

        if (context.HighestWriteTime > highestModifiedTime)
            highestModifiedTime = context.HighestWriteTime;

        return raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), highestModifiedTime);
    }

    /// <summary>
    /// Whether the finalize is one prepare, one range-lock probe, and one commit on the same ephemeral key:
    /// the shape one actor turn can replace.
    ///
    /// <para>Excluded: transactions that validate reads, because their commit-time probe asks other keys about
    /// concurrent writers, which one key's actor cannot answer; and yielding transactions, because the pin
    /// before their finalize has not been exercised with a fused turn.</para>
    /// </summary>
    private bool CanFinalizeInOneActorTurn(TransactionContext context, out string key)
    {
        key = "";

        if (!configuration.FusedEphemeralFinalize
            || context.ModifiedKeys is not { Count: 1 }
            || context.LocksAcquired is null
            || context.ConflictPolicy == TransactionConflictPolicy.Yield
            || RequiresReadSetValidation(context))
            return false;

        foreach ((string modifiedKey, KeyValueDurability durability) in context.ModifiedKeys)
        {
            if (durability != KeyValueDurability.Ephemeral || string.IsNullOrEmpty(modifiedKey))
                return false;

            key = modifiedKey;
        }

        return true;
    }

    /// <summary>
    /// Whether a transaction with these options may run inside one turn of the actor owning its only key. Its
    /// finalize must be the single-key, no-read-validation shape. This is decided before the script runs because
    /// the turn cannot change course midway.
    /// </summary>
    internal bool CanRunInActorTurn(TransactionContext context) =>
        configuration.ScriptActorTurns
        && context.ConflictPolicy != TransactionConflictPolicy.Yield
        && context.DecisionDurability != DecisionDurability.Durable
        && context.ReadTimestamp.IsNull()
        && !RequiresReadSetValidation(context);

    /// <summary>
    /// Two-phase commit for a transaction that ran inside one turn of the actor owning its only key. It is only
    /// the all-ephemeral, single-key branch of <see cref="TwoPhaseCommit"/>: eligibility rules out read
    /// validation, a durable decision and a yielding policy, and the turn refuses any key but its own.
    /// </summary>
    internal async Task FinalizeInActorTurn(TransactionContext context, string key, IActorTurnFinalizer turn, CancellationToken cancellationToken)
    {
        if (context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
            return;

        if (context.ModifiedKeys.Count != 1 || !context.ModifiedKeys.Contains((key, KeyValueDurability.Ephemeral)))
            throw new ActorTurnEscapeException("The write set is not the key the actor turn owns");

        if (context.LocksAcquired is null)
        {
            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction modified keys but holds no lock set", Refusal = CommitRefusalClass.InvalidWorkingSet };
            return;
        }

        if (!await TryFinalizeInOneActorTurn(context, key, cancellationToken, turn))
            throw new ActorTurnEscapeException("The actor turn could not finalize its own key");
    }

    /// <summary>
    /// Finalizes a single-ephemeral-key transaction in one turn of the actor that owns the key. Returns false,
    /// with the transaction back in <see cref="KeyValueTransactionState.Pending"/> and nothing done, when this
    /// node does not lead the key or the actor refused the message; the caller then runs the ordinary
    /// three-message finalize. Returns true when finalized here, whatever the outcome.
    ///
    /// <para>Each outcome is reported as the three-message path reports it (same state transitions, result,
    /// reason, metric, log line, and exception on a failed commit), so callers cannot tell which path ran.</para>
    /// </summary>
    private async Task<bool> TryFinalizeInOneActorTurn(
        TransactionContext context, string key, CancellationToken cancellationToken, IActorTurnFinalizer? turn = null)
    {
        const KeyValueDurability durability = KeyValueDurability.Ephemeral;

        if (!context.SetState(KeyValueTransactionState.Preparing, KeyValueTransactionState.Pending))
            throw new KahunaAbortedException("Failed to set transaction state to Preparing");

        HLCTimestamp commitId = MintManualCommitId(context);

        // Inside an actor turn the running actor serves the request. The locator is skipped: whoever started
        // the turn already confirmed the key is led here.
        (KeyValueResponseType, KeyValueFinalizeStage)? answer = turn is not null
            ? await turn.TryFinalize(context.TransactionId, commitId, key, context.RecordAnchorKey)
            : await manager.TryFinalizeMutationIfLocal(
                context.TransactionId, commitId, key, durability, context.RecordAnchorKey, cancellationToken);

        // Not led here, or the mailbox refused the message: in both cases nothing ran on the key.
        if (answer is null || answer.Value is (KeyValueResponseType.MustRetry, KeyValueFinalizeStage.Unknown))
        {
            if (!context.SetState(KeyValueTransactionState.Pending, KeyValueTransactionState.Preparing))
                throw new KahunaAbortedException("Failed to set transaction state back to Pending");

            return false;
        }

        (KeyValueResponseType type, KeyValueFinalizeStage stage) = answer.Value;

        DurableTransactionMetrics.FusedEphemeralFinalize();

        if (!context.SetState(KeyValueTransactionState.Prepared, KeyValueTransactionState.Preparing))
            throw new KahunaAbortedException("Failed to set transaction state to Prepared");

        switch (stage)
        {
            case KeyValueFinalizeStage.Commit:
                if (!context.SetState(KeyValueTransactionState.Committing, KeyValueTransactionState.Prepared))
                    throw new KahunaAbortedException("Failed to set transaction state to Committing");

                if (type != KeyValueResponseType.Committed)
                {
                    string reason = $"Failed to commit mutation {key}: {type}";
                    context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = reason, Refusal = CommitRefusalClass.Faulted };
                    logger.LogWarning("CommitMutations: {Type} {Key} {TicketId}", type, key, HLCTimestamp.Zero);
                    throw new KahunaAbortedException(reason);
                }

                if (!context.SetState(KeyValueTransactionState.Committed, KeyValueTransactionState.Committing))
                    throw new KahunaAbortedException("Failed to set transaction state to Committed");

                return true;

            case KeyValueFinalizeStage.RangeLock:
            {
                DurableTransactionMetrics.RangeLockFenceAborts.Add(1);

                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = $"Foreign range lock covers written key {key}",
                    Refusal = CommitRefusalClass.Conflict
                };

                logger.LogWriteSkewGuardAborted(context.TransactionId, key);

                // Inside an actor turn the running actor serves the rollback too. The ordinary rollback goes
                // through the mailbox and would wait for a turn that cannot start until this one ends.
                if (turn is not null)
                {
                    if (!context.SetState(KeyValueTransactionState.RollingBack, KeyValueTransactionState.Prepared))
                        throw new KahunaAbortedException("Failed to set transaction state to RollingBack");

                    KeyValueResponseType rolledBack = await turn.TryRollback(context.TransactionId, key);

                    if (rolledBack == KeyValueResponseType.MustRetry)
                    {
                        // The turn releases the key on its way out, which clears the intent as well.
                        logger.LogWarning("RollbackMutations: transient failure on {Key} inside the actor turn", key);
                        return true;
                    }

                    if (rolledBack != KeyValueResponseType.RolledBack)
                        logger.LogWarning("RollbackMutations: {Type} {Key} {TicketId}", rolledBack, key, HLCTimestamp.Zero);

                    if (!context.SetState(KeyValueTransactionState.RolledBack, KeyValueTransactionState.RollingBack))
                        throw new KahunaAbortedException("Failed to set transaction state to RolledBack");

                    return true;
                }

                // The prepare ran, so its write intent is in place and is rolled back the ordinary way.
                List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> prepared = [(key, HLCTimestamp.Zero, durability)];

                if (context.AsyncRelease)
                    _ = RollbackMutations(context, prepared, CancellationToken.None);
                else
                    await RollbackMutations(context, prepared, cancellationToken);

                return true;
            }

            case KeyValueFinalizeStage.Prepare:
            case KeyValueFinalizeStage.Unknown:
            default:
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations", Refusal = CommitRefusalClass.Conflict };

                logger.LogWarning("Couldn't propose {Key} {Response}", key, type);

                return true;
        }
    }
}
