
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

// The generic two-phase commit driver.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// Executes the 2PC protocol for the transaction.
    /// </summary>
    internal async Task TwoPhaseCommit(TransactionContext context, CancellationToken cancellationToken)
    {
        if (context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
        {
            // A read-only transaction must still prove its reads form one consistent cut. Each read saw the latest
            // commit at a different moment, so a commit between two reads gives a view no serial order explains.
            // The concurrent-writer probe runs first: it sees an undecided foreign intent, which the revision
            // check cannot. The revision re-check then catches reads a decided commit made stale. Failure sets
            // the Aborted result.
            if (RequiresReadSetValidation(context) && (context.ReadObservationConflict || context.ReadKeys is { Count: > 0 }))
            {
                // The probe also proves the transaction's locks are still held (see LocksStillInForce).
                if (!await CheckCommitConflicts(context, cancellationToken))
                    return;

                await ValidateReadSet(context, cancellationToken);
                return;
            }

            // A pessimistic transaction has no read set to validate; its locks make its reads consistent. That
            // holds only if the locks were held from each read until now. Locks live in leader memory, and a
            // leader change drops them silently, so prove they are still held.
            await LocksStillInForce(context, cancellationToken);
            return;
        }

        // Writes without a lock set cannot be prepared. Committing as a no-op would drop them while the caller
        // sees Committed, so abort.
        if (context.LocksAcquired is null)
        {
            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction modified keys but holds no lock set", Refusal = CommitRefusalClass.InvalidWorkingSet };
            return;
        }

        // Staged writes live only in the participant leader's memory, and a leader change drops them silently.
        // The next write then stages over the committed head, and the commit would carry that regression. Only
        // the coordinator saw both stagings (or a read without the staging), so it refuses here. The client
        // restarts the transaction; nothing is lost because nothing was committed.
        if (context.StagedChainBreak is { } stagedChainBreak)
        {
            DurableTransactionMetrics.StagedChainBreakAborts.Add(1);
            logger.LogWarning("Refusing to commit transaction {TransactionId}: {Break}", context.TransactionId, stagedChainBreak);
            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Lost staging: " + stagedChainBreak, ExclusionLost = true, Refusal = CommitRefusalClass.StagedChainBreak };
            return;
        }

        // A leader change also drops the transaction's locks. A later lock grant from another term proves it
        // before anything is prepared. If no lock was asked for after the change, the commit-time probe catches
        // it (see LocksStillInForce).
        if (context.LockGrantTermChange is { } lockGrantTermChange)
        {
            RefuseLostLock(context, new LostLock(LostLockDetection.Regrant, lockGrantTermChange));
            return;
        }

        // Durable mode promises crash-atomicity, which an ephemeral key cannot give, so reject it here. The split
        // below would commit ephemeral keys in memory, which is correct only for BestEffort. A Durable
        // transaction also needs a record anchor.
        if (context.DecisionDurability == DecisionDurability.Durable)
        {
            if (context.ModifiedKeys.Any(static k => k.Item2 != KeyValueDurability.Persistent))
            {
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "A durable transaction cannot modify an ephemeral key", Refusal = CommitRefusalClass.InvalidWorkingSet };
                return;
            }

            if (context.RecordAnchorKey is null)
            {
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "A durable transaction with modifications must have a record anchor", Refusal = CommitRefusalClass.InvalidWorkingSet };
                return;
            }
        }

        // A yielding transaction pins its intents before any prepare. A key lost to a foreground writer answers
        // Aborted, so it never commits over the winner. Applies to every finalize path below.
        if (context.ConflictPolicy == TransactionConflictPolicy.Yield && !await PinYieldingIntents(context, cancellationToken))
            return;

        // Split the working set. Persistent keys finalize through the durable-intent path (decision record plus
        // prepared intents). Ephemeral keys commit or roll back in memory, following that decision. An
        // all-ephemeral transaction falls through to the in-memory path below.
        List<(string Key, KeyValueDurability Durability)> persistentKeys = [];
        List<(string Key, KeyValueDurability Durability)> ephemeralKeys = [];
        foreach ((string, KeyValueDurability) modified in context.ModifiedKeys)
            (modified.Item2 == KeyValueDurability.Persistent ? persistentKeys : ephemeralKeys).Add(modified);

        if (TryBuildDurableFinalizeInput(context, persistentKeys, out DurableFinalizeInput? durableInput, out HLCTimestamp durableOpId))
        {
            if (ephemeralKeys.Count == 0)
            {
                await DurableFinalize(context, persistentKeys, durableInput!, durableOpId, cancellationToken).ConfigureAwait(false);
                return;
            }

            // Mixed transaction. Prepare the ephemeral keys first, so their failure aborts everything before a
            // persistent decision exists. Then the durable decision drives the ephemeral commit.
            (bool ephemeralOk, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? ephemeralMutations) =
                await PrepareMutations(context, cancellationToken, ephemeralKeys).ConfigureAwait(false);

            if (ephemeralMutations is null)
                return;

            if (!ephemeralOk)
            {
                if (context.AsyncRelease)
                    _ = RollbackMutations(context, ephemeralMutations, CancellationToken.None);
                else
                    await RollbackMutations(context, ephemeralMutations, cancellationToken).ConfigureAwait(false);
                return; // context.Result already set to Aborted by PrepareMutations
            }

            DurableFinalizeResult durableResult = await DurableFinalize(context, persistentKeys, durableInput!, durableOpId, cancellationToken).ConfigureAwait(false);

            // The durable decision is the outcome. On commit, apply the ephemeral writes (the persistent commit
            // stands either way); otherwise discard them.
            if (durableResult == DurableFinalizeResult.Committed)
                await CommitMutations(context, ephemeralMutations, cancellationToken).ConfigureAwait(false);
            else if (context.AsyncRelease)
                _ = RollbackMutations(context, ephemeralMutations, CancellationToken.None);
            else
                await RollbackMutations(context, ephemeralMutations, cancellationToken).ConfigureAwait(false);

            return;
        }

        // Persistent writes that cannot finalize durably (no staged value to replay losslessly) abort. Only
        // all-ephemeral transactions reach the in-memory path below.
        if (persistentKeys.Count > 0)
        {
            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Persistent mutation could not be staged for a durable commit", Refusal = CommitRefusalClass.InvalidWorkingSet };
            return;
        }

        {
            if (!await ValidateReadSet(context, cancellationToken))
                return;

            // One ephemeral key led by this node: prepare, range-lock probe and commit go to one actor, so run
            // them as one turn. Otherwise send the three messages.
            if (CanFinalizeInOneActorTurn(context, out string onlyKey))
            {
                // The single turn skips the commit-conflict probe, so ask for its lock proof here.
                if (!await LocksStillInForce(context, cancellationToken))
                    return;

                if (await TryFinalizeInOneActorTurn(context, onlyKey, cancellationToken))
                    return;
            }

            // Place write intents before the conflict probe, so a racing peer's probe sees them and aborts
            // (prevents write skew). The ephemeral prepare does not check range locks, so the probe also fences
            // these writes against a foreign range lock taken since staging.
            (bool success, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? mutationsPrepared) = await PrepareMutations(
                context,
                cancellationToken
            );

            if (mutationsPrepared is null)
                return;

            if (!success)
            {
                if (context.AsyncRelease)
                    _ = RollbackMutations(context, mutationsPrepared, CancellationToken.None);
                else
                    await RollbackMutations(context, mutationsPrepared, cancellationToken);
                return;
            }

            if (!await CheckCommitConflicts(context, cancellationToken))
            {
                if (context.AsyncRelease)
                    _ = RollbackMutations(context, mutationsPrepared, CancellationToken.None);
                else
                    await RollbackMutations(context, mutationsPrepared, cancellationToken);
                return;
            }

            await CommitMutations(context, mutationsPrepared, cancellationToken);
        }
    }
}
