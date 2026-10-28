
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

// Per-key prepare, commit, and rollback of mutations.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// Places write intents on every confirmed modified key (prepare phase of 2PC).
    /// </summary>
    internal async Task<(bool, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>?)> PrepareMutations(
        TransactionContext context,
        CancellationToken cancellationToken,
        IReadOnlyList<(string Key, KeyValueDurability Durability)>? keysToPrepare = null
    )
    {
        if (context.LocksAcquired is null || context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
            return (false, null);

        // Prepare a caller-supplied subset (a mixed transaction's ephemeral keys, prepared apart from the durable
        // persistent ones) or, by default, every modified key (the all-in-one ticket path).
        IReadOnlyList<(string Key, KeyValueDurability Durability)> keys = keysToPrepare ?? [.. context.ModifiedKeys];
        if (keys.Count == 0)
            return (true, []);

        if (!context.SetState(KeyValueTransactionState.Preparing, KeyValueTransactionState.Pending))
            throw new KahunaAbortedException("Failed to set transaction state to Preparing");

        HLCTimestamp commitId = MintManualCommitId(context);

        if (keys.Count == 1)
        {
            (string key, KeyValueDurability durability) = keys[0];

            (KeyValueResponseType type, HLCTimestamp ticketId, string _, KeyValueDurability _) = await manager.LocateAndTryPrepareMutations(
                context.TransactionId,
                commitId,
                key, durability,
                cancellationToken,
                recordAnchorKey: context.RecordAnchorKey
            );

            if (!context.SetState(KeyValueTransactionState.Prepared, KeyValueTransactionState.Preparing))
                throw new KahunaAbortedException("Failed to set transaction state to Prepared");

            if (type != KeyValueResponseType.Prepared)
            {
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations", Refusal = CommitRefusalClass.Conflict };

                logger.LogWarning("Couldn't propose {Key} {Response}", key, type);

                return (false, null);
            }

            return (true, [(key, ticketId, durability)]);
        }

        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> proposalResponses = await manager.LocateAndTryPrepareManyMutations(
            context.TransactionId,
            commitId,
            [.. keys],
            cancellationToken,
            context.RecordAnchorKey
        );

        if (!context.SetState(KeyValueTransactionState.Prepared, KeyValueTransactionState.Preparing))
            throw new KahunaAbortedException("Failed to set transaction state to Prepared");

        if (proposalResponses.Any(r => r.Item1 != KeyValueResponseType.Prepared))
        {
            List<(string, HLCTimestamp, KeyValueDurability)> preparedMutations = [];

            foreach ((KeyValueResponseType, HLCTimestamp, string, KeyValueDurability) proposalResponse in proposalResponses)
            {
                if (proposalResponse.Item1 == KeyValueResponseType.Prepared)
                    preparedMutations.Add((proposalResponse.Item3, proposalResponse.Item2, proposalResponse.Item4));

                logger.LogWarning("Couldn't propose {Key} {Response}", proposalResponse.Item3, proposalResponse.Item1);
            }

            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations", Refusal = CommitRefusalClass.Conflict };

            return (false, preparedMutations);
        }

        return (true, proposalResponses.Select(r => (r.Item3, r.Item2, r.Item4)).ToList());
    }

    /// <summary>
    /// Commits the prepared mutations to the key-value store, retrying transient MustRetry responses.
    /// </summary>
    internal async Task CommitMutations(TransactionContext context, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> mutationsPrepared, CancellationToken cancellationToken)
    {
        if (mutationsPrepared.Count == 0)
            return;

        if (!context.SetState(KeyValueTransactionState.Committing, KeyValueTransactionState.Prepared))
            throw new KahunaAbortedException("Failed to set transaction state to Committing");

        if (mutationsPrepared.Count == 1)
        {
            (string key, HLCTimestamp ticketId, KeyValueDurability durability) = mutationsPrepared.First();

            bool committed = false;
            for (int attempt = 0; attempt <= MaxPhase2Retries; attempt++)
            {
                if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                    break;

                (KeyValueResponseType response, long _) = await manager.LocateAndTryCommitMutations(
                    context.TransactionId,
                    key,
                    ticketId,
                    durability,
                    CancellationToken.None
                );

                if (response == KeyValueResponseType.Committed)
                {
                    committed = true;
                    break;
                }

                if (response == KeyValueResponseType.MustRetry)
                {
                    logger.LogWarning("CommitMutations: transient failure on {Key} attempt {Attempt}, retrying", key, attempt + 1);
                    continue;
                }

                string reason = $"Failed to commit mutation {key}: {response}";
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = reason, Refusal = CommitRefusalClass.Faulted };
                logger.LogWarning("CommitMutations: {Type} {Key} {TicketId}", response, key, ticketId);
                throw new KahunaAbortedException(reason);
            }

            if (!committed)
            {
                string reason = $"Exhausted retries committing mutation {key}";
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = reason, Refusal = CommitRefusalClass.Unresolved };
                logger.LogError("CommitMutations: exhausted {MaxRetries} retries for {Key}", MaxPhase2Retries, key);
                throw new KahunaAbortedException(reason);
            }

            if (!context.SetState(KeyValueTransactionState.Committed, KeyValueTransactionState.Committing))
                throw new KahunaAbortedException("Failed to set transaction state to Committed");

            return;
        }

        Dictionary<string, (HLCTimestamp ticketId, KeyValueDurability durability)> pendingCommits =
            mutationsPrepared.ToDictionary(x => x.key, x => (x.ticketId, x.durability));

        // Phase two may report Aborted only when EVERY participant provably did not commit, which is only when
        // its group's CommitLogs returned a hard error (Errored). A MustRetry is in-doubt: the shared commit may
        // have landed, or a transient may clear on re-drive. So a pending MustRetry participant, like a committed
        // one, forbids Aborted. A shared partition ticket commits its whole group at once, so this is per group.
        bool anyCommitted = false;
        HashSet<string> permanentlyFailed = [];

        for (int attempt = 0; attempt <= MaxPhase2Retries && pendingCommits.Count > 0; attempt++)
        {
            if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                break;

            List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> batch =
                [.. pendingCommits.Select(kvp => (kvp.Key, kvp.Value.ticketId, kvp.Value.durability))];

            List<(KeyValueResponseType, string, long, KeyValueDurability)> responses =
                await manager.LocateAndTryCommitManyMutations(context.TransactionId, batch, CancellationToken.None);

            bool hasTransientFailure = false;
            bool hasPermanentFailure = false;
            foreach ((KeyValueResponseType response, string key, long commitIndex, KeyValueDurability durability) in responses)
            {
                if (response == KeyValueResponseType.Committed)
                {
                    pendingCommits.Remove(key);
                    permanentlyFailed.Remove(key);
                    anyCommitted = true;
                    continue;
                }

                if (response == KeyValueResponseType.MustRetry)
                {
                    hasTransientFailure = true;
                    permanentlyFailed.Remove(key);
                    logger.LogWarning("CommitMutations: transient failure on {Key} attempt {Attempt}, retrying", key, attempt + 1);
                    continue;
                }

                // Hard failure: this group's CommitLogs never took effect. Decide abort vs in-doubt only after
                // the whole run, since a sibling group may have committed.
                hasPermanentFailure = true;
                permanentlyFailed.Add(key);
                logger.LogError("CommitMutations: permanent failure {Type} on {Key} attempt {Attempt}", response, key, attempt + 1);
            }

            if (hasPermanentFailure || !hasTransientFailure)
                break;
        }

        if (pendingCommits.Count > 0)
        {
            // In-doubt unless every still-pending participant provably failed (hard Errored) AND none committed.
            bool everyPendingProvablyFailed = pendingCommits.Keys.All(permanentlyFailed.Contains);

            if (anyCommitted || !everyPendingProvablyFailed)
            {
                // Some participant committed or is in-doubt. Aborting would lie about a possibly-committed
                // participant, so stay Committing and return MustRetry; a retry (or Durable recovery) re-drives
                // the rest.
                logger.LogWarning(
                    "CommitMutations: {Count} participant(s) not confirmed committed; in-doubt, returning MustRetry: {Keys}",
                    pendingCommits.Count, string.Join(", ", pendingCommits.Keys));

                context.Result = new() { Type = KeyValueResponseType.MustRetry, Reason = "Commit in-doubt: not every participant confirmed, remainder pending re-drive" };
                return;
            }

            // Nothing took effect anywhere, so Aborted is truthful and gives a terminal answer, not endless MustRetry.
            logger.LogError("CommitMutations: all {Count} participant(s) hard-failed to commit, none committed: {Keys}",
                pendingCommits.Count, string.Join(", ", pendingCommits.Keys));

            string reason = $"All {pendingCommits.Count} participant(s) failed to commit and none committed";
            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = reason, Refusal = CommitRefusalClass.Faulted };
            throw new KahunaAbortedException(reason);
        }

        if (!context.SetState(KeyValueTransactionState.Committed, KeyValueTransactionState.Committing))
            throw new KahunaAbortedException("Failed to set transaction state to Committed");
    }

    /// <summary>
    /// Rolls back a collection of prepared mutations, retrying MustRetry participants.
    /// </summary>
    internal async Task RollbackMutations(TransactionContext context, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> mutationsPrepared, CancellationToken cancellationToken)
    {
        if (mutationsPrepared.Count == 0)
            return;

        // Rollback may start from Prepared (a prepare or read-set failure) or from Committing (a Durable anchor
        // that never committed, after CommitMutations advanced the state). In both, every prepared ticket is
        // still uncommitted. Accepting only Prepared would throw from Committing and orphan the tickets.
        if (!context.SetState(KeyValueTransactionState.RollingBack, KeyValueTransactionState.Prepared) &&
            !context.SetState(KeyValueTransactionState.RollingBack, KeyValueTransactionState.Committing))
            throw new KahunaAbortedException("Failed to set transaction state to RollingBack");

        if (mutationsPrepared.Count == 1)
        {
            (string key, HLCTimestamp ticketId, KeyValueDurability durability) = mutationsPrepared.First();

            bool rolledBackConfirmed = false;
            for (int attempt = 0; attempt <= MaxPhase2Retries; attempt++)
            {
                if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                    break;

                (KeyValueResponseType response, long _) = await manager.LocateAndTryRollbackMutations(
                    context.TransactionId,
                    key,
                    ticketId,
                    durability,
                    CancellationToken.None
                );

                if (response == KeyValueResponseType.RolledBack)
                {
                    rolledBackConfirmed = true;
                    break;
                }

                if (response == KeyValueResponseType.MustRetry)
                {
                    logger.LogWarning("RollbackMutations: transient failure on {Key} attempt {Attempt}, retrying", key, attempt + 1);
                    continue;
                }

                logger.LogWarning("RollbackMutations: {Type} {Key} {TicketId}", response, key, ticketId);
                rolledBackConfirmed = true;
                break;
            }

            if (!rolledBackConfirmed)
            {
                logger.LogError("RollbackMutations: exhausted {MaxRetries} retries for {Key}; participant retains write intent",
                    MaxPhase2Retries, key);
                return;
            }

            if (!context.SetState(KeyValueTransactionState.RolledBack, KeyValueTransactionState.RollingBack))
                throw new KahunaAbortedException("Failed to set transaction state to RolledBack");

            return;
        }

        Dictionary<string, (HLCTimestamp ticketId, KeyValueDurability durability)> pendingRollbacks =
            mutationsPrepared.ToDictionary(x => x.key, x => (x.ticketId, x.durability));

        for (int attempt = 0; attempt <= MaxPhase2Retries && pendingRollbacks.Count > 0; attempt++)
        {
            if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                break;

            List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> batch =
                [.. pendingRollbacks.Select(kvp => (kvp.Key, kvp.Value.ticketId, kvp.Value.durability))];

            List<(KeyValueResponseType, string, long, KeyValueDurability)> responses =
                await manager.LocateAndTryRollbackManyMutations(context.TransactionId, batch, CancellationToken.None);

            bool hasTransientFailure = false;
            foreach ((KeyValueResponseType response, string key, long commitIndex, KeyValueDurability durability) in responses)
            {
                if (response == KeyValueResponseType.RolledBack)
                {
                    // A settled rollback is the only positive proof the participant's prepare state is gone.
                    pendingRollbacks.Remove(key);
                    continue;
                }

                // Anything else (MustRetry, unsettled Errored) does NOT prove the intent or proposal is gone, so
                // retain and retry. On exhaustion the pending set is logged and the state expires with its
                // write-intent lease.
                hasTransientFailure = true;
                logger.LogWarning("RollbackMutations: unsettled {Type} on {Key} attempt {Attempt}, retaining", response, key, attempt + 1);
            }

            if (!hasTransientFailure)
                break;
        }

        if (pendingRollbacks.Count > 0)
        {
            logger.LogError("RollbackMutations: exhausted {MaxRetries} retries, {Count} participants still pending: {Keys}",
                MaxPhase2Retries, pendingRollbacks.Count, string.Join(", ", pendingRollbacks.Keys));
            return;
        }

        if (!context.SetState(KeyValueTransactionState.RolledBack, KeyValueTransactionState.RollingBack))
            throw new KahunaAbortedException("Failed to set transaction state to RolledBack");
    }

    /// <summary>
    /// Waits the inter-retry backoff, honouring cancellation.
    /// Returns true when the delay elapsed normally; false when cancellation fired.
    /// </summary>
    internal static async Task<bool> DelayBetweenRetries(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(Phase2RetryDelayMs, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
