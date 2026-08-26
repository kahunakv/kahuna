using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Utils;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The manual two-phase-commit ticket path: prepare, commit and rollback of key mutations against the
/// actors, plus the bulk lock release that accompanies them.
///
/// Persistent keys are expected never to settle here — a crash-atomic transaction finalizes through the
/// durable-intent path instead — so <see cref="ManualTicketPersistentSettlementCount"/> staying at zero
/// across an all-persistent or mixed transaction is the invariant proving this path was not taken.
/// Ephemeral keys legitimately settle here and are deliberately not counted.
/// </summary>
internal sealed class LocalMutationTicketOperations
{
    private const int MaxRetries = 3;

    private readonly KeyValuesRuntime runtime;

    // Counts persistent keys settled through the manual two-phase-commit ticket path (the actor
    // TryCommit/TryRollback mutation handlers that issue CommitLogs/RollbackLogs). A crash-atomic
    // transaction is finalized through the durable-intent path instead, so this must stay at zero for
    // every all-persistent or mixed transaction — the invariant that makes the manual ticket path
    // dead code for persistent keys. Ephemeral keys legitimately settle here (their in-memory commit)
    // and are not counted. Instance-scoped so a test can assert it per node without cross-test noise.
    private long manualTicketPersistentSettlements;

    /// <summary>
    /// Number of persistent keys that have been settled through the manual two-phase-commit ticket path
    /// on this node. Zero across an all-persistent or mixed transaction proves that path was never taken —
    /// the persistent subset went through the durable-intent path instead.
    /// </summary>
    internal long ManualTicketPersistentSettlementCount => Interlocked.Read(ref manualTicketPersistentSettlements);

    internal LocalMutationTicketOperations(KeyValuesRuntime runtime) => this.runtime = runtime;

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KeyValueLocator locator => runtime.Locator;

    private KeyValueActorRing ephemeralKeyValuesRouter => runtime.Routers.Ephemeral;

    private KeyValueActorRing persistentKeyValuesRouter => runtime.Routers.Persistent;

    private static ValueTask<KeyValueResponse?> AskKeyValueActor(
        KeyValueActorRing router,
        KeyValueRequest request) => KeyValueActorRouters.AskKeyValueActor(router, request);

    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keys.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> TryReleaseManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> keys
    )
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
                KeyValueRequestType.TryReleaseExclusiveLock,
                transactionId,
                HLCTimestamp.Zero,
                key.key,
                null,
                null,
                -1,
                KeyValueFlags.None,
                0,
                HLCTimestamp.Zero,
                key.durability,
                0,
                0,
                default
            );

            try
            {
                KeyValueResponse? response;

                if (key.durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null || response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    responses.Add((KeyValueResponseType.Errored, key.key, key.durability));
                    continue;
                }

                responses.Add((response.Type, key.key, key.durability));
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }

        return responses;
    }
    
    /// <summary>
    /// Passes a TryPrepare request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryPrepareMutations,
            transactionId,
            commitId,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        request.RoutedGeneration = routedGeneration;
        request.RecordAnchorKey = recordAnchorKey;

        try
        {
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, HLCTimestamp.Zero, key, durability);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryPrepareMutations_5521");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.Ticket, key, durability);
            }

            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes many TryPrepare requests to the key/value actor for the given keys.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> TryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys,
        string? recordAnchorKey = null
    )
    {
        // Only ephemeral participants use this manual 2PC prepare. A crash-atomic (persistent) mutation is
        // prepared and finalized through the durable-intent canonical-record path, so the manual persistent
        // prepare is retired: a persistent key here (only an external gRPC caller can reach this with
        // Persistent — the coordinator sends ephemeral) is rejected rather than proposed.
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> results = new(keys.Count);

        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            if (key.durability == KeyValueDurability.Persistent)
            {
                Interlocked.Increment(ref manualTicketPersistentSettlements);
                results.Add((KeyValueResponseType.Errored, HLCTimestamp.Zero, key.key, key.durability));
                continue;
            }

            results.Add(await PrepareOneMutation(transactionId, commitId, key, recordAnchorKey));
        }

        return results;
    }

    /// <summary>
    /// Prepares one mutation the per-key way: dispatches a <c>TryPrepareMutations</c> to the owning actor,
    /// which proposes (or, for a persistent key, dispatches the propose off its mailbox) and returns the
    /// per-key proposal ticket. Used for ephemeral participants and the not-joined single-node case, which
    /// are not batched.
    /// </summary>
    private async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> PrepareOneMutation(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        (string key, KeyValueDurability durability) key,
        string? recordAnchorKey
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryPrepareMutations,
            transactionId, commitId, key.key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, key.durability, 0, 0, default);

        request.RecordAnchorKey = recordAnchorKey;

        try
        {
            KeyValueResponse? response = key.durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null || response.Type == KeyValueResponseType.WaitingForReplication)
                return (KeyValueResponseType.Errored, HLCTimestamp.Zero, key.key, key.durability);

            return (response.Type, response.Ticket, key.key, key.durability);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    
    /// <summary>
    /// Passes a TryCommit request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryCommitMutations(
        HLCTimestamp transactionId, 
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    )
    {
        // The manual persistent 2PC commit is retired — a crash-atomic mutation commits through the
        // durable-intent path. Only an external gRPC caller can reach this with Persistent (the coordinator
        // sends ephemeral); reject it rather than committing through the removed ticket path.
        if (durability == KeyValueDurability.Persistent)
        {
            Interlocked.Increment(ref manualTicketPersistentSettlements);
            return (KeyValueResponseType.Errored, -1);
        }

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryCommitMutations,
            transactionId,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            proposalTicketId,
            durability,
            0,
            0,
            default
        );
        
        try
        {
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, -1);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryCommitMutations_5666");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.Revision);
            }

            return (KeyValueResponseType.Errored, -1);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes many TryCommit requests to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> TryCommitManyMutations(
        HLCTimestamp transactionId,
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> keys
    )
    {
        // Only ephemeral participants use this manual 2PC commit. A crash-atomic (persistent) mutation commits
        // through the durable-intent path, so a persistent key here (reachable only from an external gRPC
        // caller — the coordinator sends ephemeral) is rejected rather than committed through the removed
        // ticket path.
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> results = new(keys.Count);

        foreach ((string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key in keys)
        {
            if (key.durability == KeyValueDurability.Persistent)
            {
                Interlocked.Increment(ref manualTicketPersistentSettlements);
                results.Add((KeyValueResponseType.Errored, key.key, -1L, key.durability));
                continue;
            }

            results.Add(await CommitOneMutation(transactionId, key));
        }

        return results;
    }

    /// <summary>
    /// Commits one mutation the per-key way: dispatches a <c>TryCommitMutations</c> to the owning actor,
    /// which commits its ticket (or, for a persistent key, off its mailbox) and applies. Used for ephemeral
    /// participants and persistent keys with no shared ticket, which are not batched.
    /// </summary>
    private async Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> CommitOneMutation(
        HLCTimestamp transactionId,
        (string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryCommitMutations, transactionId, HLCTimestamp.Zero, key.key,
            null, null, -1, KeyValueFlags.None, 0, key.proposalTicketId, key.durability, 0, 0, default);

        try
        {
            KeyValueResponse? response = key.durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null || response.Type == KeyValueResponseType.WaitingForReplication)
                return (KeyValueResponseType.Errored, key.key, -1, key.durability);

            return (response.Type, key.key, response.Revision, key.durability);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    private static bool IsTransientRaftStatus(RaftOperationStatus status) => status is
        RaftOperationStatus.NodeIsNotLeader or
        RaftOperationStatus.ProposalQueueFull or
        RaftOperationStatus.RestoreInProgress or
        RaftOperationStatus.ProposalTimeout or
        RaftOperationStatus.ReplicationFailed or
        RaftOperationStatus.OperationCancelled or
        RaftOperationStatus.ProposalNotFound;
    
    /// <summary>
    /// Passes a TryRollback request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(
        HLCTimestamp transactionId, 
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    )
    {
        // The manual persistent 2PC rollback is retired — a crash-atomic mutation rolls back through the
        // durable-intent path. Only an external gRPC caller can reach this with Persistent; reject it.
        if (durability == KeyValueDurability.Persistent)
        {
            Interlocked.Increment(ref manualTicketPersistentSettlements);
            return (KeyValueResponseType.Errored, -1);
        }

        KeyValueRequest request = new(
            KeyValueRequestType.TryRollbackMutations,
            transactionId,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            proposalTicketId,
            durability,
            0,
            0,
            default
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
        else
            response = await AskKeyValueActor(persistentKeyValuesRouter, request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, -1);
        
        return (response.Type, response.Revision);
    }
    
    /// <summary>
    /// Passes many TryRollback requests to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> TryRollbackManyMutations(
        HLCTimestamp transactionId,
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> keys
    )
    {
        // Only ephemeral participants use this manual 2PC rollback. A persistent key here (reachable only from
        // an external gRPC caller — the coordinator sends ephemeral) is rejected rather than rolled back
        // through the removed ticket path.
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> results = new(keys.Count);

        foreach ((string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key in keys)
        {
            if (key.durability == KeyValueDurability.Persistent)
            {
                Interlocked.Increment(ref manualTicketPersistentSettlements);
                results.Add((KeyValueResponseType.Errored, key.key, -1L, key.durability));
                continue;
            }

            results.Add(await RollbackOneMutation(transactionId, key));
        }

        return results;
    }

    /// <summary>
    /// Rolls back one mutation the per-key way: dispatches a <c>TryRollbackMutations</c> to the owning actor,
    /// which rolls back its ticket and clears the local prepare state. Used for ephemeral participants and
    /// persistent keys with no shared ticket, which are not batched.
    /// </summary>
    private async Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> RollbackOneMutation(
        HLCTimestamp transactionId,
        (string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryRollbackMutations, transactionId, HLCTimestamp.Zero, key.key,
            null, null, -1, KeyValueFlags.None, 0, key.proposalTicketId, key.durability, 0, 0, default);

        try
        {
            KeyValueResponse? response = key.durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null)
                return (KeyValueResponseType.Errored, key.key, -1, key.durability);

            return (response.Type, key.key, response.Revision, key.durability);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

}
