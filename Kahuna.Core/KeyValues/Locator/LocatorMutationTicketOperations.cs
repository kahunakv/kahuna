
using Kommander;
using Kommander.Time;
using Kommander.Diagnostics;

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kahuna.Server.Routing;
using Kahuna.Shared.Routing;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Prepare, commit and rollback of staged mutations, single and many-key.
/// </summary>
internal sealed class LocatorMutationTicketOperations
{
    private readonly KeyValuesManager manager;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly DataPartitionRouter dataPartitionRouter;

    private readonly ILogger<IKahuna> logger;

    private readonly LocatorRouting routing;

    internal LocatorMutationTicketOperations(LocatorRouting routing)
    {
        this.manager = routing.Manager;
        this.raft = routing.Raft;
        this.interNodeCommunication = routing.InterNodeCommunication;
        this.keySpaceRegistry = routing.KeySpaceRegistry;
        this.dataPartitionRouter = routing.DataPartitionRouter;
        this.logger = routing.Logger;
        this.routing = routing;
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancelationToken,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, HLCTimestamp.Zero, key, durability);

        // Resolve both partition and generation; preserve the coordinator's generation when redirected.
        int partitionId;
        long freshGeneration;
        (partitionId, freshGeneration) = RangeRouting.Locate(
            keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key);
        if (routedGeneration == 0)
            routedGeneration = freshGeneration;

        if (!raft.Joined || await routing.ConfirmLeadershipForActorMutation(partitionId, cancelationToken))
            return await manager.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration, recordAnchorKey);

        string? leader = await routing.TryWaitForLeader(partitionId, cancelationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability);

        logger.LogPrepareKeyValueRedirected(key, partitionId, leader);

        return await interNodeCommunication.TryPrepareMutations(leader, transactionId, commitId, key, durability, routedGeneration, cancelationToken, recordAnchorKey);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryPrepareManyMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys,
        CancellationToken cancelationToken,
        string? recordAnchorKey = null
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, HLCTimestamp.Zero, key.key, key.durability)];

            int partitionId = routing.RouteKey(key.key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, k.key, k.durability))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryPrepareNodeMutations(transactionId, commitId, leader, localNode, leaderByPartition, xkeys, lockSync, responses, cancelationToken, recordAnchorKey));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryPrepareNodeMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string leader, 
        string localNode, 
        Dictionary<int, string> leaderByPartition,
        List<(string key, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        logger.LogPrepareManyKeyValueRedirect(xkeys.Count, leader);

        if (leader == localNode)
        {
            if (!await routing.ConfirmLeadershipForGroupMutation(leader, leaderByPartition, cancellationToken))
            {
                lock (lockSync)
                {
                    foreach ((string key, KeyValueDurability durability) in xkeys)
                        responses.Add((KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability));
                }

                return;
            }

            List<(KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability)> prepareResponses = await manager.TryPrepareManyMutations(transactionId, commitId, xkeys, recordAnchorKey);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability) item in prepareResponses)
                    responses.Add((item.type, item.ticketId, item.key, item.durability));
            }

            return;
        }

        await interNodeCommunication.TryPrepareNodeMutations(leader, transactionId, commitId, xkeys, lockSync, responses, cancellationToken, recordAnchorKey);
    }
    
    /// <summary>
    /// Whether the actor that owns <paramref name="key"/> is on this node, with leadership of the key's
    /// partition confirmed the way any actor mutation confirms it, and the key is routed by hash. A key in a
    /// range-routed space is left out: its operations carry a routing generation that fences them against a
    /// range move, and work that bypasses the locator would bypass that fence.
    /// </summary>
    public async Task<bool> IsLocallyLedHashKey(string key, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(key) || !raft.Joined)
            return false;

        (int partitionId, _, bool isKeyRange, _) = routing.LocateRangeWithMode(key);

        if (isKeyRange)
            return false;

        return await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken);
    }

    /// <summary>
    /// Finalizes one ephemeral mutation in a single turn of the actor that owns the key, when that actor is
    /// on this node. Returns null when it is not — the partition is led elsewhere, or leadership could not be
    /// confirmed — and the caller then takes the ordinary prepare, probe, and commit path, which knows how
    /// to reach a remote leader. There is deliberately no inter-node form of this message: the fallback is
    /// the path every transaction used before, so a remote key loses nothing but the shortcut.
    ///
    /// <para>Leadership is confirmed the same way as for any actor mutation, so a deposed leader cannot
    /// commit through here what it could not commit through the three messages.</para>
    /// </summary>
    public async Task<(KeyValueResponseType, Handlers.KeyValueFinalizeStage)?> TryFinalizeMutationIfLocal(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        string? recordAnchorKey,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return null;

        int partitionId = routing.RouteKey(key);

        if (raft.Joined && !await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken))
            return null;

        return await manager.TryFinalizeMutation(transactionId, commitId, key, durability, recordAnchorKey);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = routing.RouteKey(key);

        if (!raft.Joined || await routing.ConfirmLeadershipForActorMutation(partitionId, cancelationToken))
            return await manager.TryCommitMutations(transactionId, key, ticketId, durability);
            
        string? leader = await routing.TryWaitForLeader(partitionId, cancelationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);
        
        logger.LogCommitKeyValueRedirected(key, partitionId, leader);
        
        return await interNodeCommunication.TryCommitMutations(leader, transactionId, key, ticketId, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryCommitManyMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, 0, key.durability)];

            int partitionId = routing.RouteKey(key.key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, 0L, k.durability))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryCommitManyMutations(transactionId, leader, localNode, leaderByPartition, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryCommitManyMutations(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        Dictionary<int, string> leaderByPartition,
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogCommitManyKeyValueRedirect(xkeys.Count, leader);
        
        if (leader == localNode)
        {
            if (!await routing.ConfirmLeadershipForGroupMutation(leader, leaderByPartition, cancelationToken))
            {
                lock (lockSync)
                {
                    foreach ((string key, HLCTimestamp _, KeyValueDurability durability) in xkeys)
                        responses.Add((KeyValueResponseType.MustRetry, key, 0, durability));
                }

                return;
            }

            List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> commitResponses = await manager.TryCommitManyMutations(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability) item in commitResponses)
                    responses.Add((item.type, item.key, item.proposalIndex, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryCommitNodeMutations(leader, transactionId, xkeys, lockSync, responses, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = routing.RouteKey(key);

        if (!raft.Joined || await routing.ConfirmLeadershipForActorMutation(partitionId, cancelationToken))
            return await manager.TryRollbackMutations(transactionId, key, ticketId, durability);

        string? leader = await routing.TryWaitForLeader(partitionId, cancelationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);

        logger.LogRollbackKeyValueRedirected(key, partitionId, leader);

        return await interNodeCommunication.TryRollbackMutations(leader, transactionId, key, ticketId, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryRollbackManyMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, 0, key.durability)];

            int partitionId = routing.RouteKey(key.key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, 0L, k.durability))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryRollbackManyMutations(transactionId, leader, localNode, leaderByPartition, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryRollbackManyMutations(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        Dictionary<int, string> leaderByPartition,
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogRollbackManyKeyValueRedirect(xkeys.Count, leader);
        
        if (leader == localNode)
        {
            if (!await routing.ConfirmLeadershipForGroupMutation(leader, leaderByPartition, cancelationToken))
            {
                lock (lockSync)
                {
                    foreach ((string key, HLCTimestamp _, KeyValueDurability durability) in xkeys)
                        responses.Add((KeyValueResponseType.MustRetry, key, 0, durability));
                }

                return;
            }

            List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> commitResponses = await manager.TryRollbackManyMutations(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability) item in commitResponses)
                    responses.Add((item.type, item.key, item.proposalIndex, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryRollbackNodeMutations(leader, transactionId, xkeys, lockSync, responses, cancelationToken);
    }
}
