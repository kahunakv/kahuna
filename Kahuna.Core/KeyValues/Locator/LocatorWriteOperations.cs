
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
/// Set, delete and extend operations: routes each write to the partition leader, batching many-key writes per node.
/// </summary>
internal sealed class LocatorWriteOperations
{
    private readonly KeyValuesManager manager;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly ILogger<IKahuna> logger;

    private readonly LocatorRouting routing;

    internal LocatorWriteOperations(LocatorRouting routing)
    {
        this.manager = routing.Manager;
        this.raft = routing.Raft;
        this.interNodeCommunication = routing.InterNodeCommunication;
        this.logger = routing.Logger;
        this.routing = routing;
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TrySet request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        long routedGeneration = 0
    )
    {
        if (string.IsNullOrEmpty(key) || expiresMs < 0)
            return (KeyValueResponseType.InvalidInput, 0, HLCTimestamp.Zero);

        // Key-range spaces route + fence via the descriptor map. Hash spaces use DataPartitionRouter.
        // routedGeneration is non-zero when this call arrived via an inter-node redirect; the coordinator's
        // generation is preserved so the remote fence checks against the coordinator's view, catching the
        // case where the coordinator is fresher (split applied there but not yet here) or staler (split
        // applied here but not there — fence fails → MustRetry → coordinator re-resolves).
        (int partitionId, long freshGeneration, bool isKeyRange, RangeDescriptor? descriptor) = routing.LocateRangeWithMode(key);
        if (isKeyRange)
        {
            if (routedGeneration == 0)
                routedGeneration = freshGeneration;

            // A range being moved to another partition refuses writes for the window between the
            // catch-up export and the cutover, so a write cannot land on the source after its
            // contents were copied. Bouncing here keeps the doomed write off the wire; the binding
            // refusal is the leader-side one at admission, which also covers writes routed by a node
            // whose map had not yet applied the quiesce. Clients retry after cutover and are then
            // routed to the partition that owns the range.
            if (routing.IsRangeQuiesced(descriptor, key))
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        }

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        // A transactional set stages an MVCC entry in the actor and never proposes to Raft, so it
        // needs the quorum-confirmed gate; a direct set replicates and fails on a deposed leader by itself.
        bool stagesInActor = transactionId != HLCTimestamp.Zero;

        if (stagesInActor
                ? await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken)
                : await routing.AmILeaderIfHosted(partitionId, cancellationToken))
        {
            routing.RecordLocalKeyRoute(key, partitionId, routedGeneration);

            return await manager.TrySetKeyValue(
                transactionId,
                key,
                value,
                compareValue,
                compareRevision,
                flags,
                expiresMs,
                durability,
                routedGeneration
            );
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        if (leader == raft.GetLocalEndpoint())
        {
            // The election view names this node but the quorum did not confirm it above: a staged
            // write must not land on a belief-only leader.
            if (stagesInActor)
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

            routing.RecordLocalKeyRoute(key, partitionId, routedGeneration);
            return await manager.TrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, routedGeneration);
        }

        routing.RecordForwardedKeyRoute(key, partitionId, leader, routedGeneration);

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        (KeyValueResponseType, long, HLCTimestamp) response = await interNodeCommunication.TrySetKeyValue(
            leader,
            transactionId,
            key,
            value,
            compareValue,
            compareRevision,
            flags,
            expiresMs,
            durability,
            routedGeneration,
            cancellationToken
        );

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogSetKeyValueRedirected(key, partitionId, leader, stopwatch.GetElapsedMilliseconds());

        return response;
    }

    /// <summary>
    /// Routes a set-many batch to the leaders of its keys' partitions and runs it there.
    ///
    /// <para>A non-transactional batch is independent writes: each leader's group runs once, and a key in a range
    /// being moved answers <see cref="KeyValueResponseType.MustRetry"/> alone while the rest of the batch proceeds.</para>
    ///
    /// <para>A transactional batch stages every item or none. A batch that resolves to one leader runs there once,
    /// and that node stages it all or none. A batch that resolves to several leaders runs in two rounds: every leader
    /// first checks its items and stages nothing, and only when every item everywhere passed does every leader stage
    /// its items. A refused check answers the refused items their refusal and every other item
    /// <see cref="KeyValueResponseType.NotApplied"/>. A key in a range being moved refuses the whole batch the same
    /// way, before anything is sent. Under optimistic locking a foreign write that lands between the two rounds can
    /// refuse one leader's items after another leader staged its own; the answers then show the staged items as
    /// <see cref="KeyValueResponseType.Set"/>, so the caller can see that the batch is partially staged and roll the
    /// transaction back. Under pessimistic locking the caller holds exclusive locks on every key since its lock
    /// round, so the stage round sees what the check round saw.</para>
    /// </summary>
    /// <param name="checkOnly">
    /// Run only the check round and stage nothing anywhere: the forwarded check of a batch another node coordinates.
    /// </param>
    public async Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
        List<KahunaSetKeyValueRequestItem> setManyItems, 
        CancellationToken cancellationToken,
        bool checkOnly = false
    )
    {                
        string localNode = raft.GetLocalEndpoint();

        bool transactional = LocatorRouting.AnyTransactional(setManyItems);

        // A non-transactional batch stages nothing, so there is nothing to check.
        if (checkOnly && !transactional)
            return [.. setManyItems.Select(static i => new KahunaSetKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.InvalidInput, Durability = i.Durability })];
        
        Dictionary<string, List<KahunaSetKeyValueRequestItem>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];

        // Items whose range is being moved, by reference: an item is one object of the batch.
        HashSet<KahunaSetKeyValueRequestItem>? quiesced = null;
        
        foreach (KahunaSetKeyValueRequestItem key in setManyItems)
        {
            if (string.IsNullOrEmpty(key.Key))
                return [new KahunaSetKeyValueResponseItem { Key = key.Key, Type = KeyValueResponseType.InvalidInput, Durability = key.Durability }];

            (int partitionId, long freshGeneration, bool isKeyRange, RangeDescriptor? descriptor) = routing.LocateRangeWithMode(key.Key);
            // Preserve a coordinator-supplied generation (non-zero = already redirected once);
            // on the first call resolve fresh and stamp it so the remote fence can check it.
            // Hash path: no generation fence, RoutedGeneration stays 0.
            if (isKeyRange && key.RoutedGeneration == 0)
                key.RoutedGeneration = freshGeneration;

            // A key in a range being moved cannot be written until the move lands. In an independent batch it
            // bounces alone: the rest of the batch may sit in ranges that are perfectly writable, and failing them
            // all would turn one split into a batch-wide retry. In a transactional batch it refuses the whole batch
            // before anything is sent, so nothing is staged for the caller to resend around.
            if (isKeyRange && routing.IsRangeQuiesced(descriptor, key.Key))
            {
                quiesced ??= new HashSet<KahunaSetKeyValueRequestItem>(ReferenceEqualityComparer.Instance);
                quiesced.Add(key);
                continue;
            }

            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return [.. setManyItems.Select(static i => new KahunaSetKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry, Durability = i.Durability })];

            routing.RecordBatchKeyRoute(key.Key, partitionId, leader, key.RoutedGeneration);

            if (acquisitionPlan.TryGetValue(leader, out List<KahunaSetKeyValueRequestItem>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }

        if (transactional && quiesced is not null)
        {
            List<KahunaSetKeyValueResponseItem> refused = new(setManyItems.Count);

            foreach (KahunaSetKeyValueRequestItem item in setManyItems)
                refused.Add(new()
                {
                    Key = item.Key,
                    Type = quiesced.Contains(item) ? KeyValueResponseType.MustRetry : KeyValueResponseType.NotApplied,
                    Durability = item.Durability
                });

            return refused;
        }

        if (!transactional || checkOnly || acquisitionPlan.Count <= 1)
        {
            List<KahunaSetKeyValueResponseItem> responses = await RunSetManyRound(acquisitionPlan, localNode, leaderByPartition, setManyItems.Count, checkOnly, cancellationToken);

            if (quiesced is not null)
            {
                foreach (KahunaSetKeyValueRequestItem item in quiesced)
                    responses.Add(new() { Key = item.Key, Type = KeyValueResponseType.MustRetry, Durability = item.Durability });
            }

            return responses;
        }

        // Several leaders: check everywhere, then stage everywhere.
        List<KahunaSetKeyValueResponseItem> checkRound = await RunSetManyRound(acquisitionPlan, localNode, leaderByPartition, setManyItems.Count, true, cancellationToken);

        foreach (KahunaSetKeyValueResponseItem response in checkRound)
        {
            if (response.Type != KeyValueResponseType.NotApplied)
                return checkRound;
        }

        if (manager.BeforeSetManyStageRoundForTesting is { } beforeStageRound)
            await beforeStageRound(setManyItems[0].TransactionId);

        return await RunSetManyRound(acquisitionPlan, localNode, leaderByPartition, setManyItems.Count, false, cancellationToken);
    }

    /// <summary>Sends every leader its group of the batch, in parallel, and collects the answers.</summary>
    private async Task<List<KahunaSetKeyValueResponseItem>> RunSetManyRound(
        Dictionary<string, List<KahunaSetKeyValueRequestItem>> acquisitionPlan,
        string localNode,
        Dictionary<int, string> leaderByPartition,
        int itemCount,
        bool checkOnly,
        CancellationToken cancellationToken)
    {
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<KahunaSetKeyValueResponseItem> responses = new(itemCount);

        // Requests to nodes are sent in parallel
        foreach ((string leader, List<KahunaSetKeyValueRequestItem> items) in acquisitionPlan)
            tasks.Add(TrySetManyNodeKeyValue(leader, localNode, leaderByPartition, items, checkOnly, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TrySetManyNodeKeyValue(
        string leader, 
        string localNode, 
        Dictionary<int, string> leaderByPartition,
        List<KahunaSetKeyValueRequestItem> items, 
        bool checkOnly,
        Lock lockSync, 
        List<KahunaSetKeyValueResponseItem> responses, 
        CancellationToken cancellationToken
    )
    {
        logger.LogSetManyKeyValueRedirect(items.Count, leader);
        
        if (leader == localNode)
        {
            // A transactional item stages in the actor without a proposal: the whole local group then needs the
            // quorum-confirmed gate (see the single-key set path). The check round takes the gate too, so a node
            // whose leadership no quorum confirmed refuses the batch before any other leader stages its items.
            if (LocatorRouting.AnyTransactional(items) && !await routing.ConfirmLeadershipForGroupMutation(leader, leaderByPartition, cancellationToken))
            {
                lock (lockSync)
                {
                    foreach (KahunaSetKeyValueRequestItem item in items)
                        responses.Add(new() { Key = item.Key, Type = KeyValueResponseType.MustRetry, Durability = item.Durability });
                }

                return;
            }

            List<KahunaSetKeyValueResponseItem> acquireResponses = await manager.SetManyNodeKeyValue(items, checkOnly);

            lock (lockSync)            
                responses.AddRange(acquireResponses);            

            return;
        }
            
        await interNodeCommunication.TrySetManyNodeKeyValue(leader, items, checkOnly, lockSync, responses, cancellationToken);
    }

    public async Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(
        List<KahunaDeleteKeyValueRequestItem> deleteManyItems,
        CancellationToken cancellationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();

        Dictionary<string, List<KahunaDeleteKeyValueRequestItem>> acquisitionPlan = [];
        List<KahunaDeleteKeyValueResponseItem> responses = new(deleteManyItems.Count);

        Dictionary<int, string> leaderByPartition = [];
        
        foreach (KahunaDeleteKeyValueRequestItem item in deleteManyItems)
        {
            if (string.IsNullOrEmpty(item.Key))
            {
                responses.Add(new()
                {
                    Key = item.Key,
                    Type = KeyValueResponseType.InvalidInput,
                    Durability = item.Durability
                });
                continue;
            }

            int partitionId = routing.RouteKey(item.Key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return [.. deleteManyItems.Select(static i => new KahunaDeleteKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry, Durability = i.Durability })];

            routing.RecordBatchKeyRoute(item.Key, partitionId, leader);

            if (acquisitionPlan.TryGetValue(leader, out List<KahunaDeleteKeyValueRequestItem>? list))
                list.Add(item);
            else
                acquisitionPlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);

        foreach ((string leader, List<KahunaDeleteKeyValueRequestItem> items) in acquisitionPlan)
            tasks.Add(TryDeleteManyNodeKeyValue(leader, localNode, leaderByPartition, items, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryDeleteManyNodeKeyValue(
        string leader,
        string localNode,
        Dictionary<int, string> leaderByPartition,
        List<KahunaDeleteKeyValueRequestItem> items,
        Lock lockSync,
        List<KahunaDeleteKeyValueResponseItem> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogDeleteManyKeyValueRedirect(items.Count, leader);

        if (leader == localNode)
        {
            // A transactional item stages in the actor without a proposal (see the set-many path).
            if (LocatorRouting.AnyTransactional(items) && !await routing.ConfirmLeadershipForGroupMutation(leader, leaderByPartition, cancellationToken))
            {
                lock (lockSync)
                {
                    foreach (KahunaDeleteKeyValueRequestItem item in items)
                        responses.Add(new() { Key = item.Key, Type = KeyValueResponseType.MustRetry, Durability = item.Durability });
                }

                return;
            }

            List<KahunaDeleteKeyValueResponseItem> acquireResponses = await manager.DeleteManyNodeKeyValue(items);

            lock (lockSync)
                responses.AddRange(acquireResponses);

            return;
        }

        await interNodeCommunication.TryDeleteManyNodeKeyValue(leader, items, lockSync, responses, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryDelete request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0, HLCTimestamp.Zero);
        
        int partitionId = routing.RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        // A transactional delete stages in the actor without a proposal (see the set path).
        bool stagesInActor = transactionId != HLCTimestamp.Zero;

        if (stagesInActor
                ? await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken)
                : await routing.AmILeaderIfHosted(partitionId, cancellationToken))
        {
            routing.RecordLocalKeyRoute(key, partitionId);
            return await manager.TryDeleteKeyValue(transactionId, key, durability);
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        if (leader == raft.GetLocalEndpoint())
        {
            if (stagesInActor)
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

            routing.RecordLocalKeyRoute(key, partitionId);
            return await manager.TryDeleteKeyValue(transactionId, key, durability);
        }

        logger.LogDeleteKeyValueRedirected(key, partitionId, leader);

        routing.RecordForwardedKeyRoute(key, partitionId, leader);

        return await interNodeCommunication.TryDeleteKeyValue(leader, transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExtend request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0, HLCTimestamp.Zero);
        
        int partitionId = routing.RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        // A transactional extend stages in the actor without a proposal (see the set path).
        bool stagesInActor = transactionId != HLCTimestamp.Zero;

        if (stagesInActor
                ? await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken)
                : await routing.AmILeaderIfHosted(partitionId, cancellationToken))
        {
            routing.RecordLocalKeyRoute(key, partitionId);
            return await manager.TryExtendKeyValue(transactionId, key, expiresMs, durability);
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        if (leader == raft.GetLocalEndpoint())
        {
            if (stagesInActor)
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

            routing.RecordLocalKeyRoute(key, partitionId);
            return await manager.TryExtendKeyValue(transactionId, key, expiresMs, durability);
        }

        logger.LogExtendKeyValueRedirected(key, partitionId, leader);

        routing.RecordForwardedKeyRoute(key, partitionId, leader);

        return await interNodeCommunication.TryExtendKeyValue(leader, transactionId, key, expiresMs, durability, cancellationToken);
    }
}
