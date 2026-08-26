
using Kommander;
using Kommander.Time;
using Kommander.Diagnostics;

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Locates the appropriate leader node for a given key and executes the corresponding key-value operations.
/// </summary>
internal sealed class KeyValueLocator
{
    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;
    
    private readonly IRaft raft;
    
    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly KeySpaceRegistry keySpaceRegistry;


    private readonly DataPartitionRouter dataPartitionRouter;

    private readonly ILogger<IKahuna> logger;

    public KeyValueLocator(
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        IRaft raft,
        IInterNodeCommunication interNodeCommunication,
        KeySpaceRegistry keySpaceRegistry,
        ILogger<IKahuna> logger
    )
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.interNodeCommunication = interNodeCommunication;
        this.keySpaceRegistry = keySpaceRegistry;
        this.dataPartitionRouter = new DataPartitionRouter(raft);
        this.logger = logger;
    }

    /// <summary>
    /// The key-order router: resolves <paramref name="key"/> to <c>(partitionId,
    /// generation)</c> through the range-descriptor map for key-range spaces, or falls back to the
    /// hash router (<c>GetPartitionKey</c>) for hash spaces. Both this router and the leader-side direct-write
    /// resolver (<c>BaseHandler.ResolveDirectWritePartition</c>) go through the same
    /// <see cref="RangeRouting.Locate"/> so the two routing sites cannot drift.
    /// </summary>
    public (int PartitionId, long Generation) LocateRange(string key) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key);

    /// <summary>Routes a key and reports its routing mode in the same single classification pass.</summary>
    private (int PartitionId, long Generation, bool IsKeyRange, RangeDescriptor? Descriptor) LocateRangeWithMode(string key) =>
        RangeRouting.LocateWithMode(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key);

    /// <summary>
    /// True when <paramref name="key"/> lands in the part of <paramref name="descriptor"/> that is
    /// currently refusing writes. Reading the clock costs a mint, so the deadline is tested for being
    /// set at all first — a range that is not being moved, which is every range almost all the time,
    /// never pays for it.
    /// </summary>
    private bool IsRangeQuiesced(RangeDescriptor? descriptor, string key) =>
        descriptor is not null
        && descriptor.QuiescedUntil != HLCTimestamp.Zero
        && descriptor.IsQuiescedAt(key, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()));

    /// <summary>Routes a per-key operation via <see cref="RangeRouting.Locate"/>.</summary>
    private int RouteKey(string key) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key).PartitionId;

    /// <summary>
    /// Routes a prefix/bucket operation. A bare prefix (no trailing <c>/</c>) is the key space
    /// itself; appending <c>/</c> lets <see cref="KeySpaceRegistry.ExtractKeySpace"/> strip it
    /// back to the prefix, consistent with how real keys look (<c>"t:r/0001"</c> → space <c>"t:r"</c>).
    /// </summary>
    private int RoutePrefixKey(string prefix) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, prefix + "/").PartitionId;

    /// <summary>
    /// Resolves the partition leader, mapping the retryable Raft resolution failures — a node that
    /// has not finished cluster initialization after a (re)join (<see cref="RaftNodeNotReadyException"/>),
    /// a leader still undecided within the election budget, or a failed partition restore — to a
    /// <see langword="null"/> result so callers answer <see cref="KeyValueResponseType.MustRetry"/>
    /// instead of leaking the exception to the client as an unhandled server error. No operation has
    /// been performed at this point, so telling the caller to retry (ideally on another node) is
    /// always safe. Mirrors <c>LockLocator</c>'s handling of the same condition.
    /// </summary>
    private async ValueTask<string?> TryWaitForLeader(int partitionId, CancellationToken cancellationToken)
    {
        try
        {
            string? leader = await raft.TryResolveLeader(partitionId, cancellationToken);
            if (leader is null)
                logger.LogKeyValueLeaderNotResolved(partitionId, "Partition is not hosted on this node");

            return leader;
        }
        catch (RaftException ex)
        {
            logger.LogKeyValueLeaderNotResolved(partitionId, ex.Message);

            return null;
        }
    }

    /// <summary>
    /// Memoizing variant of <see cref="TryWaitForLeader(int, CancellationToken)"/> for batch
    /// planning loops: resolves each distinct partition's leader once per call instead of once
    /// per key. The memo must stay call-local — leader identity is inherently racy, but it must
    /// never be cached across requests. Failed resolutions are not memoized so a later key on
    /// the same partition can still observe a leader elected mid-loop.
    /// </summary>
    private async ValueTask<string?> TryWaitForLeader(
        int partitionId,
        Dictionary<int, string> leaderByPartition,
        CancellationToken cancellationToken
    )
    {
        if (leaderByPartition.TryGetValue(partitionId, out string? cached))
            return cached;

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is not null)
            leaderByPartition[partitionId] = leader;

        return leader;
    }

    /// <summary>
    /// Gates an authoritative local read on a quorum-confirmed leadership check (Raft read-index).
    /// Local belief (<see cref="IRaft.AmILeader"/>) is not enough for reads: a minority-partitioned
    /// leader keeps believing it leads until it receives a higher-term message, and a belief-gated
    /// read serves stale state as a successful response. Writes don't need this — replication itself
    /// fails on a deposed leader — so read paths answer <see cref="KeyValueResponseType.MustRetry"/>
    /// whenever this returns <see langword="false"/>, matching the write path's behavior.
    /// </summary>
    private ValueTask<bool> ConfirmLeadershipForRead(int partitionId, CancellationToken cancellationToken) =>
        raft.ConfirmLeadershipIfHosted(partitionId, cancellationToken);

    /// <summary>
    /// <see cref="ConfirmLeadershipForRead"/> for a locally-led key group: confirms every distinct
    /// partition the group's keys route to. A group is served locally only when this node believes
    /// it leads all of them, so all must confirm before any key in the group is answered from
    /// local state. The planning loop that grouped the keys already resolved each partition's
    /// leader into <paramref name="leaderByPartition"/>, so the group's partitions are recovered
    /// by filtering that map instead of re-routing every key.
    /// </summary>
    private async ValueTask<bool> ConfirmLeadershipForGroupRead(
        string leader,
        Dictionary<int, string> leaderByPartition,
        CancellationToken cancellationToken
    )
    {
        foreach ((int partitionId, string partitionLeader) in leaderByPartition)
        {
            if (partitionLeader != leader)
                continue;

            if (!await ConfirmLeadershipForRead(partitionId, cancellationToken))
                return false;
        }

        return true;
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
        (int partitionId, long freshGeneration, bool isKeyRange, RangeDescriptor? descriptor) = LocateRangeWithMode(key);
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
            if (IsRangeQuiesced(descriptor, key))
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        }

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        if (await raft.AmILeaderIfHosted(partitionId, cancellationToken))
        {
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

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        if (leader == raft.GetLocalEndpoint())
            return await manager.TrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, routedGeneration);

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
    ///
    /// </summary>
    /// <param name="setManyItems"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>    
    public async Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
        List<KahunaSetKeyValueRequestItem> setManyItems, 
        CancellationToken cancellationToken
    )
    {                
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<KahunaSetKeyValueRequestItem>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];

        List<KahunaSetKeyValueResponseItem>? quiesced = null;
        
        foreach (KahunaSetKeyValueRequestItem key in setManyItems)
        {
            if (string.IsNullOrEmpty(key.Key))
                return [new KahunaSetKeyValueResponseItem { Key = key.Key, Type = KeyValueResponseType.InvalidInput, Durability = key.Durability }];

            (int partitionId, long freshGeneration, bool isKeyRange, RangeDescriptor? descriptor) = LocateRangeWithMode(key.Key);
            // Preserve a coordinator-supplied generation (non-zero = already redirected once);
            // on the first call resolve fresh and stamp it so the remote fence can check it.
            // Hash path: no generation fence, RoutedGeneration stays 0.
            if (isKeyRange && key.RoutedGeneration == 0)
                key.RoutedGeneration = freshGeneration;

            // One key in a range being moved bounces only that key: the rest of the batch may sit in
            // ranges that are perfectly writable, and failing them all would turn one split into a
            // batch-wide retry.
            if (isKeyRange && IsRangeQuiesced(descriptor, key.Key))
            {
                quiesced ??= [];
                quiesced.Add(new KahunaSetKeyValueResponseItem
                {
                    Key = key.Key, Type = KeyValueResponseType.MustRetry, Durability = key.Durability
                });
                continue;
            }

            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return [.. setManyItems.Select(static i => new KahunaSetKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry, Durability = i.Durability })];

            if (acquisitionPlan.TryGetValue(leader, out List<KahunaSetKeyValueRequestItem>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<KahunaSetKeyValueResponseItem> responses = new(setManyItems.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<KahunaSetKeyValueRequestItem> items) in acquisitionPlan)
            tasks.Add(TrySetManyNodeKeyValue(leader, localNode, items, lockSync, responses, cancellationToken));
        
        await Task.WhenAll(tasks);

        if (quiesced is not null)
            responses.AddRange(quiesced);

        return responses;
    }

    private async Task TrySetManyNodeKeyValue(
        string leader, 
        string localNode, 
        List<KahunaSetKeyValueRequestItem> items, 
        Lock lockSync, 
        List<KahunaSetKeyValueResponseItem> responses, 
        CancellationToken cancellationToken
    )
    {
        logger.LogSetManyKeyValueRedirect(items.Count, leader);
        
        if (leader == localNode)
        {
            List<KahunaSetKeyValueResponseItem> acquireResponses = await manager.SetManyNodeKeyValue(items);

            lock (lockSync)            
                responses.AddRange(acquireResponses);            

            return;
        }
            
        await interNodeCommunication.TrySetManyNodeKeyValue(leader, items, lockSync, responses, cancellationToken);
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

            int partitionId = RouteKey(item.Key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return [.. deleteManyItems.Select(static i => new KahunaDeleteKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry, Durability = i.Durability })];

            if (acquisitionPlan.TryGetValue(leader, out List<KahunaDeleteKeyValueRequestItem>? list))
                list.Add(item);
            else
                acquisitionPlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);

        foreach ((string leader, List<KahunaDeleteKeyValueRequestItem> items) in acquisitionPlan)
            tasks.Add(TryDeleteManyNodeKeyValue(leader, localNode, items, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryDeleteManyNodeKeyValue(
        string leader,
        string localNode,
        List<KahunaDeleteKeyValueRequestItem> items,
        Lock lockSync,
        List<KahunaDeleteKeyValueResponseItem> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogDeleteManyKeyValueRedirect(items.Count, leader);

        if (leader == localNode)
        {
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
        
        int partitionId = RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        if (await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.TryDeleteKeyValue(transactionId, key, durability);
            
        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        if (leader == raft.GetLocalEndpoint())
            return await manager.TryDeleteKeyValue(transactionId, key, durability);

        logger.LogDeleteKeyValueRedirected(key, partitionId, leader);
        
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
        
        int partitionId = RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        if (await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.TryExtendKeyValue(transactionId, key, expiresMs, durability);
            
        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        if (leader == raft.GetLocalEndpoint())
            return await manager.TryExtendKeyValue(transactionId, key, expiresMs, durability);

        logger.LogExtendKeyValueRedirected(key, partitionId, leader);

        return await interNodeCommunication.TryExtendKeyValue(leader, transactionId, key, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);

        int partitionId = RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, null);

        if (await ConfirmLeadershipForRead(partitionId, cancellationToken))
            return await manager.TryGetValue(transactionId, key, revision, readTimestamp, durability);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, null);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        (KeyValueResponseType, ReadOnlyKeyValueEntry?) response = await interNodeCommunication.TryGetValue(leader, transactionId, key, revision, readTimestamp, durability, cancellationToken);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogGetKeyValueRedirected(key, partitionId, leader, stopwatch.GetElapsedMilliseconds());

        return response;
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExistsValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);

        int partitionId = RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, null);

        if (await ConfirmLeadershipForRead(partitionId, cancellationToken))
            return await manager.TryExistsValue(transactionId, key, revision, readTimestamp, durability);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, null);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        logger.LogExistsKeyValueRedirect(key, partitionId, leader);

        return await interNodeCommunication.TryExistsValue(leader, transactionId, key, revision, readTimestamp, durability, cancellationToken);
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        if (keys.Count == 0)
            return [(KeyValueResponseType.InvalidInput, string.Empty, KeyValueDurability.Persistent, null)];

        string localNode = raft.GetLocalEndpoint();
        Dictionary<string, List<(string key, long revision, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, long revision, KeyValueDurability durability) item in keys)
        {
            if (string.IsNullOrEmpty(item.key))
                return [(KeyValueResponseType.InvalidInput, item.key, item.durability, null)];

            int partitionId = RouteKey(item.key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, k.durability, (ReadOnlyKeyValueEntry?)null))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, long revision, KeyValueDurability durability)>? list))
                list.Add(item);
            else
                acquisitionPlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = new(keys.Count);

        foreach ((string leader, List<(string key, long revision, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryExistsManyNodeValues(transactionId, readTimestamp, leader, localNode, leaderByPartition, xkeys, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    /// <summary>
    /// Staged-base variant of <see cref="LocateAndTryExistsManyValues"/> for the commit-time
    /// write-side compare-and-set: keys whose partition this node believes it leads are answered
    /// from local committed state <b>without</b> a read-index confirmation round or a per-key
    /// leader wait; keys this node does not believe it leads fall back to the ordinary confirmed
    /// path. On a leader under commit load this removes two partition-executor round-trips from
    /// every durable finalize, which otherwise serialize all committing sessions through one actor.
    ///
    /// <para><b>Why skipping the read-index is safe here and for no other caller.</b> The results
    /// only decide whether the finalizer drives its durable decision proposal. A
    /// minority-partitioned leader that has not heard of its own deposition can serve stale
    /// committed state from this path — but the decision proposal it would then make cannot
    /// replicate on that same deposed node, so a stale "base unchanged" can never become a durably
    /// wrong outcome: it becomes a failed proposal and a retryable answer. A stale mismatch merely
    /// aborts a transaction whose proposal was going to fail anyway. Ordinary reads return state to
    /// clients as an authoritative success and MUST keep the read-index gate
    /// (<see cref="ConfirmLeadershipForRead"/>).</para>
    /// </summary>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValuesUnconfirmed(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        if (keys.Count == 0)
            return [(KeyValueResponseType.InvalidInput, string.Empty, KeyValueDurability.Persistent, null)];

        List<(string key, long revision, KeyValueDurability durability)>? localKeys = null;
        List<(string key, long revision, KeyValueDurability durability)>? fallbackKeys = null;

        foreach ((string key, long revision, KeyValueDurability durability) item in keys)
        {
            if (string.IsNullOrEmpty(item.key))
                return [(KeyValueResponseType.InvalidInput, item.key, item.durability, null)];

            // Belief check only (no ack round): its fast path is a field comparison. A false
            // negative just routes the key through the confirmed path, which is always correct.
            if (await raft.AmILeaderQuickIfHosted(RouteKey(item.key)))
                (localKeys ??= new(keys.Count)).Add(item);
            else
                (fallbackKeys ??= []).Add(item);
        }

        if (fallbackKeys is null)
            return await manager.TryExistsManyValues(transactionId, readTimestamp, localKeys!);

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = new(keys.Count);

        if (localKeys is not null)
            responses.AddRange(await manager.TryExistsManyValues(transactionId, readTimestamp, localKeys));

        responses.AddRange(await LocateAndTryExistsManyValues(transactionId, readTimestamp, fallbackKeys, cancellationToken));

        return responses;
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        if (keys.Count == 0)
            return [(KeyValueResponseType.InvalidInput, string.Empty, KeyValueDurability.Persistent, null)];

        string localNode = raft.GetLocalEndpoint();
        Dictionary<string, List<(string key, long revision, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, long revision, KeyValueDurability durability) item in keys)
        {
            if (string.IsNullOrEmpty(item.key))
                return [(KeyValueResponseType.InvalidInput, item.key, item.durability, null)];

            int partitionId = RouteKey(item.key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, k.durability, (ReadOnlyKeyValueEntry?)null))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, long revision, KeyValueDurability durability)>? list))
                list.Add(item);
            else
                acquisitionPlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = new(keys.Count);

        foreach ((string leader, List<(string key, long revision, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryGetManyNodeValues(transactionId, readTimestamp, leader, localNode, leaderByPartition, xkeys, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryExistsManyNodeValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string leader,
        string localNode,
        Dictionary<int, string> leaderByPartition,
        List<(string key, long revision, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogExistsManyKeyValueRedirect(xkeys.Count, leader);

        if (leader == localNode)
        {
            if (!await ConfirmLeadershipForGroupRead(leader, leaderByPartition, cancellationToken))
            {
                lock (lockSync)
                {
                    foreach ((string key, _, KeyValueDurability durability) in xkeys)
                        responses.Add((KeyValueResponseType.MustRetry, key, durability, null));
                }

                return;
            }

            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> readResponses =
                await manager.TryExistsManyValues(transactionId, readTimestamp, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) item in readResponses)
                    responses.Add((item.type, item.key, item.durability, item.entry));
            }

            return;
        }

        await interNodeCommunication.TryExistsManyNodeValues(leader, transactionId, readTimestamp, xkeys, lockSync, responses, cancellationToken);
    }

    private async Task TryGetManyNodeValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string leader,
        string localNode,
        Dictionary<int, string> leaderByPartition,
        List<(string key, long revision, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogGetManyKeyValueRedirect(xkeys.Count, leader);

        if (leader == localNode)
        {
            if (!await ConfirmLeadershipForGroupRead(leader, leaderByPartition, cancellationToken))
            {
                lock (lockSync)
                {
                    foreach ((string key, _, KeyValueDurability durability) in xkeys)
                        responses.Add((KeyValueResponseType.MustRetry, key, durability, null));
                }

                return;
            }

            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> readResponses =
                await manager.TryGetManyValues(transactionId, readTimestamp, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) item in readResponses)
                    responses.Add((item.type, item.key, item.durability, item.entry));
            }

            return;
        }

        await interNodeCommunication.TryGetManyNodeValues(leader, transactionId, readTimestamp, xkeys, lockSync, responses, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and checks whether a live write intent from another
    /// transaction exists. Used at commit time by optimistic transactions as a write-skew guard.
    /// Returns Aborted when a conflicting write intent is found; DoesNotExist otherwise.
    /// </summary>
    public async Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return KeyValueResponseType.InvalidInput;

        int partitionId = RouteKey(key);

        if (!raft.Joined)
            return KeyValueResponseType.MustRetry;

        if (await ConfirmLeadershipForRead(partitionId, cancellationToken))
            return await manager.TryCheckWriteIntentValue(transactionId, key, durability);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return KeyValueResponseType.MustRetry;
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogCheckWriteIntentRedirect(key, partitionId, leader);

        return await interNodeCommunication.TryCheckWriteIntentValue(leader, transactionId, key, durability, cancellationToken);
    }

    /// <summary>
    /// Probes several keys for concurrent write intents, grouping them by the node that leads each key's
    /// partition so the whole set costs one call per owning node instead of one per key. Used at commit time by
    /// optimistic transactions as a write-skew guard over the read set.
    ///
    /// Returns exactly one result per requested key, in no particular order — callers correlate by key and
    /// durability. The per-key coverage is part of the contract: a caller must never have to treat a key it asked
    /// about but did not hear back on as "no conflict", so every rejection path fills in the affected keys rather
    /// than dropping them. A remote group that faults propagates its exception, exactly as a single-key probe
    /// does, so a failed probe is never silently read as a pass.
    ///
    /// Only the routing is grouped. Keys the local node owns are still probed one actor request at a time
    /// (see <see cref="KeyValuesManager.TryCheckManyWriteIntentValues"/>), which measurement showed is not the
    /// cost worth removing — the round trip per remote key is.
    /// </summary>
    public async Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> LocateAndTryCheckManyWriteIntents(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken cancellationToken
    )
    {
        if (keys.Count == 0)
            return [];

        if (!raft.Joined)
            return BuildManyWriteIntentRejection(keys, KeyValueResponseType.MustRetry);

        string localNode = raft.GetLocalEndpoint();
        Dictionary<string, List<KeyValueConflictProbe>> probePlan = [];
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses = new(keys.Count);

        Dictionary<int, string> leaderByPartition = [];

        foreach (KeyValueConflictProbe item in keys)
        {
            // A malformed key is reported against that key alone; the rest of the set is still probed, so one
            // bad entry cannot quietly cancel the write-skew guard for every other read dependency.
            if (string.IsNullOrEmpty(item.Key))
            {
                responses.Add((KeyValueResponseType.InvalidInput, item.Key, item.Durability));
                continue;
            }

            int partitionId = RouteKey(item.Key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return BuildManyWriteIntentRejection(keys, KeyValueResponseType.MustRetry);

            if (probePlan.TryGetValue(leader, out List<KeyValueConflictProbe>? list))
                list.Add(item);
            else
                probePlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(probePlan.Count);

        foreach ((string leader, List<KeyValueConflictProbe> xkeys) in probePlan)
            tasks.Add(TryCheckManyWriteIntentsOnNode(transactionId, leader, localNode, leaderByPartition, xkeys, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryCheckManyWriteIntentsOnNode(
        HLCTimestamp transactionId,
        string leader,
        string localNode,
        Dictionary<int, string> leaderByPartition,
        List<KeyValueConflictProbe> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogCheckManyWriteIntentsRedirect(xkeys.Count, leader);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> nodeResponses;

        if (leader == localNode)
            nodeResponses = await ConfirmLeadershipForGroupRead(leader, leaderByPartition, cancellationToken)
                ? await manager.TryCheckManyWriteIntentValues(transactionId, xkeys)
                : BuildManyWriteIntentRejection(xkeys, KeyValueResponseType.MustRetry);
        else
            nodeResponses = await interNodeCommunication.TryCheckManyWriteIntents(leader, transactionId, xkeys, cancellationToken);

        lock (lockSync)
        {
            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) item in nodeResponses)
                responses.Add(item);
        }
    }

    /// <summary>
    /// Builds one result per requested key carrying the same rejection type, for the paths that never reach a
    /// leader. Callers correlate results by key, so a rejection has to name every key it covers.
    /// </summary>
    private static List<(KeyValueResponseType type, string key, KeyValueDurability durability)> BuildManyWriteIntentRejection(
        List<KeyValueConflictProbe> keys,
        KeyValueResponseType type
    )
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> rejected = new(keys.Count);

        foreach (KeyValueConflictProbe probe in keys)
            rejected.Add((type, probe.Key, probe.Durability));

        return rejected;
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, key, durability, HLCTimestamp.Zero);

        int partitionId = RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);

        if (await raft.AmILeaderIfHosted(partitionId, cancelationToken))
            return await manager.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);

        string? leader = await TryWaitForLeader(partitionId, cancelationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);
        if (leader == raft.GetLocalEndpoint())
            return await manager.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);

        logger.LogAcquireLockKeyValueRedirected(key, partitionId, leader);

        return await interNodeCommunication.TryAcquireExclusiveLock(leader, transactionId, key, expiresMs, durability, cancelationToken);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefixKey))
            return KeyValueResponseType.InvalidInput;

        if (!RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixKey))
        {
            // Deprecated by design: a prefix lock is a single-partition bucket lock and cannot cover
            // a key space that has been key-range split across partitions. Callers must use the
            // per-range exclusive range lock instead (TryAcquireExclusiveRangeLock).
            logger.LogWarning("ACQUIRE-PREFIX-LOCK: prefix {Prefix} is on a key-range-split space — prefix locks are unsupported there; use a range lock", prefixKey);
            return KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace;
        }

        int partitionId = RoutePrefixKey(prefixKey);

        if (!raft.Joined)
            return KeyValueResponseType.MustRetry;

        if (await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.TryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability);
            
        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return KeyValueResponseType.MustRetry;
        if (leader == raft.GetLocalEndpoint())
            return await manager.TryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability);

        logger.LogAcquirePrefixLockKeyValueRedirected(prefixKey, partitionId, leader);
        
        return await interNodeCommunication.TryAcquireExclusivePrefixLock(leader, transactionId, prefixKey, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryAcquireManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys,
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();

        Dictionary<string, List<(string key, int expiresMs, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, int expiresMs, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, key.durability, HLCTimestamp.Zero)];

            int partitionId = RouteKey(key.key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, int expiresMs, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> responses = new(keys.Count);

        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, int expiresMs, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryAcquireNodeExclusiveLocks(transactionId, leader, localNode, xkeys, lockSync, responses, cancelationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryAcquireNodeExclusiveLocks(
        HLCTimestamp transactionId,
        string leader,
        string localNode,
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogAcquireManyLocksKeyValueRedirect(xkeys.Count, leader);

        if (leader == localNode)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> acquireResponses = await manager.TryAcquireManyExclusiveLocks(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder) item in acquireResponses)
                    responses.Add((item.type, item.key, item.durability, item.holder));
            }

            return;
        }

        await interNodeCommunication.TryAcquireNodeExclusiveLocks(leader, transactionId, xkeys, lockSync, responses, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, key);
        
        int partitionId = RouteKey(key);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.TryReleaseExclusiveLock(transactionId, key, durability);
            
        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, key);
        
        logger.LogReleaseLockKeyValueRedirected(key, partitionId, leader);
        
        return await interNodeCommunication.TryReleaseExclusiveLock(leader, transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefixKey))
            return KeyValueResponseType.InvalidInput;

        if (!RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixKey))
        {
            // Deprecated by design (see acquire): prefix locks are unsupported on key-range-split
            // spaces. A release on such a space can only be a caller error — there is no prefix lock
            // to release — so surface the typed response rather than a generic error.
            logger.LogWarning("RELEASE-PREFIX-LOCK: prefix {Prefix} is on a key-range-split space — prefix locks are unsupported there; use a range lock", prefixKey);
            return KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace;
        }

        int partitionId = RoutePrefixKey(prefixKey);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.TryReleaseExclusivePrefixLock(transactionId, prefixKey, durability);
            
        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;
        
        logger.LogReleasePrefixLockKeyValueRedirected(prefixKey, partitionId, leader);
        
        return await interNodeCommunication.TryReleaseExclusivePrefixLock(leader, transactionId, prefixKey, durability, cancellationToken);
    }
    
    /// <summary>
    /// Acquires an exclusive range lock covering <c>[startKey, endKey)</c> within <paramref name="prefix"/>.
    /// Fan-outs to one sub-lock per intersecting <see cref="RangeDescriptor"/>; rolls back partial
    /// acquisitions on failure.
    ///
    /// <para><b>Local-snapshot fence (F2).</b> After acquiring all sub-locks the method re-reads
    /// <see cref="RangeMapStore.Current"/> and compares descriptor sets via
    /// <see cref="DescriptorSetStable"/>. If any descriptor was added, removed, or bumped (a split
    /// or merge committed in the acquire window <em>and replicated to this node</em>), all sub-locks
    /// are released and <c>MustRetry</c> is returned so the caller re-routes on the fresh map.</para>
    ///
    /// <para><b>Limitation — local-node visibility only.</b> The fence compares two consecutive
    /// reads of this node's local descriptor map. A split that committed on the meta-partition leader
    /// but has not yet replicated here is invisible to both reads: the lock lands on the pre-split
    /// partition and a writer on an ahead node that already sees the new partition is not blocked.
    /// The write-path generation fence (carried on every <c>TrySet</c> / 2PC-prepare RPC) remains
    /// the primary serializability guard; this fence is a best-effort defence for the
    /// frequently-consistent case. Fully closing the cross-node skew window would require the lock
    /// to carry and verify a descriptor generation end-to-end against the writer's view.</para>
    /// </summary>
    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken
    ) => LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, null, cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) => LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, null, cancellationToken);

    internal Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        Func<Task>? afterSnapshot,
        CancellationToken cancellationToken
    ) => LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, afterSnapshot, cancellationToken);

    internal async Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        Func<Task>? afterSnapshot,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefix))
            return (KeyValueResponseType.InvalidInput, HLCTimestamp.Zero);

        ArraySegment<RangeDescriptor> descriptors =
            manager.RangeMapStore.Current.FindIntersecting(prefix, startKey, endKey);

        if (afterSnapshot != null)
            await afterSnapshot();

        if (descriptors.Count == 0)
        {
            // Hash-space or range space with no descriptors yet: single-partition path.
            // Hash spaces never split, so no generation fence is needed.
            int hashPartitionId = RoutePrefixKey(prefix);
            return await AcquireRangeLockOnPartition(transactionId, hashPartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
        }

        if (descriptors.Count == 1)
        {
            (KeyValueResponseType result, HLCTimestamp holder) = await AcquireRangeLockOnPartition(
                transactionId, descriptors[0].PartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);

            if (result != KeyValueResponseType.Locked)
                return (result, holder);

            // Generation fence: a split that committed after FindIntersecting but before the
            // sub-lock RPC would leave P' un-locked. Re-check the map; if the descriptor set
            // changed, roll back and signal the caller to re-resolve.
            if (!DescriptorSetStable(descriptors, manager.RangeMapStore.Current.FindIntersecting(prefix, startKey, endKey)))
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(transactionId, descriptors[0].PartitionId, prefix,
                    startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: fence rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, descriptors[0].PartitionId, rel);

                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
            }

            return (KeyValueResponseType.Locked, HLCTimestamp.Zero);
        }

        // Multi-descriptor: per-range sub-locks with roll-back on partial failure.
        var acquired = new List<(int PartitionId, string? ClampStart, bool ClampStartIncl, string? ClampEnd, bool ClampEndIncl)>(descriptors.Count);

        foreach (RangeDescriptor desc in descriptors)
        {
            (string? cs, bool csI, string? ce, bool ceI) = ClipRange(
                startKey, startInclusive, endKey, endInclusive, desc);

            (KeyValueResponseType result, HLCTimestamp holder) = await AcquireRangeLockOnPartition(
                transactionId, desc.PartitionId, prefix, cs, csI, ce, ceI, expiresMs, durability, mode, cancellationToken);

            if (result == KeyValueResponseType.Locked)
            {
                acquired.Add((desc.PartitionId, cs, csI, ce, ceI));
                continue;
            }

            foreach ((int pid, string? rcs, bool rcsi, string? rce, bool rcei) in acquired)
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                    transactionId, pid, prefix, rcs, rcsi, rce, rcei, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: partial-acquire rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, pid, rel);
            }

            return (result, holder);
        }

        // Generation fence: re-check after all sub-locks are held. If the map changed
        // (split committed in the acquire window), roll everything back and MustRetry.
        if (!DescriptorSetStable(descriptors, manager.RangeMapStore.Current.FindIntersecting(prefix, startKey, endKey)))
        {
            logger.LogAcquireRangeLockDescriptorChanged(prefix);

            foreach ((int pid, string? rcs, bool rcsi, string? rce, bool rcei) in acquired)
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                    transactionId, pid, prefix, rcs, rcsi, rce, rcei, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: fence rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, pid, rel);
            }

            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
        }

        return (KeyValueResponseType.Locked, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Returns true when both snapshots cover the same descriptors in the same order with the
    /// same generations. Used as the acquire-time generation fence: if false, a split or merge
    /// committed <em>and replicated to this node</em> in the window between
    /// <c>FindIntersecting</c> and the sub-lock RPCs, and the caller must roll back acquired
    /// sub-locks and retry. Splits that have not yet replicated here are not detected — see the
    /// <c>LocateAndTryAcquireExclusiveRangeLock</c> doc for the full local-snapshot caveat.
    /// </summary>
    private static bool DescriptorSetStable(
        ArraySegment<RangeDescriptor> before,
        ArraySegment<RangeDescriptor> after)
    {
        if (before.Count != after.Count)
            return false;

        for (int i = 0; i < before.Count; i++)
        {
            if (before[i].PartitionId != after[i].PartitionId ||
                before[i].Generation  != after[i].Generation)
                return false;
        }

        return true;
    }

    public async Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefix))
            return KeyValueResponseType.InvalidInput;

        ArraySegment<RangeDescriptor> descriptors =
            manager.RangeMapStore.Current.FindIntersecting(prefix, startKey, endKey);

        if (descriptors.Count == 0)
        {
            int hashPartitionId = RoutePrefixKey(prefix);
            return await ReleaseRangeLockOnPartition(transactionId, hashPartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        }

        if (descriptors.Count == 1)
        {
            return await ReleaseRangeLockOnPartition(transactionId, descriptors[0].PartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        }

        // Release all sub-locks even if one fails (best-effort). Return Unlocked only when
        // every descriptor released successfully; return the first non-Unlocked result otherwise
        // so the caller knows at least one sub-lock was not cleaned up.
        KeyValueResponseType firstFailure = KeyValueResponseType.Unlocked;
        foreach (RangeDescriptor desc in descriptors)
        {
            (string? cs, bool csI, string? ce, bool ceI) = ClipRange(
                startKey, startInclusive, endKey, endInclusive, desc);

            KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                transactionId, desc.PartitionId, prefix, cs, csI, ce, ceI, durability, cancellationToken);

            if (rel != KeyValueResponseType.Unlocked)
            {
                logger.LogWarning("RELEASE-RANGE-LOCK {Prefix} P{Pid}: release returned {Status} — sub-lock leaks until TTL",
                    prefix, desc.PartitionId, rel);

                if (firstFailure == KeyValueResponseType.Unlocked)
                    firstFailure = rel;
            }
        }

        return firstFailure;
    }

    private async Task<(KeyValueResponseType, HLCTimestamp)> AcquireRangeLockOnPartition(
        HLCTimestamp transactionId,
        int partitionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

        logger.LogAcquireRangeLockKeyValueRedirected(prefix, partitionId, leader);

        return await interNodeCommunication.TryAcquireRangeLock(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
    }

    private async Task<KeyValueResponseType> ReleaseRangeLockOnPartition(
        HLCTimestamp transactionId,
        int partitionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.TryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogReleaseRangeLockKeyValueRedirected(prefix, partitionId, leader);

        return await interNodeCommunication.TryReleaseExclusiveRangeLock(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryReleaseManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, key.durability)];

            int partitionId = RouteKey(key.key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, k.durability))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryReleaseNodeExclusiveLocks(transactionId, leader, localNode, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }
    
    private async Task TryReleaseNodeExclusiveLocks(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        List<(string key, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogReleaseManyLocksKeyValueRedirect(xkeys.Count, leader);
        
        if (leader == localNode)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability)> acquireResponses = await manager.TryReleaseManyExclusiveLocks(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) item in acquireResponses)
                    responses.Add((item.type, item.key, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryReleaseNodeExclusiveLocks(leader, transactionId, xkeys, lockSync, responses, cancelationToken);
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

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancelationToken))
            return await manager.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration, recordAnchorKey);

        string? leader = await TryWaitForLeader(partitionId, cancelationToken);
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

            int partitionId = RouteKey(key.key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
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
            tasks.Add(TryPrepareNodeMutations(transactionId, commitId, leader, localNode, xkeys, lockSync, responses, cancelationToken, recordAnchorKey));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryPrepareNodeMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string leader, 
        string localNode, 
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
        
        int partitionId = RouteKey(key);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancelationToken))
            return await manager.TryCommitMutations(transactionId, key, ticketId, durability);
            
        string? leader = await TryWaitForLeader(partitionId, cancelationToken);
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

            int partitionId = RouteKey(key.key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
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
            tasks.Add(TryCommitManyMutations(transactionId, leader, localNode, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryCommitManyMutations(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogCommitManyKeyValueRedirect(xkeys.Count, leader);
        
        if (leader == localNode)
        {
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
        
        int partitionId = RouteKey(key);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancelationToken))
            return await manager.TryRollbackMutations(transactionId, key, ticketId, durability);

        string? leader = await TryWaitForLeader(partitionId, cancelationToken);
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

            int partitionId = RouteKey(key.key);
            string? leader = await TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
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
            tasks.Add(TryRollbackManyMutations(transactionId, leader, localNode, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryRollbackManyMutations(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogRollbackManyKeyValueRedirect(xkeys.Count, leader);
        
        if (leader == localNode)
        {
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

    /// <summary>
    /// Locates the appropriate node for the specified key prefix and retrieves the corresponding key-value items.
    /// For unsplit spaces routes to the single partition leader. For split key-range spaces fans out across
    /// all descriptors in parallel, pages through each with <see cref="QueryDescriptorRange"/>, and
    /// returns the concatenated result.
    ///
    /// <para>
    /// <b>Fan-out model:</b> all descriptors are queried concurrently via <c>Task.WhenAll</c>,
    /// bounded by <c>MaxParallelBucketDescriptors = 8</c>. One <see cref="QueryDescriptorRange"/> task
    /// is issued per descriptor; the concurrency limit ensures the fan-out does not stampede the cluster
    /// with descriptors at the boundaries of that limit.
    /// </para>
    ///
    /// <para>
    /// <b>Leader coalescing — transport-delegated, locator-level deferred.</b> When several descriptors
    /// share the same leader endpoint, this method still issues one <see cref="QueryDescriptorRange"/>
    /// call per descriptor (not one per unique leader). On the gRPC transport, concurrent calls to the
    /// same endpoint are automatically multiplexed onto a single streaming connection by
    /// <c>GrpcServerBatcher</c>, so the wire cost is already one stream per leader — but the locator
    /// does not merge the descriptor ranges before dispatching. True locator-level grouping (resolve
    /// leaders → merge adjacent same-leader ranges → one <c>GetByRange</c> per leader) would further
    /// reduce RPC message count on the in-memory and gRPC transports alike; that optimisation is
    /// deferred for now.
    /// </para>
    ///
    /// <para>
    /// Full materialisation: the entire bucket is buffered before returning. Callers needing
    /// bounded-memory streaming should use <see cref="LocateAndGetByRange"/> instead.
    /// </para>
    /// </summary>
    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(
        HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability,
        CancellationToken cancellationToken) =>
        LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, null, null, cancellationToken);

    /// <summary>
    /// Internal overload with test hooks.
    /// <paramref name="beforeQuery"/> is called (with the descriptor index) after the semaphore is
    /// acquired but before the first page RPC for that descriptor — lets tests gate all tasks to prove
    /// concurrency.
    /// <paramref name="afterDescriptor"/> is called after all pages for a descriptor are collected —
    /// lets tests inject a mid-fan-out split for <c>Bucket_SplitMidScan_NoDupNoMissing</c>.
    /// </summary>
    internal async Task<KeyValueGetByBucketResult> LocateAndGetByBucket(
        HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability,
        Func<int, Task>? beforeQuery,
        Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(prefixedKey))
            return new(KeyValueResponseType.Errored, []);

        // Fast path: unsplit (or hash/schema-log) space — single partition, no fan-out overhead.
        if (RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixedKey))
        {
            int singlePartitionId = RoutePrefixKey(prefixedKey);

            if (!raft.Joined || await ConfirmLeadershipForRead(singlePartitionId, cancellationToken))
                return await manager.GetByBucket(transactionId, prefixedKey, readTimestamp, durability);

            string? singleLeader = await TryWaitForLeader(singlePartitionId, cancellationToken);
            if (singleLeader is null || singleLeader == raft.GetLocalEndpoint())
                return new(KeyValueResponseType.MustRetry, []);

            logger.LogGetPrefixKeyValueRedirected(prefixedKey, singlePartitionId, singleLeader);

            return await interNodeCommunication.GetByBucket(singleLeader, transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
        }

        // Multi-range path (parallel): key-range space is split; fan out to all descriptors
        // concurrently. Snapshot the map once — safe because orphan retention + MVCC means the source
        // partition still answers snapshot reads for stale entries after a cutover.
        // The scan prefix is already the key space, so extracting the portion before a trailing
        // separator would just return it unchanged.
        string keySpace = prefixedKey;
        IReadOnlyList<RangeDescriptor> descriptors = manager.RangeMapStore.Current.FindAll(keySpace);

        if (descriptors.Count == 0)
            return new(KeyValueResponseType.Get, []);

        const int bucketPageSize = 512;
        const int maxParallelDescriptors = 8;

        using var sem = new SemaphoreSlim(maxParallelDescriptors, maxParallelDescriptors);

        // Pre-allocate one slot per descriptor; tasks write by index so no lock is needed.
        var slots = new (KeyValueResponseType Type, List<(string, ReadOnlyKeyValueEntry)> Items)[descriptors.Count];

        Task[] fanOutTasks = Enumerable.Range(0, descriptors.Count)
            .Select(idx => FetchDescriptorSlotAsync(
                idx, descriptors[idx], transactionId, prefixedKey,
                readTimestamp, durability, bucketPageSize, sem, slots,
                beforeQuery, afterDescriptor, cancellationToken))
            .ToArray();

        await Task.WhenAll(fanOutTasks);

        // Propagate any early-exit response (MustRetry / WaitingForReplication).
        foreach ((KeyValueResponseType type, _) in slots)
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                return new(type, []);

        // Concatenate in descriptor StartKey order (FindAll is sorted; ranges are disjoint, so
        // the concatenation is already globally ordered — same guarantee as the sequential version).
        var allItems = new List<(string, ReadOnlyKeyValueEntry)>();
        foreach ((_, List<(string, ReadOnlyKeyValueEntry)> items) in slots)
            allItems.AddRange(items);

        return new(KeyValueResponseType.Get, allItems);
    }

    private async Task FetchDescriptorSlotAsync(
        int idx, RangeDescriptor descriptor,
        HLCTimestamp transactionId, string prefixedKey,
        HLCTimestamp readTimestamp, KeyValueDurability durability,
        int bucketPageSize, SemaphoreSlim sem,
        (KeyValueResponseType, List<(string, ReadOnlyKeyValueEntry)>)[] slots,
        Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken)
    {
        await sem.WaitAsync(cancellationToken);
        try
        {
            if (beforeQuery is not null)
                await beforeQuery(idx);

            (string? clStart, bool clStartInc, string? clEnd, bool clEndInc) =
                ClipRange(null, true, null, false, descriptor);

            string? cursorKey = clStart;
            bool    cursorInc = clStartInc;
            var items = new List<(string, ReadOnlyKeyValueEntry)>();

            while (true)
            {
                KeyValueGetByRangeResult page = await QueryDescriptorRange(
                    descriptor.PartitionId, transactionId, prefixedKey,
                    cursorKey, cursorInc, clEnd, clEndInc,
                    bucketPageSize, readTimestamp, durability, cancellationToken);

                if (page.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                {
                    slots[idx] = (page.Type, []);
                    return;
                }

                if (page.Type != KeyValueResponseType.Get)
                    break;

                items.AddRange(page.Items);

                if (!page.HasMore || page.NextCursor is null)
                    break;

                if (!KeyValueRangeCursor.TryDecode(page.NextCursor, out string lastKey, out _, out _, out _))
                    break;

                cursorKey = lastKey;
                cursorInc = false;
            }

            slots[idx] = (KeyValueResponseType.Get, items);

            if (afterDescriptor is not null)
                await afterDescriptor(idx);
        }
        finally
        {
            sem.Release();
        }
    }

    /// <summary>
    /// Locates the leader for the given prefix and executes a bounded, cursor-paged range scan.
    /// For unsplit spaces routes to the single partition leader directly. For split key-range spaces
    /// fans out across all intersecting descriptors in StartKey order, clips each sub-range, and
    /// merges results maintaining key order (multi-range stitch).
    /// </summary>
    public async Task<KeyValueGetByRangeResult> LocateAndGetByRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(prefix))
            return new(KeyValueResponseType.Errored, [], null, false);

        // Fast path: unsplit space (or hash space) — single partition, no fan-out overhead.
        if (RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefix))
        {
            int singlePartitionId = RoutePrefixKey(prefix);

            if (!raft.Joined || await ConfirmLeadershipForRead(singlePartitionId, cancellationToken))
                return await manager.GetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability);

            string? singleLeader = await TryWaitForLeader(singlePartitionId, cancellationToken);
            if (singleLeader is null || singleLeader == raft.GetLocalEndpoint())
                return new(KeyValueResponseType.MustRetry, [], null, false);

            logger.LogGetRangeKeyValueRedirected(prefix, singlePartitionId, singleLeader);

            return await interNodeCommunication.GetByRange(singleLeader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);
        }

        // Multi-range path: key-range space has been split; fan out across intersecting descriptors.
        // RangeMap is snapshotted once for this page. A split landing mid-fan-out means the loop
        // may query a now-stale source partition, but that is safe: the split transaction orphan-retains [K,E) on
        // the source, so the fixed readTimestamp (MVCC) still resolves correctly from there. The
        // next page re-resolves RangeMapStore.Current fresh and routes to the new partition.
        // The scan prefix is already the key space, so extracting the portion before a trailing
        // separator would just return it unchanged.
        string keySpace = prefix;
        RangeMap rangeMap = manager.RangeMapStore.Current;
        ArraySegment<RangeDescriptor> descriptors = rangeMap.FindIntersecting(keySpace, startKey, endKey);

        if (descriptors.Count == 0)
            return new(KeyValueResponseType.Get, [], null, false);

        var accumulated = new List<(string, ReadOnlyKeyValueEntry)>();
        int remaining   = limit > 0 ? limit : int.MaxValue;
        bool hasMore    = false;

        foreach (RangeDescriptor descriptor in descriptors)
        {
            if (remaining <= 0) { hasMore = true; break; }

            (string? clStart, bool clStartInc, string? clEnd, bool clEndInc) =
                ClipRange(startKey, startInclusive, endKey, endInclusive, descriptor);

            int pageLimit = remaining == int.MaxValue ? 0 : remaining;

            KeyValueGetByRangeResult part = await QueryDescriptorRange(
                descriptor.PartitionId, transactionId, prefix,
                clStart, clStartInc, clEnd, clEndInc,
                pageLimit, readTimestamp, durability, cancellationToken);

            if (part.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                return part;

            if (part.Type != KeyValueResponseType.Get)
                continue;

            accumulated.AddRange(part.Items);

            if (limit > 0)
                remaining -= part.Items.Count;

            if (part.HasMore) { hasMore = true; break; }
        }

        if (accumulated.Count == 0)
            return new(KeyValueResponseType.Get, [], null, false);

        string? cursor = null;
        if (hasMore)
        {
            string lastKey = accumulated[^1].Item1;
            HLCTimestamp ts = readTimestamp.IsNull() ? HLCTimestamp.Zero : readTimestamp;
            // Intentionally generation-free: each page re-resolves FindIntersecting from lastKey
            // against the live map, so a split between pages is handled unconditionally — no
            // generation miss-detection needed. Do not add rangeGeneration here.
            cursor = KeyValueRangeCursor.Encode(lastKey, durability, prefix, ts);
        }

        return new(KeyValueResponseType.Get, accumulated, cursor, hasMore);
    }

    /// <summary>Routes a GetByRange page to <paramref name="partitionId"/>'s leader.</summary>
    private async Task<KeyValueGetByRangeResult> QueryDescriptorRange(
        int partitionId,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await ConfirmLeadershipForRead(partitionId, cancellationToken))
            return await manager.GetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return new(KeyValueResponseType.MustRetry, [], null, false);

        return await interNodeCommunication.GetByRange(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Clips the caller's query range <c>[queryStart, queryEnd)</c> to the descriptor's half-open
    /// interval <c>[d.StartKey, d.EndKey)</c>, preserving the caller's inclusive/exclusive flags
    /// where they dominate; the descriptor boundary is always inclusive at start, exclusive at end.
    /// </summary>
    private static (string? start, bool startInc, string? end, bool endInc) ClipRange(
        string? queryStart, bool queryStartInc,
        string? queryEnd,   bool queryEndInc,
        RangeDescriptor d)
    {
        string? clStart;
        bool    clStartInc;

        if (d.StartKey is null)
        {
            // descriptor starts at -∞; query start is the effective lower bound
            clStart    = queryStart;
            clStartInc = queryStartInc;
        }
        else if (queryStart is null)
        {
            // query unbounded below; descriptor's start is the effective lower bound (inclusive)
            clStart    = d.StartKey;
            clStartInc = true;
        }
        else
        {
            int cmp = string.CompareOrdinal(queryStart, d.StartKey);
            if (cmp >= 0) { clStart = queryStart; clStartInc = queryStartInc; }
            else          { clStart = d.StartKey;  clStartInc = true; }
        }

        string? clEnd;
        bool    clEndInc;

        if (d.EndKey is null && queryEnd is null)
        {
            clEnd    = null;
            clEndInc = false;
        }
        else if (d.EndKey is null)
        {
            clEnd    = queryEnd;
            clEndInc = queryEndInc;
        }
        else if (queryEnd is null)
        {
            clEnd    = d.EndKey;
            clEndInc = false;  // descriptor boundary is always exclusive
        }
        else
        {
            int cmp = string.CompareOrdinal(queryEnd, d.EndKey);
            if (cmp <= 0) { clEnd = queryEnd;  clEndInc = queryEndInc; }
            else          { clEnd = d.EndKey;   clEndInc = false; }
        }

        return (clStart, clStartInc, clEnd, clEndInc);
    }

    /// <summary>
    /// Attempts to locate the appropriate partition leader and starts a transaction based on the provided options.
    /// </summary>
    /// <param name="options">The transaction options, including a unique identifier used to determine the partition.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A tuple containing the result of the transaction operation (<see cref="KeyValueResponseType"/>),
    /// and a <see cref="TransactionHandle"/> that must be used for all subsequent commit/rollback calls.</returns>
    public async Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(options.CoordinatorKey))
            options.CoordinatorKey = Guid.NewGuid().ToString("N");

        int partitionId = dataPartitionRouter.Locate(options.CoordinatorKey);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.StartTransaction(options);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return new(KeyValueResponseType.MustRetry, TransactionHandle.None);

        logger.LogStartTransactionRedirected(options.CoordinatorKey, partitionId, leader);

        return await interNodeCommunication.StartTransaction(leader, options, cancellationToken);
    }

    /// <summary>
    /// Locates the appropriate partition and commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="acquiredLocks">The list of keys that have been locked as part of the transaction.</param>
    /// <param name="modifiedKeys">The list of keys that have been modified as part of the transaction.</param>
    /// <param name="readKeys">The list of keys read during the transaction.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A <see cref="KeyValueResponseType"/> indicating the outcome of the transaction operation.
    /// </returns>
    public async Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        if (handle.IsEmpty)
            return (KeyValueResponseType.Errored, null);

        int partitionId = dataPartitionRouter.Locate(handle.CoordinatorKey);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.CommitTransaction(handle);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        logger.LogCommitTransactionRedirected(handle.CoordinatorKey, partitionId, leader);

        return await interNodeCommunication.CommitTransaction(leader, handle, cancellationToken);
    }

    /// <summary>
    /// Locates and rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="KeyValueResponseType"/> indicating the result of the operation.</returns>
    public async Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        if (handle.IsEmpty)
            return KeyValueResponseType.Errored;

        int partitionId = dataPartitionRouter.Locate(handle.CoordinatorKey);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.RollbackTransaction(handle);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogRollbackTransactionRedirected(handle.CoordinatorKey, partitionId, leader);

        return await interNodeCommunication.RollbackTransaction(leader, handle, cancellationToken);
    }

    /// <summary>
    /// Routes an operation registration to the node that leads the coordinator partition for
    /// <paramref name="coordinatorKey"/> — the node that holds the session — registering it locally
    /// when this node is that leader and forwarding otherwise.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (OperationRegistrationOutcome.RejectedSessionClosed, KeyValueResponseType.Errored, 0, HLCTimestamp.Zero, null);

        int partitionId = dataPartitionRouter.Locate(coordinatorKey);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return manager.BeginOperation(transactionId, operationId, kind, payloadDigest);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (OperationRegistrationOutcome.AlreadyPending, KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero, null);

        return await interNodeCommunication.BeginOperation(leader, coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);
    }

    /// <summary>Returns the partition id that owns the coordinator session for <paramref name="coordinatorKey"/>.</summary>
    public int LocatePartition(string coordinatorKey) => dataPartitionRouter.Locate(coordinatorKey);

    /// <summary>Routes an operation completion to the coordinator-partition leader for <paramref name="coordinatorKey"/>. Returns the acknowledged outcome and the record anchor after the fold, or MustRetry when routing did not deliver the completion.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (KeyValueResponseType.MustRetry, null);

        int partitionId = dataPartitionRouter.Locate(coordinatorKey);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return (KeyValueResponseType.Set, manager.CompleteOperation(transactionId, operationId, payload));

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        return await interNodeCommunication.CompleteOperation(leader, coordinatorKey, transactionId, operationId, payload, cancellationToken);
    }

    /// <summary>Routes a working-set query to the coordinator-partition leader for <paramref name="coordinatorKey"/>.</summary>
    public async Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return null;

        int partitionId = dataPartitionRouter.Locate(coordinatorKey);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return manager.GetTransactionWorkingSet(transactionId);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return null;

        return await interNodeCommunication.GetTransactionWorkingSet(leader, coordinatorKey, transactionId, cancellationToken);
    }

    /// <summary>Routes a close-and-snapshot to the coordinator-partition leader for <paramref name="coordinatorKey"/>.</summary>
    public async Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (KeyValueResponseType.Errored, null);

        int partitionId = dataPartitionRouter.Locate(coordinatorKey);

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.CloseTransaction(transactionId, cancellationToken);

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        return await interNodeCommunication.CloseTransaction(leader, coordinatorKey, transactionId, cancellationToken);
    }

    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        // Every per-node view (and the local disk page) can lag the commit frontier: a node that has
        // not yet applied a committed delete or overwrite still contributes the previous version of
        // the key. Merging newest-wins by (revision, commit HLC) — with tombstones traveling so a
        // delete is a mergeable version rather than an absence — lets the freshest contribution
        // (the key's partition leader applies synchronously before acking, and is always scanned)
        // suppress the stale copies. First-wins union would resurrect deleted keys and serve stale
        // values whenever a lagging node happened to be merged first.
        Dictionary<string, ReadOnlyKeyValueEntry> merged = new(StringComparer.Ordinal);

        static void MergeItems(Dictionary<string, ReadOnlyKeyValueEntry> merged, List<(string, ReadOnlyKeyValueEntry)> items)
        {
            foreach ((string key, ReadOnlyKeyValueEntry entry) in items)
            {
                if (merged.TryGetValue(key, out ReadOnlyKeyValueEntry? existing)
                    && (existing.Revision > entry.Revision
                        || (existing.Revision == entry.Revision && existing.LastModified >= entry.LastModified)))
                    continue;

                merged[key] = entry;
            }
        }

        KeyValueGetByBucketResult items = await manager.ScanByPrefix(prefixKeyName, readTimestamp, durability, includeTombstones: true);

        if (items.Type == KeyValueResponseType.Get)
            MergeItems(merged, items.Items);

        IReadOnlyList<string> scanEndpoints = ResolveScanFanOutEndpoints(prefixKeyName);

        Task<KeyValueGetByBucketResult>[] tasks = new Task<KeyValueGetByBucketResult>[scanEndpoints.Count];
        for (int i = 0; i < scanEndpoints.Count; i++)
            tasks[i] = NodeScanByPrefix(scanEndpoints[i], prefixKeyName, readTimestamp, durability, cancellationToken);

        KeyValueGetByBucketResult[] nodeResults = await Task.WhenAll(tasks);

        foreach (KeyValueGetByBucketResult nodeResult in nodeResults)
        {
            if (nodeResult.Type == KeyValueResponseType.Get)
                MergeItems(merged, nodeResult.Items);
        }

        if (durability == KeyValueDurability.Persistent)
        {
            KeyValueGetByBucketResult result = await manager.ScanByPrefixFromDisk(prefixKeyName, readTimestamp, includeTombstones: true);

            if (result.Type == KeyValueResponseType.Get)
                MergeItems(merged, result.Items);
        }

        List<(string, ReadOnlyKeyValueEntry)> unionItems = new(merged.Count);

        foreach ((string key, ReadOnlyKeyValueEntry entry) in merged)
        {
            if (entry.State != KeyValueState.Deleted)
                unionItems.Add((key, entry));
        }

        return new(KeyValueResponseType.Get, unionItems);
    }

    /// <summary>
    /// Resolves the remote nodes a prefix scan must consult. Ephemeral data is leader-local and
    /// leadership moves, so the scan must reach every node that may ever have led the prefix's
    /// partitions. Under legacy full replication (any touched partition with an empty replica
    /// set) that is every peer — today's broadcast, unchanged. Under replica placement only a
    /// partition's replicas can ever have led it, so the fan-out narrows to the union of their
    /// remote endpoints (every role: a replica marked for removal may have led moments ago).
    /// Narrowing is also the correct filter, not just cheaper: a node that lost its replica may
    /// still hold stale leftovers until they are purged, and consulting it would resurface data
    /// that point reads can no longer return. The local node is excluded — its in-memory and
    /// disk contributions are merged separately by the caller.
    /// </summary>
    private IReadOnlyList<string> ResolveScanFanOutEndpoints(string prefixKeyName)
    {
        // Same prefix classification the bucket fan-out uses: an unsplit/hash space routes whole
        // to one partition; a split key-range space touches each of its descriptors' partitions.
        List<int> partitions = [];

        if (RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixKeyName))
            partitions.Add(RoutePrefixKey(prefixKeyName));
        else
        {
            foreach (RangeDescriptor descriptor in manager.RangeMapStore.Current.FindAll(prefixKeyName))
                partitions.Add(descriptor.PartitionId);
        }

        string localEndpoint = raft.GetLocalEndpoint();
        HashSet<string> endpoints = new(StringComparer.Ordinal);

        foreach (int partitionId in partitions)
        {
            IReadOnlyList<Kommander.System.RaftReplica> replicas = raft.GetPartitionReplicas(partitionId);

            // Legacy full replication: any peer may hold a copy — keep the all-node broadcast.
            if (replicas.Count == 0)
                return [.. raft.GetNodes().Select(static node => node.Endpoint)];

            foreach (Kommander.System.RaftReplica replica in replicas)
            {
                if (!string.Equals(replica.Endpoint, localEndpoint, StringComparison.Ordinal))
                    endpoints.Add(replica.Endpoint);
            }
        }

        return [.. endpoints];
    }

    private async Task<KeyValueGetByBucketResult> NodeScanByPrefix(
        string endpoint,
        string prefixKeyName,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return await interNodeCommunication.ScanByPrefix(endpoint, prefixKeyName, readTimestamp, durability, includeTombstones: true, cancellationToken);
    }
}
