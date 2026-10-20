
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
/// Point and many-key get / exists operations: routes each read to a confirmed leader or serves it from a follower where allowed.
/// </summary>
internal sealed class LocatorReadOperations
{
    private readonly KeyValuesManager manager;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly ILogger<IKahuna> logger;

    private readonly LocatorRouting routing;

    internal LocatorReadOperations(LocatorRouting routing)
    {
        this.manager = routing.Manager;
        this.raft = routing.Raft;
        this.interNodeCommunication = routing.InterNodeCommunication;
        this.logger = routing.Logger;
        this.routing = routing;
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

        int partitionId = routing.RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, null);

        // The leadership check is stamped on both sides and tagged by whether it completed synchronously:
        // a synchronous answer is the leader's lease, an asynchronous one is an executor read-index round.
        long confirmStart = Stopwatch.GetTimestamp();
        ValueTask<bool> confirmation = routing.ConfirmLeadershipForRead(partitionId, cancellationToken);
        bool leaseHit = confirmation.IsCompleted;
        bool confirmed = await confirmation;
        long confirmEnd = Stopwatch.GetTimestamp();
        KeyValueReadMetrics.RecordConfirm(confirmStart, confirmEnd, leaseHit);

        if (confirmed)
        {
            routing.RecordLocalKeyRoute(key, partitionId);
            LocalPointRead local = await manager.TryGetValueLocal(transactionId, key, revision, readTimestamp, durability);
            KeyValueReadMetrics.RecordActor(confirmEnd, Stopwatch.GetTimestamp(), local.ServedFromPublishedHead);
            return (local.Type, local.Entry);
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, null);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        routing.RecordForwardedKeyRoute(key, partitionId, leader);

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

        int partitionId = routing.RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, null);

        if (await routing.ConfirmLeadershipForRead(partitionId, cancellationToken))
        {
            routing.RecordLocalKeyRoute(key, partitionId);
            return await manager.TryExistsValue(transactionId, key, revision, readTimestamp, durability);
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return (KeyValueResponseType.MustRetry, null);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        logger.LogExistsKeyValueRedirect(key, partitionId, leader);

        routing.RecordForwardedKeyRoute(key, partitionId, leader);

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

            int partitionId = routing.RouteKey(item.key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, k.durability, (ReadOnlyKeyValueEntry?)null))];

            routing.RecordBatchKeyRoute(item.key, partitionId, leader);

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
            if (await routing.AmILeaderQuickIfHosted(routing.RouteKey(item.key)))
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

            int partitionId = routing.RouteKey(item.key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, k.durability, (ReadOnlyKeyValueEntry?)null))];

            routing.RecordBatchKeyRoute(item.key, partitionId, leader);

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
            if (!await routing.ConfirmLeadershipForGroupRead(leader, leaderByPartition, cancellationToken))
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
            if (!await routing.ConfirmLeadershipForGroupRead(leader, leaderByPartition, cancellationToken))
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
}
