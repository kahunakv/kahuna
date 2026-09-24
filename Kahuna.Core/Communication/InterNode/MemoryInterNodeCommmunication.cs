using System.Collections.Concurrent;
using Kahuna.Server.KeyValues.Data;
using System.Diagnostics.CodeAnalysis;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Provides inter-node communication functionality using memory-based calls, implementing operations
/// such as locking, unlocking, key-value management, and transactional support among distributed nodes.
/// <para>
/// One instance can serve every node of an in-process cluster. It then does not know which node makes
/// a call, so it cannot drop traffic between two given nodes. For that, give each node its own view
/// from <see cref="ForNode"/>: the views share the node table and the set of blocked links, and each
/// view checks the link from its own node to the target (see <see cref="BlockLink"/>).
/// </para>
/// </summary>
public class MemoryInterNodeCommmunication : IInterNodeCommunication
{
    /// <summary>
    /// The state that an instance and all of its views share: the node table and the blocked links.
    /// </summary>
    private sealed class SharedRoutes
    {
        /// <summary>
        /// Routing table mapping node names to their <see cref="IKahuna"/> instances. Volatile, and
        /// defensively copied in <see cref="SetNodes"/>: the request-dispatch methods read it
        /// concurrently from many executor threads, so a caller re-registering nodes mid-run (e.g.
        /// adding a joiner to an already-running cluster) must atomically publish a <b>new</b>
        /// dictionary. Mutating a shared instance during a resize silently drops or corrupts
        /// inter-node RPC delivery between existing members, which manifests as spurious leadership
        /// churn and stalled promotions.
        /// </summary>
        public volatile Dictionary<string, IKahuna>? Nodes;

        /// <summary>
        /// Blocked links as (from, to) endpoint pairs, or null when no link is blocked. The set is
        /// never changed after it is published: a change publishes a new set, so a dispatch reads a
        /// complete snapshot without a lock, and the common case (no filter) costs one null check.
        /// </summary>
        public volatile HashSet<(string From, string To)>? BlockedLinks;

        /// <summary>Serializes the writers of <see cref="BlockedLinks"/>. Readers never take it.</summary>
        public readonly object LinkGate = new();
    }

    private readonly SharedRoutes routes;

    /// <summary>The endpoint of the node that calls through this view; null for an unbound instance.</summary>
    private readonly string? localEndpoint;

    public MemoryInterNodeCommmunication()
    {
        routes = new();
    }

    private MemoryInterNodeCommmunication(SharedRoutes routes, string localEndpoint)
    {
        this.routes = routes;
        this.localEndpoint = localEndpoint;
    }

    private int getByRangeCallCount;
    private int beginOperationCallCount;
    private int completeOperationCallCount;
    private int checkWriteIntentCallCount;
    private int checkManyWriteIntentsCallCount;
    private int existsManyCallCount;
    private int sequenceForwardCallCount;

    /// <summary>Number of <c>GetByRange</c> RPCs dispatched to a remote node. Used by multi-range fan-out tests.</summary>
    public int GetByRangeCallCount => Volatile.Read(ref getByRangeCallCount);

    /// <summary>Number of single-key <c>TryCheckWriteIntent</c> RPCs dispatched to a remote node — one per read
    /// key probed at commit time, so it measures the fan-out of the commit-time concurrent-writer probe.</summary>
    public int CheckWriteIntentCallCount => Volatile.Read(ref checkWriteIntentCallCount);

    /// <summary>Number of grouped <c>TryCheckManyWriteIntents</c> RPCs dispatched to a remote node — one per
    /// remote node holding any of the probed keys, however many of its keys the probe covers.</summary>
    public int CheckManyWriteIntentsCallCount => Volatile.Read(ref checkManyWriteIntentsCallCount);

    /// <summary>Number of <c>TryExistsMany</c> RPCs dispatched to a remote node — one per remote node holding
    /// any of the requested keys, so it measures what the same key set costs when grouped by owner.</summary>
    public int ExistsManyCallCount => Volatile.Read(ref existsManyCallCount);

    /// <summary>Sequence operations forwarded to another node, so a test can prove the redirect happened.</summary>
    public int SequenceForwardCallCount => Volatile.Read(ref sequenceForwardCallCount);

    /// <summary>Number of register-remote <c>BeginOperation</c> RPCs dispatched to a remote coordinator node.</summary>
    public int BeginOperationCallCount => Volatile.Read(ref beginOperationCallCount);

    /// <summary>Number of register-remote <c>CompleteOperation</c> RPCs dispatched to a remote coordinator node.</summary>
    public int CompleteOperationCallCount => Volatile.Read(ref completeOperationCallCount);

    /// <summary>
    /// Sets the nodes for inter-node communication. The map is copied so the published table is never
    /// mutated by the caller after the fact and concurrent readers always observe a complete snapshot.
    /// The table is shared with every view from <see cref="ForNode"/>.
    /// </summary>
    /// <param name="nodes">A dictionary mapping node names to `IKahuna` instances.</param>
    public void SetNodes(Dictionary<string, IKahuna> nodes)
    {
        routes.Nodes = new(nodes);
    }

    /// <summary>
    /// A view of this transport for the node at <paramref name="localEndpoint"/>. The view shares the
    /// node table and the blocked links with this instance and with its other views, and it drops a
    /// call when the link between its node and the target is blocked. The call counters and the test
    /// seams belong to each instance and are not shared.
    /// </summary>
    public MemoryInterNodeCommmunication ForNode(string localEndpoint)
    {
        ArgumentException.ThrowIfNullOrEmpty(localEndpoint);

        return new(routes, localEndpoint);
    }

    /// <summary>
    /// Blocks the link from <paramref name="from"/> to <paramref name="to"/>. A call is a request and a
    /// reply, so a call between the two nodes fails in either direction while the link is blocked. It
    /// fails before it runs, with a <see cref="KahunaServerException"/>, as a call to a stopped node
    /// does. A real network can also lose only the reply after the call ran; this transport does not
    /// model that. Only calls through a view from <see cref="ForNode"/> are checked.
    /// </summary>
    public void BlockLink(string from, string to) => ChangeLink(from, to, block: true);

    /// <summary>Unblocks the link from <paramref name="from"/> to <paramref name="to"/>.</summary>
    public void UnblockLink(string from, string to) => ChangeLink(from, to, block: false);

    /// <summary>Unblocks every link.</summary>
    public void UnblockAllLinks()
    {
        lock (routes.LinkGate)
            routes.BlockedLinks = null;
    }

    /// <summary>True when the link from <paramref name="from"/> to <paramref name="to"/> is blocked.</summary>
    public bool IsLinkBlocked(string from, string to) =>
        routes.BlockedLinks is { } blocked && blocked.Contains((from, to));

    private void ChangeLink(string from, string to, bool block)
    {
        ArgumentException.ThrowIfNullOrEmpty(from);
        ArgumentException.ThrowIfNullOrEmpty(to);

        lock (routes.LinkGate)
        {
            HashSet<(string, string)> next = routes.BlockedLinks is { } current ? new(current) : [];

            if (block)
                next.Add((from, to));
            else
                next.Remove((from, to));

            routes.BlockedLinks = next.Count == 0 ? null : next;
        }
    }

    /// <summary>
    /// Resolves the target node of a call. False when the node is not registered, or when the link
    /// between this view's node and the target is blocked in either direction.
    /// </summary>
    private bool TryGetNode(string node, [NotNullWhen(true)] out IKahuna? kahunaNode)
    {
        Dictionary<string, IKahuna>? current = routes.Nodes;

        if (current is null || !current.TryGetValue(node, out kahunaNode))
        {
            kahunaNode = null;
            return false;
        }

        if (localEndpoint is not null && routes.BlockedLinks is { } blocked
            && (blocked.Contains((localEndpoint, node)) || blocked.Contains((node, localEndpoint))))
        {
            kahunaNode = null;
            return false;
        }

        return true;
    }

    /// <summary>The failure of a call whose target is not registered or cannot be reached.</summary>
    private KahunaServerException Unreachable(string node) =>
        localEndpoint is not null && routes.Nodes is { } current && current.ContainsKey(node)
            ? new($"The node {node} is not reachable from {localEndpoint}: the link between them is blocked.")
            : new($"The node {node} does not exist.");
    
    /// <summary>
    /// Attempts to acquire a lock on a resource in a specific node.
    /// </summary>
    /// <param name="node">The target node.</param>
    /// <param name="resource">The resource to lock.</param>
    /// <param name="owner">The owner of the lock.</param>
    /// <param name="expiresMs">The expiration time in milliseconds.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A tuple containing the lock response type and the lock revision.</returns>
    /// <exception cref="KahunaServerException">Thrown if the node does not exist.</exception>
    public async Task<(LockResponseType, long)> TryLock(
        string node, 
        string resource, 
        byte[] owner, 
        int expiresMs, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            // Re-enter the locator (like the production gRPC transport does) so a hosting
            // non-leader receiver redirects once to its accurately-resolved local leader instead
            // of running the lock on a follower; the forwarded marker keeps a non-hosting
            // receiver from forwarding onward.
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryLock(resource, owner, expiresMs, durability, cancellationToken);
        }
        
        throw Unreachable(node);
    }
    
    /// <summary>
    /// Attempts to extend an existing lock on a resource in a specific node.
    /// </summary>
    /// <param name="node">The target node.</param>
    /// <param name="resource">The resource to extend the lock on.</param>
    /// <param name="owner">The owner of the lock.</param>
    /// <param name="expiresMs">The new expiration time in milliseconds.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A tuple containing the lock response type and the lock revision.</returns>
    /// <exception cref="KahunaServerException">Thrown if the node does not exist.</exception>
    public async Task<(LockResponseType, long)> TryExtendLock(
        string node, 
        string resource, 
        byte[] owner, 
        int expiresMs, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryExtendLock(resource, owner, expiresMs, durability, cancellationToken);
        }
        
        throw Unreachable(node);
    }
    
    /// <summary>
    /// Attempts to release a lock on a resource in a specific node.
    /// </summary>
    /// <param name="node">The target node.</param>
    /// <param name="resource">The resource to unlock.</param>
    /// <param name="owner">The owner of the lock.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The lock response type.</returns>
    /// <exception cref="KahunaServerException">Thrown if the node does not exist.</exception>
    public async Task<LockResponseType> TryUnlock(
        string node, 
        string resource, 
        byte[] owner, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryUnlock(resource, owner, durability, cancellationToken);
        }
        
        throw Unreachable(node);
    }
    
    /// <summary>
    /// Retrieves information about the lock
    /// </summary>
    /// <param name="node">The target node.</param>
    /// <param name="resource">The resource to retrieve the lock for.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A tuple containing the lock response type and the lock context.</returns>
    /// <exception cref="KahunaServerException">Thrown if the node does not exist.</exception>
    public async Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(
        string node, 
        string resource, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndGetLock(resource, durability, cancellationToken);
        }
        
        throw Unreachable(node);
    }

    /// <summary>Forwards a sequence create to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, long)> CreateSequence(
        string node,
        string name,
        long initialValue,
        long increment,
        long? maxValue,
        int? blockSize,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref sequenceForwardCallCount);
            return await kahunaNode.CreateSequence(name, initialValue, increment, maxValue, blockSize, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>Forwards a sequence update to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, long)> UpdateSequence(
        string node,
        string name,
        SequenceUpdate update,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref sequenceForwardCallCount);
            return await kahunaNode.UpdateSequence(name, update, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>Forwards a sequence read to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, ReadOnlySequenceEntry?)> GetSequence(
        string node,
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref sequenceForwardCallCount);
            return await kahunaNode.GetSequence(name, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>Forwards a single-value allocation to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, SequenceAllocation)> NextSequenceValue(
        string node,
        string name,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref sequenceForwardCallCount);
            return await kahunaNode.NextSequenceValue(name, idempotencyKey, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>Forwards a multi-value allocation to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, SequenceAllocation)> ReserveSequenceRange(
        string node,
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref sequenceForwardCallCount);
            return await kahunaNode.ReserveSequenceRange(name, count, idempotencyKey, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>Forwards a sequence delete to the node that owns the sequence's partition.</summary>
    public async Task<SequenceResponseType> DeleteSequence(
        string node,
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref sequenceForwardCallCount);
            return await kahunaNode.DeleteSequence(name, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
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
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        long routedGeneration,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="items"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TrySetManyNodeKeyValue(
        string node, 
        List<KahunaSetKeyValueRequestItem> items, 
        Lock lockSync, 
        List<KahunaSetKeyValueResponseItem> responses, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            // Hand the receiver the whole batch, as the gRPC transport does: the receiver's batched path
            // keeps every per-item field the single-key tuple cannot carry (the conflict holder, the item's
            // conflict policy) and applies the same group leadership gate the production hop applies.
            List<KahunaSetKeyValueResponseItem> remote = await kahunaNode.LocateAndTrySetManyKeyValue(items, cancellationToken);

            lock (lockSync)
                responses.AddRange(remote);

            return;
        }
        
        throw Unreachable(node);
    }
    
    public async Task TryDeleteManyNodeKeyValue(
        string node,
        List<KahunaDeleteKeyValueRequestItem> items,
        Lock lockSync,
        List<KahunaDeleteKeyValueResponseItem> responses,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            ConcurrentBag<KahunaDeleteKeyValueResponseItem> bag = [];

            foreach (KahunaDeleteKeyValueRequestItem item in items)
            {
                (KeyValueResponseType, long, HLCTimestamp) resp = await kahunaNode.LocateAndTryDeleteKeyValue(
                    item.TransactionId,
                    item.Key ?? "",
                    item.Durability,
                    cancellationToken
                );

                bag.Add(new()
                {
                    Key = item.Key,
                    Type = resp.Item1,
                    Revision = resp.Item2,
                    LastModified = resp.Item3,
                    Durability = item.Durability
                });
            }

            foreach (KahunaDeleteKeyValueResponseItem responseBag in bag)
            {
                lock (lockSync)
                    responses.Add(responseBag);
            }

            return;
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);
        }
        
        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);
        }
        
        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task TryGetManyNodeValues(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> readResponses =
                await kahunaNode.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken);

            // Test seam: replace designated per-key results with MustRetry to simulate a per-key routing
            // transient. Only fires when GetManyValuesFault is set by a test.
            if (GetManyValuesFault is not null)
            {
                for (int i = 0; i < readResponses.Count; i++)
                {
                    (_, string key, KeyValueDurability dur, _) = readResponses[i];
                    if (GetManyValuesFault(transactionId, key))
                        readResponses[i] = (KeyValueResponseType.MustRetry, key, dur, null);
                }
            }

            // Test seam: replace designated per-key results with Errored to simulate a dropped actor
            // response (a non-transient, non-confirmed type). Only fires when GetManyValuesErrorFault is set.
            if (GetManyValuesErrorFault is not null)
            {
                for (int i = 0; i < readResponses.Count; i++)
                {
                    (_, string key, KeyValueDurability dur, _) = readResponses[i];
                    if (GetManyValuesErrorFault(transactionId, key))
                        readResponses[i] = (KeyValueResponseType.Errored, key, dur, null);
                }
            }

            AddToReadManyResponses(readResponses, lockSync, responses);
            return;
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// Test seam: when set, invoked per-key after the remote batch read completes. Returns true to replace
    /// that key's result with <c>MustRetry</c>, simulating a per-key routing transient in a batch read.
    /// </summary>
    public Func<HLCTimestamp, string, bool>? GetManyValuesFault { get; set; }

    /// <summary>
    /// Test seam: when set, invoked per-key after the remote batch read completes. Returns true to replace
    /// that key's result with <c>Errored</c>, simulating a dropped actor response — a non-transient,
    /// non-confirmed type that must still be excluded from the read set rather than folded as "absent".
    /// </summary>
    public Func<HLCTimestamp, string, bool>? GetManyValuesErrorFault { get; set; }

    public async Task TryExistsManyNodeValues(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref existsManyCallCount);

            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> readResponses =
                await kahunaNode.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken);

            AddToReadManyResponses(readResponses, lockSync, responses);
            return;
        }

        throw Unreachable(node);
    }

    private static void AddToReadManyResponses(
        IEnumerable<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> items,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses
    )
    {
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) in items)
        {
            lock (lockSync)
                responses.Add((type, key, durability, entry));
        }
    }

    public async Task<KeyValueResponseType> TryCheckWriteIntentValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref checkWriteIntentCallCount);

            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryCheckWriteIntent(transactionId, key, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntents(
        string node,
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref checkManyWriteIntentsCallCount);

            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryCheckManyWriteIntents(transactionId, keys, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId, long BaseRevision)> TryAcquireExclusiveLock(
        string node,
        HLCTimestamp transactionId,
        string key,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryAcquireExclusiveLockObserved(transactionId, key, expiresMs, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(
        string node, 
        HLCTimestamp transactionId,
        string prefixKey, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);
        }
        
        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryAcquireNodeExclusiveLocks(
        string node,
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder, long baseRevision)> responses,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            // Acquired one key at a time in the order given, stopping at the first refusal, which is what the
            // remote node does when this same request arrives over gRPC. Taking every key regardless would
            // leave a transaction holding locks past the one it was already refused, so two transactions over
            // the same keys could each end up holding part of the overlap and both abort with nothing done.
            // Order is preserved for the same reason: the caller reports the first refusal it finds.
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder, long baseRevision)> acquired = new(xkeys.Count);

            foreach ((string key, int expiresMs, KeyValueDurability durability) in xkeys)
            {
                (KeyValueResponseType type, string keyName, KeyValueDurability keyDurability, HLCTimestamp holder, long baseRevision) =
                    await kahunaNode.LocateAndTryAcquireExclusiveLockObserved(transactionId, key, expiresMs, durability, cancellationToken);

                acquired.Add((type, keyName, keyDurability, holder, baseRevision));

                if (type != KeyValueResponseType.Locked)
                    break;
            }

            lock (lockSync)
                responses.AddRange(acquired);

            return;
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken);
        }
        
        throw Unreachable(node);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(
        string node, 
        HLCTimestamp transactionId, 
        string prefixKey, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// Optional hook invoked before every <c>TryAcquireRangeLock</c> RPC. Used by tests to inject latency
    /// or cancellation for a specific transaction so the bounded-parallel sweep can be stressed without
    /// needing a real slow participant.
    /// </summary>
    public Func<HLCTimestamp, string, CancellationToken, Task>? AcquireRangeLockHook { get; set; }

    public async Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken
    )
    {
        // Pass the caller's token so an injected delay can honor the renewal-sweep deadline (a slow
        // participant is cancelled at the budget, not merely delayed).
        if (AcquireRangeLockHook is not null)
            await AcquireRangeLockHook(transactionId, prefix, cancellationToken);

        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
        }
        throw Unreachable(node);
    }

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) => TryAcquireRangeLock(node, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, cancellationToken);

    public async Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        int? targetPartitionId = null
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            // A partition-pinned release executes on the receiver's own actor state: the sender
            // targeted the node where the lock was acquired, and re-routing through the receiver's
            // locator would misdirect the release after a split/merge cutover moved the bounds.
            if (targetPartitionId is not null)
                return await kahunaNode.TryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);

            return await kahunaNode.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        }
        throw Unreachable(node);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryReleaseNodeExclusiveLocks(
        string node, 
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability)> bag = [];

            foreach ((string key, KeyValueDurability durability) in xkeys)
            {
                (KeyValueResponseType type, string _) = await kahunaNode.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken);
                bag.Add((type, key, durability));
            }

            AddToReleaseLockResponses(bag, lockSync, responses);
            return;
        }
        
        throw Unreachable(node);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void AddToReleaseLockResponses(ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability)> bag, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses)
    {
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, key, durability));
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        long routedGeneration,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancellationToken, routedGeneration, recordAnchorKey);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryPrepareNodeMutations(
        string node, 
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            ConcurrentBag<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> bag = [];

            foreach ((string key, KeyValueDurability durability) in xkeys)
            {
                (KeyValueResponseType type, HLCTimestamp proposalId, string _, KeyValueDurability _) = await kahunaNode.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancellationToken, 0, recordAnchorKey);
                bag.Add((type, proposalId, key, durability));
            }

            AddToPrepareMutationsResponses(bag, lockSync, responses);
            return;
        }
        
        throw Unreachable(node);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void AddToPrepareMutationsResponses(
        ConcurrentBag<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> bag, 
        Lock lockSync, 
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses
    )
    {
        foreach ((KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, ticketId, key, durability));
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long)> TryCommitMutations(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            // Test seam: fail a specific participant's inter-node commit transiently before it reaches the
            // remote leader, so its prepare survives on that leader for recovery to drive — the partial durable
            // commit condition. Returns the retryable signal, never a definite failure.
            if (CommitMutationsFault is not null && CommitMutationsFault(transactionId, key))
                return (KeyValueResponseType.MustRetry, 0);

            // Test seam: let the commit apply on the remote leader (so the value, receipt, and — for an anchor —
            // the durable decision are all installed there) but hide its success from the caller as MustRetry.
            // This models a committed-but-lost/covered response: the coordinator cannot tell the commit landed.
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            if (CommitMutationsResponseFault is not null && CommitMutationsResponseFault(transactionId, key))
            {
                await kahunaNode.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancellationToken);
                return (KeyValueResponseType.MustRetry, 0);
            }

            return await kahunaNode.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<bool> DurableOperation(string node, int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.DurableOperationLocal(partitionId, kind, logType, payload, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>Test knob: when false, the typed durable bundle and decision operations answer null exactly as an
    /// older receiver that lacks them does, so the sender's per-entry fallback path can be exercised in-process.</summary>
    internal bool TypedDurableOperations { get; set; } = true;

    public async Task<DurableBundleWireReply?> DurableBundle(
        string node, int partitionId, IReadOnlyList<(string LogType, byte[] Payload)> entries,
        bool terminal, int stage, string? fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        if (!TypedDurableOperations)
            return null;

        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.DurableBundleLocal(partitionId, entries, terminal, stage, fenceKey, fenceGeneration, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<DurableDecisionWireReply?> DurableDecision(
        string node, int partitionId, byte[] decisionDelta, HLCTimestamp transactionId, long epoch,
        string? fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        if (!TypedDurableOperations)
            return null;

        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.DurableDecisionLocal(partitionId, decisionDelta, transactionId, epoch, fenceKey, fenceGeneration, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<DurableOnePhaseWireReply?> DurableOnePhase(
        string node, int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, byte[] decisionDelta,
        HLCTimestamp transactionId, long epoch, HLCTimestamp opId,
        string? fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        if (!TypedDurableOperations)
            return null;

        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.DurableOnePhaseLocal(partitionId, recordInitDelta, anchorPrepareDelta, decisionDelta, transactionId, epoch, opId, fenceKey, fenceGeneration, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<bool> ReplicateKeyValueRangePage(string node, int partitionId, byte[] page, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            // The receiving node applies the page when it leads the destination group, and relays
            // it to the leader it resolves otherwise — the sender only guessed a replica.
            return await kahunaNode.ReplicateKeyValueRangePageOnLeader(partitionId, page, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<(bool Ok, List<CompletionReceiptRecord> Receipts, byte[] TransactionRecords, byte[] PreparedIntents, bool HasMore, string? NextCursor)> GetRangeTransactionState(string node, int partitionId, string? startKey, string? endKey, KeyValueRangeStateKinds kinds, string? cursor, int maxItems, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.GetRangeTransactionStateLocal(partitionId, startKey, endKey, kinds, cursor, maxItems, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// Optional test hook consulted before every <c>GetStagedBaseVerdicts</c> RPC with the target node and the
    /// requested apply wait. Returning true models a replica whose apply has stalled (its disk paused, its WAL
    /// saturated): the call holds for the requested wait and answers <c>NotApplied</c> for every key, exactly
    /// what such a replica's handler produces when its wait budget runs out.
    /// </summary>
    public Func<string, int, bool>? StagedBaseVerdictsStalledHook { get; set; }

    /// <summary>Every <c>GetStagedBaseVerdicts</c> ask sent through this transport (node, requested wait), so a
    /// test can assert how the fence asked a replica. Null until a test assigns it.</summary>
    public ConcurrentQueue<(string Node, int WaitMs)>? StagedBaseVerdictAsks { get; set; }

    public async Task<(bool Serviced, IReadOnlyList<KeyValueStagedBaseVerdictEntry> Verdicts)> GetStagedBaseVerdicts(string node, int partitionId, HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys, int waitMs, CancellationToken cancellationToken)
    {
        StagedBaseVerdictAsks?.Enqueue((node, waitMs));

        if (StagedBaseVerdictsStalledHook is { } stalled && stalled(node, waitMs))
        {
            if (waitMs > 0)
                await Task.Delay(waitMs, cancellationToken);

            KeyValueStagedBaseVerdictEntry[] notApplied = new KeyValueStagedBaseVerdictEntry[keys.Count];
            for (int i = 0; i < keys.Count; i++)
                notApplied[i] = new KeyValueStagedBaseVerdictEntry(KeyValueStagedBaseVerdict.NotApplied, -1);

            return (true, notApplied);
        }

        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.GetStagedBaseVerdictsLocal(partitionId, transactionId, epoch, keys, waitMs, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<byte[]?> LookupTransactionRecord(string node, int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LookupTransactionRecordLocal(partitionId, transactionId, epoch, anchorKey, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryCommitNodeMutations(
        string node, 
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken
    )
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            ConcurrentBag<(KeyValueResponseType, string, long, KeyValueDurability)> bag = [];

            foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) in xkeys)
            {
                // Same transient fault seam as the single-key inter-node commit: a faulted participant's commit
                // never reaches the remote leader, so its prepare survives for recovery to drive.
                if (CommitMutationsFault is not null && CommitMutationsFault(transactionId, key))
                {
                    bag.Add((KeyValueResponseType.MustRetry, key, 0, durability));
                    continue;
                }

                (KeyValueResponseType type, long commitIndex) = await kahunaNode.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancellationToken);
                bag.Add((type, key, commitIndex, durability));
            }

            AddToCommitMutationsResponses(bag, lockSync, responses);
            return;
        }
        
        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void AddToCommitMutationsResponses(
        ConcurrentBag<(KeyValueResponseType, string, long, KeyValueDurability)> bag, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses
    )
    {
        foreach ((KeyValueResponseType type, string key, long commitIndex, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, key, commitIndex, durability));
        }
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            RollbackObserver?.Invoke(transactionId, key);
            return await kahunaNode.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryRollbackNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            ConcurrentBag<(KeyValueResponseType, string, long, KeyValueDurability)> bag = [];

            foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) in xkeys)
            {
                RollbackObserver?.Invoke(transactionId, key);
                (KeyValueResponseType type, long commitIndex) = await kahunaNode.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancellationToken);
                bag.Add((type, key, commitIndex, durability));
            }

            AddToRollbackMutationsResponses(bag, lockSync, responses);
            return;
        }
        
        throw Unreachable(node);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void AddToRollbackMutationsResponses(
        ConcurrentBag<(KeyValueResponseType, string, long, KeyValueDurability)> bag, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses
    )
    {
        foreach ((KeyValueResponseType type, string key, long commitIndex, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, key, commitIndex, durability));
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByBucketResult> GetByBucket(string node, HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<KeyValueGetByRangeResult> GetByRange(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, bool snapshotAtLeader = false)
    {
        Interlocked.Increment(ref getByRangeCallCount);

        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            // This call re-enters the receiving node's locator, so it must carry the
            // forwarded-request marker: a non-hosting receiver answers MustRetry instead of
            // forwarding onward, mirroring the gRPC server-batcher receive path.
            using Kahuna.Server.ForwardedRequestScope.Scope forwardedScope = Kahuna.Server.ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, snapshotAtLeader: snapshotAtLeader);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByBucketResult> ScanByPrefix(string node, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, bool includeTombstones, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
            return await kahunaNode.ScanByPrefix(prefixedKey, readTimestamp, durability, includeTombstones);

        throw Unreachable(node);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="options"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndStartTransaction(options, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<(KeyValueResponseType, string?)> CommitTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndCommitTransaction(handle, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<KeyValueResponseType> RollbackTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndRollbackTransaction(handle, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy)> BeginOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref beginOperationCallCount);

            // Route through the locator: the sender may have guessed a coordinator-partition
            // replica that does not hold the session; a hosting non-leader receiver redirects
            // once to its accurately-resolved local leader instead of answering
            // RejectedSessionClosed from the wrong node's session table.
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref completeOperationCallCount);

            // Test seam: simulate a completion that never reaches the coordinator (the record stays
            // pending) so participant-side retry recovery can be exercised. The throw happens before the
            // coordinator is touched, matching a lost/failed inbound RPC.
            if (CompleteOperationFault is not null && CompleteOperationFault(transactionId, operationId))
                throw new KahunaServerException("Injected completion fault (test seam).");

            // Test seam: simulate a non-throwing "not delivered" response (e.g. a leader swap between
            // the routing decision and the RPC landing) — returns MustRetry without calling the
            // coordinator, exercising the aliasing path that previously returned a silent false-ack.
            if (CompleteOperationRedirectFault is not null && CompleteOperationRedirectFault(transactionId, operationId))
                return (KeyValueResponseType.MustRetry, null);

            return await kahunaNode.CompleteOperationInbound(coordinatorKey, transactionId, operationId, payload);
        }

        throw Unreachable(node);
    }

    /// <summary>
    /// Test seam: when set and it returns true for a given completion, that <see cref="CompleteOperation"/>
    /// call throws before reaching the coordinator, leaving the operation record pending so a same-id
    /// retry must recover it from the participant cache instead of reapplying the operation.
    /// </summary>
    public Func<HLCTimestamp, TransactionOperationId, bool>? CompleteOperationFault { get; set; }

    /// <summary>
    /// Test seam: when set and it returns true for a given completion, that <see cref="CompleteOperation"/>
    /// call returns <c>(MustRetry, null)</c> without reaching the coordinator, simulating the non-throwing
    /// not-delivered outcome that arises when the target node loses its coordinator-partition leadership
    /// between the routing decision and the RPC landing.
    /// </summary>
    public Func<HLCTimestamp, TransactionOperationId, bool>? CompleteOperationRedirectFault { get; set; }

    /// <summary>
    /// Test seam: when set and it returns true for a given (transaction, key), that inter-node
    /// <see cref="TryCommitMutations"/> call returns <c>MustRetry</c> before reaching the remote leader, so the
    /// participant's prepare survives there. Used to force a partial durable commit — the anchor commits while a
    /// secondary stays pending — and prove recovery drives that surviving prepare to completion.
    /// </summary>
    public Func<HLCTimestamp, string, bool>? CommitMutationsFault { get; set; }

    /// <summary>
    /// When set and it returns true for a given (transactionId, key), the inter-node <see cref="TryCommitMutations"/>
    /// call lets the commit apply on the remote leader — installing the value, receipt, and (for an anchor) the
    /// durable decision record — but returns <c>MustRetry</c> to the caller, hiding the success. Models a
    /// committed-but-lost response so the coordinator must consult the durable record instead of assuming failure.
    /// </summary>
    public Func<HLCTimestamp, string, bool>? CommitMutationsResponseFault { get; set; }

    /// <summary>
    /// When set, invoked for every (transactionId, key) whose rollback is issued over the inter-node transport,
    /// just before it reaches the remote leader. Lets a test observe that a prepared participant's ticket was
    /// actually driven to rollback — as opposed to being orphaned by a rollback that threw before issuing any RPC.
    /// </summary>
    public Action<HLCTimestamp, string>? RollbackObserver { get; set; }

    public async Task<TransactionWorkingSet?> GetTransactionWorkingSet(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
            return await Task.FromResult(kahunaNode.GetTransactionWorkingSet(transactionId));

        throw Unreachable(node);
    }

    public async Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
            return await kahunaNode.CloseTransaction(transactionId, cancellationToken);

        throw Unreachable(node);
    }

    public async Task<bool> EnsureKeyRangeSeeded(string node, string keySpace, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.RegisterKeyRangeAsync(keySpace, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<bool> EnsureKeyRangeRemoved(string node, string keySpace, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.RemoveKeyRangeAsync(keySpace, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<List<KeyValueRangeLock>> GetRangeLocks(string node, string keySpace, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
            return await kahunaNode.GetRangeLocks(keySpace);

        throw Unreachable(node);
    }

    public async Task ImportRangeLocks(string node, string keySpace, List<KeyValueRangeLock> locks, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            await kahunaNode.ImportRangeLocks(keySpace, locks);
            return;
        }

        throw Unreachable(node);
    }

    public async Task<bool> ImportCompletionReceipts(string node, int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts, CancellationToken cancellationToken, bool forget = false)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
            return forget
                ? await kahunaNode.ForgetCompletionReceiptsReplicated(partitionId, receipts)
                : await kahunaNode.ImportCompletionReceiptsReplicated(partitionId, receipts);

        throw Unreachable(node);
    }

    public async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)>
        AcquireSnapshotHold(string node, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndAcquireSnapshotHold(holderId, timestamp, leaseMs, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)>
        RenewSnapshotHold(string node, string holdId, int leaseMs, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndRenewSnapshotHold(holdId, leaseMs, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<KeyValueResponseType>
        ReleaseSnapshotHold(string node, string holdId, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.LocateAndReleaseSnapshotHold(holdId, cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<(KeyValueResponseType Type, HLCTimestamp Floor, int LiveHolds)>
        GetSnapshotFloor(string node, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.GetSnapshotFloor(cancellationToken);
        }

        throw Unreachable(node);
    }

    public async Task<(KeyValueResponseType Type, KeyValueApplyFingerprint Fingerprint)>
        GetPartitionApplyFingerprint(string node, int partitionId, CancellationToken cancellationToken)
    {
        if (TryGetNode(node, out IKahuna? kahunaNode))
        {
            using ForwardedRequestScope.Scope forwardedScope = ForwardedRequestScope.Enter();

            return await kahunaNode.GetPartitionApplyFingerprint(partitionId, cancellationToken);
        }

        throw Unreachable(node);
    }
}
