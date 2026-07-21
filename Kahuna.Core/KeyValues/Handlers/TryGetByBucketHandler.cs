
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryGetByBucketHandler : BaseHandler
{
    public TryGetByBucketHandler(KeyValueContext context) : base(context)
    {

    }

    /// <summary>
    /// Executes the get by bucket request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return GetByBucketEphemeral(message);

        return Task.FromResult(GetByBucketPersistent(message));
    }

    private async Task<KeyValueResponse> GetByBucketEphemeral(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // Bound inspected entries, not just returned ones: a run of interleaved tombstones/expired rows
        // must not make this synchronous walk scan the whole resident bucket on the mailbox thread.
        int inspectionBudget = KeyValueScanLimits.MaxPrefixScanResults + KeyValueScanLimits.MaxScanInspectionSlack;
        int inspected = 0;
        bool truncated = false;

        foreach ((string key, KeyValueEntry? entry) in context.Store.GetByBucket(message.Key))
        {
            if (entry is null)
                continue;

            inspected++;

            KeyValueResponse? result = BucketScanContinuation.EvaluateEntry(
                context, currentTime, message.TransactionId, message.ReadTimestamp, key, entry);

            if (result is null || result.Type == KeyValueResponseType.DoesNotExist)
            {
                if (inspected >= inspectionBudget)
                {
                    truncated = true;
                    break;
                }
                continue;
            }

            if (result.Type != KeyValueResponseType.Get || result.Entry is null)
                return new(result.Type, []);

            items.Add((key, result.Entry));

            if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;

            if (inspected >= inspectionBudget)
            {
                truncated = true;
                break;
            }
        }

        if (truncated)
            context.Logger.LogWarning(
                "Ephemeral bucket scan of {Prefix} stopped after inspecting {Inspected} entries with {Collected} results; " +
                "bucket is tombstone-dense — use the paginated range scan for a complete result set",
                message.Key, inspected, items.Count);

        items.Sort(EnsureLexicographicalOrder);

        // Durable-intent bucket visibility: overlay prepared intents belonging to this bucket. No-op off the
        // durable path.
        (items, bool mustRetry) = BucketScanContinuation.OverlayBucketIntents(
            context, message.Key, items, currentTime, message.ReadTimestamp);
        if (mustRetry)
            return KeyValueStaticResponses.MustRetryResponse;

        return new(KeyValueResponseType.Get, items);
    }

    private KeyValueResponse GetByBucketPersistent(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        HashSet<string> seenKeys = [];

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // Stage 1: in-memory scan (actor thread, synchronous).
        // context.Store.GetByBucket only yields keys already in the resident store, so
        // EvaluateEntry never issues a disk read from this loop.
        foreach ((string key, KeyValueEntry? entry) in context.Store.GetByBucket(message.Key))
        {
            if (entry is null)
                continue;

            KeyValueResponse? result = BucketScanContinuation.EvaluateEntry(
                context, currentTime, message.TransactionId, message.ReadTimestamp, key, entry);

            if (result is null || result.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (result.Type != KeyValueResponseType.Get || result.Entry is null)
                return new(result.Type, []); // scan-aborting response (WaitingForReplication, etc.)

            seenKeys.Add(key);
            items.Add((key, result.Entry));

            if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;
        }

        // If the cap was already reached from memory, skip the disk read entirely.
        if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
        {
            items.Sort(EnsureLexicographicalOrder);

            (items, bool mustRetry) = BucketScanContinuation.OverlayBucketIntents(
                context, message.Key, items, currentTime, message.ReadTimestamp);
            if (mustRetry)
                return KeyValueStaticResponses.MustRetryResponse;

            return new(KeyValueResponseType.Get, items);
        }

        // Stage 2 dispatch: detach GetKeyValueByPrefix off the actor mailbox.
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
            context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        TaskCompletionSource<KeyValueResponse?> promise = actorContext.Reply.Value.Promise!;

        // Only plain (non-transactional, non-snapshot) bucket scans are coalesced.
        // A transactional scan builds MVCC entries under its own txId; a snapshot scan
        // reads as-of a specific readTimestamp — both produce caller-specific results
        // that must not be shared with a different caller's scan of the same prefix.
        // Such scans still detach but use a private, unregistered continuation.
        bool isCoalesceable = message.TransactionId == HLCTimestamp.Zero && message.ReadTimestamp.IsNull();

        // (prefix, -2, false) = plain bucket scan coalescing slot. Sentinel -2 is distinct
        // from latest-point (-1) and by-revision (>= 0) so bucket scans never cross-coalesce
        // with point reads.
        (string, long, bool)? scanKey = isCoalesceable ? (message.Key, -2L, false) : null;

        if (scanKey.HasValue && context.PendingReads.TryGetValue(scanKey.Value, out ReadContinuation? inflight))
        {
            if (!inflight.AddWaiter(promise))
                return KeyValueStaticResponses.MustRetryResponse;
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        BucketScanContinuation cont = new(
            message.Key, message.TransactionId, message.ReadTimestamp,
            items, seenKeys, currentTime, promise, scanKey);
        ArmReadDeadline(cont, currentTime);
        if (scanKey.HasValue)
            context.PendingReads[scanKey.Value] = cont;

        bool isSnapshotScan = !message.ReadTimestamp.IsNull();
        HLCTimestamp capturedReadTs = message.ReadTimestamp;

        Task<(List<(string, ReadOnlyKeyValueEntry)>, Dictionary<string, ReadOnlyKeyValueEntry>?)> readTask;
        try
        {
            // Route the disk scan to the FairReadScheduler partition that owns this data range,
            // not message.PartitionId (which the manager leaves at 0 for scans). Enqueuing every
            // scan under partition 0 would collapse per-partition fairness/back-pressure and the
            // read-after-write FIFO ordering onto a single scheduler queue. Point reads resolve
            // the partition the same way.
            readTask = context.Raft.ReadScheduler.EnqueueTask(
                ResolvePartition(message.Key),
                () => ProjectBucketPage(context.PersistenceBackend.GetKeyValueByPrefix(message.Key),
                    isSnapshotScan, capturedReadTs, currentTime, context.PersistenceBackend));
        }
        catch (Exception ex)
        {
            if (scanKey.HasValue)
                context.PendingReads.Remove(scanKey.Value);
            context.Logger.LogWarning(
                "KeyValueActor/BucketScan: read scheduler rejected enqueue for prefix {Prefix}: {Ex}",
                message.Key, ex.Message);
            cont.Resolve(KeyValueStaticResponses.MustRetryResponse);
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.MustRetryResponse;
        }

        _ = readTask.ContinueWith(t =>
        {
            if (!t.IsCompletedSuccessfully) cont.SetFaulted();
            else (cont.ScanDiskResult, cont.SnapshotProjections) = t.Result;
            actorContext.Self.Send(
                new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont });
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Builds the raw disk-prefix page and, for snapshot scans, resolves per-key snapshot
    /// projections in the same scheduler task. Must run on the off-actor scheduler thread —
    /// <c>GetKeyValueRevisionAtOrBefore</c> does I/O and must not block the actor mailbox.
    ///
    /// For each disk row:
    /// <list type="bullet">
    ///   <item>If <c>LastModified ≤ readTimestamp</c>, the row is already the at-or-before
    ///   version — stored in the projections dict as-is so <c>EvaluateEntry</c> can serve it
    ///   if the in-memory archive has been trimmed beyond the snapshot.</item>
    ///   <item>If <c>LastModified > readTimestamp</c>, look up the highest revision
    ///   at-or-before the snapshot via <c>GetKeyValueRevisionAtOrBefore</c>; drop the key
    ///   from projections if null, Deleted, Undefined, or expired.</item>
    /// </list>
    /// </summary>
    internal static (List<(string, ReadOnlyKeyValueEntry)> Raw, Dictionary<string, ReadOnlyKeyValueEntry>? Projections)
        ProjectBucketPage(
            List<(string, ReadOnlyKeyValueEntry)> raw,
            bool isSnapshotScan,
            HLCTimestamp readTimestamp,
            HLCTimestamp currentTime,
            IPersistenceBackend backend)
    {
        if (!isSnapshotScan)
            return (raw, null);

        Dictionary<string, ReadOnlyKeyValueEntry> projections = new(raw.Count);
        foreach ((string k, ReadOnlyKeyValueEntry e) in raw)
        {
            if (e.LastModified.CompareTo(readTimestamp) <= 0)
            {
                // Row is already at-or-before the snapshot; store it directly so the fallback
                // can serve it when a resident entry supersedes it and the archive is trimmed.
                projections[k] = e;
                continue;
            }

            // Current disk row was written after the snapshot; resolve the at-or-before revision.
            KeyValueEntry? snap = backend.GetKeyValueRevisionAtOrBefore(k, e.Revision - 1, readTimestamp);
            if (snap is null || snap.State is KeyValueState.Deleted or KeyValueState.Undefined)
                continue;
            if (snap.Expires != HLCTimestamp.Zero && snap.Expires.CompareTo(currentTime) < 0)
                continue;
            projections[k] = new ReadOnlyKeyValueEntry(
                snap.Value, snap.Revision, snap.Expires, snap.LastUsed, snap.LastModified, snap.State);
        }
        return (raw, projections);
    }

    private static int EnsureLexicographicalOrder((string, ReadOnlyKeyValueEntry) x, (string, ReadOnlyKeyValueEntry) y)
    {
        return string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
    }
}
