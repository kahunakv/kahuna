
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
    public ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return GetByBucketEphemeral(message);

        return ValueTask.FromResult(GetByBucketPersistent(message));
    }

    private async ValueTask<KeyValueResponse> GetByBucketEphemeral(KeyValueRequest message)
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
            context, message.Key, items, currentTime, message.ReadTimestamp, message.TransactionId, message.ForeignScanDecisions);
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
                context, message.Key, items, currentTime, message.ReadTimestamp, message.TransactionId, message.ForeignScanDecisions);
            if (mustRetry)
                return KeyValueStaticResponses.MustRetryResponse;

            return new(KeyValueResponseType.Get, items);
        }

        // Stage 2 dispatch: detach GetKeyValueByPrefix off the actor mailbox.
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
            context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        KeyValueReplyRef promise = KeyValueReplyRef.From(actorContext.Reply.Value);

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

        // The prefix's intents as they stand before the disk read: a settlement that lands between the read and
        // the stage-3 merge removes an intent the page was read without (see BucketScanContinuation).
        IReadOnlyList<Transactions.Data.PreparedIntent>? capturedIntents = context.PreparedIntentStore?.SnapshotPrefix(message.Key);

        BucketScanContinuation cont = new(
            message.Key, message.TransactionId, message.ReadTimestamp,
            items, seenKeys, currentTime, promise, scanKey, message.ForeignScanDecisions, capturedIntents);
        ArmReadDeadline(cont, currentTime);
        if (scanKey.HasValue)
            context.PendingReads[scanKey.Value] = cont;

        bool isSnapshotScan = !message.ReadTimestamp.IsNull();
        HLCTimestamp capturedReadTs = message.ReadTimestamp;

        // Copy into a local before capturing: the deadline can resolve the continuation (and
        // complete the caller, which returns the pooled request) before the scheduler runs the
        // closure — capturing the request itself would read a cleared or re-rented message.
        string bucketKey = message.Key;

        // The production backend is the unflushed-writes overlay; its index says which keys still have a
        // revision queued for the writer, which the as-of projection must not read around.
        UnflushedKeyValueWritesIndex? unflushed = context.UnflushedWrites;

        Task<(List<(string, ReadOnlyKeyValueEntry)>, Dictionary<string, ReadOnlyKeyValueEntry>?, Dictionary<string, long>?)> readTask;
        try
        {
            // Route the disk scan to the FairReadScheduler partition that owns this data range,
            // not message.PartitionId (which the manager leaves at 0 for scans). Enqueuing every
            // scan under partition 0 would collapse per-partition fairness/back-pressure and the
            // read-after-write FIFO ordering onto a single scheduler queue. Point reads resolve
            // the partition the same way.
            readTask = context.BackendReadScheduler.EnqueueTask(
                ResolvePartition(message.Key),
                () =>
                {
                    // The scheduler can run this long after the deadline sweep expired the
                    // continuation; skip the disk work then — the stage-3 late-completion guard
                    // drops the result anyway.
                    if (cont.Cancelled)
                    {
                        KeyValueScanMetrics.ScansAbandonedCancelled.Add(1);
                        return (new List<(string, ReadOnlyKeyValueEntry)>(),
                            (Dictionary<string, ReadOnlyKeyValueEntry>?)null, (Dictionary<string, long>?)null);
                    }

                    if (!isSnapshotScan)
                        return (context.PersistenceBackend.GetKeyValueByPrefix(bucketKey),
                            (Dictionary<string, ReadOnlyKeyValueEntry>?)null, (Dictionary<string, long>?)null);

                    // Snapshot scan: one backend pass resolves the raw head page and every key's
                    // as-of image together, instead of one revision-history read per stale key.
                    return ProjectBucketPage(
                        context.PersistenceBackend.GetKeyValueByPrefixAtOrBefore(
                            bucketKey, capturedReadTs, () => cont.Cancelled),
                        capturedReadTs, currentTime, unflushed);
                });
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
            else (cont.ScanDiskResult, cont.SnapshotProjections, cont.HistoryFencedKeys) = t.Result;
            actorContext.Self.Send(
                new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont });
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Splits a snapshot-scan backend page into the raw head rows and the per-key snapshot
    /// projections. Runs on the off-actor scheduler thread as part of the stage-2 task.
    ///
    /// For each key of the as-of page:
    /// <list type="bullet">
    ///   <item>When the head row is already at-or-before the snapshot, its as-of image is the
    ///   head itself — stored in the projections dict as-is so <c>EvaluateEntry</c> can serve it
    ///   if the in-memory archive has been trimmed beyond the snapshot.</item>
    ///   <item>Otherwise the as-of image is a historical revision; drop the key from projections
    ///   if there is none, or it is Deleted, Undefined, or expired.</item>
    /// </list>
    /// The history can answer for such a key only below the oldest revision of it still queued for the
    /// background writer (<see cref="UnflushedHistoryFence"/>): a queued revision the as-of image should
    /// be is missing from it, and the projection would be an older row, or no row for a key whose first
    /// revision is the queued one. Such a key is named in the returned map with the ceiling the history is
    /// complete below, and its projection (taken by the backend from the flushed rows, so from below that
    /// ceiling) is kept: it is the as-of image exactly when every queued revision is past the snapshot.
    /// Stage 3 serves the key from a resident entry, whose archive holds the queued revisions and so can
    /// tell, and answers MustRetry when the key is not resident (a row this actor does not own is served
    /// from the projection alone, so nothing else could fence it).
    /// </summary>
    internal static (List<(string, ReadOnlyKeyValueEntry)> Raw, Dictionary<string, ReadOnlyKeyValueEntry>? Projections, Dictionary<string, long>? HistoryFencedKeys)
        ProjectBucketPage(
            List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> asOfPage,
            HLCTimestamp readTimestamp,
            HLCTimestamp currentTime,
            UnflushedKeyValueWritesIndex? unflushed = null)
    {
        List<(string, ReadOnlyKeyValueEntry)> raw = new(asOfPage.Count);
        Dictionary<string, ReadOnlyKeyValueEntry> projections = new(asOfPage.Count);
        Dictionary<string, long>? fencedKeys = null;

        foreach ((string k, ReadOnlyKeyValueEntry current, ReadOnlyKeyValueEntry? snap) in asOfPage)
        {
            raw.Add((k, current));

            if (current.LastModified.CompareTo(readTimestamp) <= 0)
            {
                // Head is already at-or-before the snapshot; store it directly so the fallback
                // can serve it when a resident entry supersedes it and the archive is trimmed.
                projections[k] = current;
                continue;
            }

            // Head was written after the snapshot; the as-of image is a historical revision, which is
            // not complete while a revision below the head is still queued for the writer. The backend's
            // projection below the flushed rows is kept for the resident entry to judge (see the summary).
            if (UnflushedHistoryFence.HistoryMayLag(unflushed, k, archive: null, current.Revision - 1, out long flushedCeiling))
                (fencedKeys ??= new(StringComparer.Ordinal))[k] = flushedCeiling;

            if (snap is null || snap.State is KeyValueState.Deleted or KeyValueState.Undefined)
                continue;
            if (snap.Expires != HLCTimestamp.Zero && snap.Expires.CompareTo(currentTime) < 0)
                continue;
            projections[k] = snap;
        }

        return (raw, projections, fencedKeys);
    }

    private static int EnsureLexicographicalOrder((string, ReadOnlyKeyValueEntry) x, (string, ReadOnlyKeyValueEntry) y)
    {
        return string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
    }
}
