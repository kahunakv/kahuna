
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Represents a handler that attempts to scan key-value entries from disk based on a specified prefix.
/// </summary>
/// <remarks>
/// This handler asks the backend persistence to scan the key-value store for entries that match the given prefix.
/// </remarks>
internal sealed class TryScanByPrefixFromDiskHandler : BaseHandler
{
    public TryScanByPrefixFromDiskHandler(KeyValueContext context) : base(context)
    {

    }

    /// <summary>
    /// Executes the scan by prefix from disk request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        return ValueTask.FromResult(ExecuteCore(message));
    }

    private KeyValueResponse ExecuteCore(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        HLCTimestamp readTimestamp = message.ReadTimestamp;

        // Stage 2 dispatch: detach the full prefix scan (plus the snapshot as-of projection)
        // off the actor mailbox.
        // Both shapes coalesce: multiple callers for the same prefix (and, for snapshot scans,
        // the same read timestamp) share one disk read. Snapshot coalescing matters under retry
        // storms — a caller that timed out and retries the same snapshot must attach to the
        // in-flight read instead of enqueueing another full-cost scan behind it.
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
            context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        KeyValueReplyRef promise = KeyValueReplyRef.From(actorContext.Reply.Value);

        // (prefix, -3, includeTombstones) = non-snapshot prefix-from-disk scan. The tombstone flag is
        // part of the coalescing key so a tombstone-carrying scan never shares a continuation (and its
        // filtered result) with a plain scan for the same prefix.
        // Snapshot scans coalesce in their own map, keyed additionally by the read timestamp,
        // because their result depends on it.
        bool isNonSnapshot = readTimestamp.IsNull();
        (string, long, bool)? scanKey = isNonSnapshot ? (message.Key, -3L, message.IncludeTombstones) : null;
        (string, HLCTimestamp, bool)? snapshotScanKey = isNonSnapshot ? null : (message.Key, readTimestamp, message.IncludeTombstones);

        ReadContinuation? inflight = null;
        if (scanKey.HasValue)
            context.PendingReads.TryGetValue(scanKey.Value, out inflight);
        else if (snapshotScanKey.HasValue)
            context.PendingSnapshotPrefixScans.TryGetValue(snapshotScanKey.Value, out inflight);

        if (inflight is not null)
        {
            if (!inflight.AddWaiter(promise))
                return KeyValueStaticResponses.MustRetryResponse;
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        PrefixFromDiskScanContinuation cont = new(message.Key, readTimestamp, currentTime, promise, scanKey, snapshotScanKey, message.IncludeTombstones);
        ArmReadDeadline(cont, currentTime);
        if (scanKey.HasValue)
            context.PendingReads[scanKey.Value] = cont;
        else if (snapshotScanKey.HasValue)
            context.PendingSnapshotPrefixScans[snapshotScanKey.Value] = cont;

        // Copy into a local before capturing: the deadline can resolve the continuation (and
        // complete the caller, which returns the pooled request) before the scheduler runs the
        // closure — capturing the request itself would read a cleared or re-rented message.
        string prefixKey = message.Key;

        Task<List<(string, ReadOnlyKeyValueEntry)>> readTask;
        try
        {
            // Route the disk scan to the FairReadScheduler partition that owns this data range,
            // not message.PartitionId (=0 for scans), matching the point-read path. Enqueuing
            // scans under partition 0 collapses per-partition fairness/back-pressure and ordering.
            readTask = context.BackendReadScheduler.EnqueueTask(
                ResolvePartition(message.Key),
                () =>
                {
                    // The scheduler can run this long after the deadline sweep expired the
                    // continuation and resolved its waiters with MustRetry. Skip the disk work
                    // then: the result would be dropped by the stage-3 late-completion guard, and
                    // an abandoned full-prefix scan still costs a full read of the range.
                    if (cont.Cancelled)
                    {
                        KeyValueScanMetrics.ScansAbandonedCancelled.Add(1);
                        return new List<(string, ReadOnlyKeyValueEntry)>();
                    }

                    if (readTimestamp.IsNull())
                        return context.PersistenceBackend.GetKeyValueByPrefix(prefixKey);

                    // Snapshot scan: resolve every key's as-of image in one backend pass. The
                    // backend keeps polling the cancellation flag so an expired scan stops
                    // mid-range instead of running to completion. A key with no committed version
                    // at-or-before the snapshot is dropped (it did not exist at that time);
                    // deleted and expired as-of entries are kept — stage 3 applies the tombstone
                    // and expiry policy for both scan shapes.
                    List<(string, ReadOnlyKeyValueEntry, ReadOnlyKeyValueEntry?)> asOf =
                        context.PersistenceBackend.GetKeyValueByPrefixAtOrBefore(
                            prefixKey, readTimestamp, () => cont.Cancelled);

                    List<(string, ReadOnlyKeyValueEntry)> projected = new(asOf.Count);

                    foreach ((string key, _, ReadOnlyKeyValueEntry? snapshot) in asOf)
                    {
                        if (snapshot is not null)
                            projected.Add((key, snapshot));
                    }

                    return projected;
                });
        }
        catch (Exception ex)
        {
            cont.RemovePendingKey(context);
            context.Logger.LogWarning(
                "KeyValueActor/PrefixFromDiskScan: read scheduler rejected enqueue for prefix {Prefix}: {Ex}",
                message.Key, ex.Message);
            cont.Resolve(KeyValueStaticResponses.MustRetryResponse);
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.MustRetryResponse;
        }

        _ = readTask.ContinueWith(t =>
        {
            if (!t.IsCompletedSuccessfully) cont.SetFaulted();
            else cont.ScanDiskResult = t.Result;
            actorContext.Self.Send(
                new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont });
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }
}
