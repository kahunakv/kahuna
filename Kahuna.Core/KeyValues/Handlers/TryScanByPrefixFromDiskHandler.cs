
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
    public Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        return Task.FromResult(ExecuteCore(message));
    }

    private KeyValueResponse ExecuteCore(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        HLCTimestamp readTimestamp = message.ReadTimestamp;

        // Stage 2 dispatch: detach the full prefix scan (plus optional per-key snapshot
        // revision walk) off the actor mailbox.
        // Non-snapshot requests coalesce: multiple callers for the same prefix share one disk
        // read. Snapshot requests are not coalesced because their result depends on readTimestamp.
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
            context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        TaskCompletionSource<KeyValueResponse?> promise = actorContext.Reply.Value.Promise;

        // (prefix, -3, false) = non-snapshot prefix-from-disk scan.
        // Snapshot scans are not registered in PendingReads (no coalescing).
        bool isNonSnapshot = readTimestamp.IsNull();
        (string, long, bool)? scanKey = isNonSnapshot ? (message.Key, -3L, false) : null;

        if (scanKey.HasValue &&
            context.PendingReads.TryGetValue(scanKey.Value, out ReadContinuation? inflight))
        {
            inflight.AddWaiter(promise);
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        PrefixFromDiskScanContinuation cont = new(message.Key, readTimestamp, currentTime, promise, scanKey);
        if (scanKey.HasValue)
            context.PendingReads[scanKey.Value] = cont;

        Task<List<(string, ReadOnlyKeyValueEntry)>> readTask;
        try
        {
            // Route the disk scan to the FairReadScheduler partition that owns this data range,
            // not message.PartitionId (=0 for scans), matching the point-read path. Enqueuing
            // scans under partition 0 collapses per-partition fairness/back-pressure and ordering.
            readTask = context.Raft.ReadScheduler.EnqueueTask(
                ResolvePartition(message.Key),
                () =>
                {
                    List<(string, ReadOnlyKeyValueEntry)> scanned =
                        context.PersistenceBackend.GetKeyValueByPrefix(message.Key);

                    if (readTimestamp.IsNull())
                        return scanned;

                    // Snapshot scan: the prefix scan returns each key's latest committed revision.
                    // When that revision is newer than the snapshot, walk the persisted revision
                    // history backwards to the most recent revision at-or-before the snapshot,
                    // mirroring the in-memory scan path. A key with no retained revision
                    // at-or-before the snapshot is dropped (it did not exist at that time).
                    List<(string, ReadOnlyKeyValueEntry)> projected = new(scanned.Count);

                    foreach ((string key, ReadOnlyKeyValueEntry entry) in scanned)
                    {
                        if (entry.LastModified.CompareTo(readTimestamp) <= 0)
                        {
                            projected.Add((key, entry));
                            continue;
                        }

                        KeyValueEntry? snapshot = context.PersistenceBackend.GetKeyValueRevisionAtOrBefore(
                            key, entry.Revision - 1, readTimestamp);
                        if (snapshot is null || snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined)
                            continue;
                        if (snapshot.Expires != HLCTimestamp.Zero && snapshot.Expires.CompareTo(currentTime) < 0)
                            continue;
                        projected.Add((key, new(snapshot.Value, snapshot.Revision,
                            snapshot.Expires, snapshot.LastUsed, snapshot.LastModified, snapshot.State)));
                    }

                    return projected;
                });
        }
        catch (Exception ex)
        {
            if (scanKey.HasValue)
                context.PendingReads.Remove(scanKey.Value);
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
