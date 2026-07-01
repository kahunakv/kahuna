
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

        foreach ((string key, KeyValueEntry? entry) in context.Store.GetByBucket(message.Key))
        {
            if (entry is null)
                continue;

            KeyValueResponse? result = BucketScanContinuation.EvaluateEntry(
                context, currentTime, message.TransactionId, message.ReadTimestamp, key, entry);

            if (result is null || result.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (result.Type != KeyValueResponseType.Get || result.Entry is null)
                return new(result.Type, []);

            items.Add((key, result.Entry));

            if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;
        }

        items.Sort(EnsureLexicographicalOrder);

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
            return new(KeyValueResponseType.Get, items);
        }

        // Stage 2 dispatch: detach GetKeyValueByPrefix off the actor mailbox.
        // Register a coalescing slot so concurrent requests for the same prefix share one disk read.
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
            context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        TaskCompletionSource<KeyValueResponse?> promise = actorContext.Reply.Value.Promise;

        // (prefix, -2, false) = bucket scan. Sentinel -2 is distinct from latest-point (-1)
        // and by-revision (>= 0) slots so bucket scans never cross-coalesce with point reads.
        (string, long, bool) scanKey = (message.Key, -2L, false);

        if (context.PendingReads.TryGetValue(scanKey, out ReadContinuation? inflight))
        {
            inflight.AddWaiter(promise);
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        BucketScanContinuation cont = new(
            message.Key, message.TransactionId, message.ReadTimestamp,
            items, seenKeys, currentTime, promise);
        context.PendingReads[scanKey] = cont;

        Task<List<(string, ReadOnlyKeyValueEntry)>> readTask;
        try
        {
            readTask = context.Raft.ReadScheduler.EnqueueTask(
                message.PartitionId,
                () => context.PersistenceBackend.GetKeyValueByPrefix(message.Key));
        }
        catch (Exception ex)
        {
            context.PendingReads.Remove(scanKey);
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
            else cont.ScanDiskResult = t.Result;
            actorContext.Self.Send(
                new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont });
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    private static int EnsureLexicographicalOrder((string, ReadOnlyKeyValueEntry) x, (string, ReadOnlyKeyValueEntry) y)
    {
        return string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
    }
}
