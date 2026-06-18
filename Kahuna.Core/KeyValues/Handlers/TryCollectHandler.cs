
using System.Diagnostics;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles periodic collection and eviction of key-value pairs: garbage reclamation always runs;
/// approximate LRU eviction runs when the actor exceeds entry or byte budgets.
/// </summary>
internal sealed class TryCollectHandler : BaseHandler
{
    private readonly HashSet<string> keysToEvict = [];
    private string? lruSampleCursorKey;
    private int collectCycleCount;
    
    public TryCollectHandler(KeyValueContext context) : base(context)
    {
        
    }

    public bool IsOverBudget() => context.IsOverStoreBudget();

    public void Execute()
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        int garbageEvicted = 0;
        int lruEvicted = 0;
        int metadataTrimmed = 0;
        int evicted = 0;
        int batchMax = context.CollectBatchMax;
        KahunaConfiguration config = context.Configuration;
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        collectCycleCount++;
        bool trimMetadata = ShouldTrimMetadata(config);
        List<string>? metadataCandidates = trimMetadata ? [] : null;

        // Dirty-entry safety window.  The time-guard proxy for "not yet flushed" is:
        //   entry.IsDirty(safetyWindowMs, currentTime) = Revision > FlushedRevision
        //                                                && (now - LastModified) < safetyWindowMs
        // safetyWindowMs is floored at 10 000 ms so that the guard survives BackgroundWriterActor's
        // worst-case flush path: DirtyObjectsWriterDelay tick + up to 5 retry rounds of
        // DecorrelatedJitterBackoffV2(median 1000 ms) ≈ DirtyObjectsWriterDelay + ~10 000 ms.
        // Without the floor, the standalone default (200 ms × 2 = 400 ms) would not cover a
        // single retry cycle.  If FlushedRevision is eventually advanced by a flush-ack signal
        // (the spec's primary approach) the window can be tightened or removed.
        long rawDelayMs = config.DirtyObjectsWriterDelay > 0 ? config.DirtyObjectsWriterDelay : 5000L;
        long safetyWindowMs = Math.Max(rawDelayMs * 2, 10_000L);

        // Step 1: always reclaim pure garbage (no entry floor).
        foreach (KeyValuePair<string, KeyValueEntry> key in context.Store.GetItems())
        {
            if (metadataCandidates is not null && MightNeedMetadataTrim(key.Value, config))
                metadataCandidates.Add(key.Key);

            if (evicted >= batchMax)
            {
                if (metadataCandidates is null)
                    break;

                continue;
            }

            if (key.Value.WriteIntent is not null || key.Value.ReplicationIntent is not null)
                continue;

            if (key.Value.IsDirty(safetyWindowMs, currentTime))
                continue;

            if (key.Value.State is KeyValueState.Deleted or KeyValueState.Undefined)
            {
                keysToEvict.Add(key.Key);
                evicted++;
                garbageEvicted++;
                continue;
            }

            if (key.Value.Expires == HLCTimestamp.Zero)
                continue;

            if ((key.Value.Expires - currentTime) > TimeSpan.Zero)
                continue;

            keysToEvict.Add(key.Key);
            evicted++;
            garbageEvicted++;
        }

        long projectedBytes = context.ApproximateStoreBytes - EstimateEvictionBytes(keysToEvict);
        int projectedCount = context.Store.Count - keysToEvict.Count;

        // Step 2: bounded approximate LRU when over budget.
        int sampleSize = config.LruSampleSize;
        int scanMax = config.LruSampleScanMax;

        while (IsProjectedOverBudget(projectedCount, projectedBytes, config) && evicted < batchMax)
        {
            List<string> victims = KeyValueCollectSampler.SampleOldestVictims(
                context.Store,
                keysToEvict,
                currentTime,
                sampleSize,
                scanMax,
                batchMax - evicted,
                ref lruSampleCursorKey,
                safetyWindowMs: safetyWindowMs
            );

            if (victims.Count == 0)
                break;

            foreach (string key in victims)
            {
                if (evicted >= batchMax)
                    break;

                if (!IsProjectedOverBudget(projectedCount, projectedBytes, config))
                    break;

                if (!keysToEvict.Add(key))
                    continue;

                evicted++;
                lruEvicted++;

                projectedCount--;
                if (context.Store.TryGetValue(key, out KeyValueEntry? entry))
                    projectedBytes -= (key.Length * sizeof(char)) + entry.CachedBytes;
            }
        }

        foreach (string key in keysToEvict)
            context.RemoveStoreEntry(key);

        if (metadataCandidates is not null)
            metadataTrimmed = TrimMetadataCandidates(metadataCandidates, currentTime, config.RevisionRetention);

        bool backlog = IsProjectedOverBudget(projectedCount, projectedBytes, config) && evicted >= batchMax;

        if (keysToEvict.Count > 0 || metadataTrimmed > 0)
        {
            context.Logger.LogKeyValueEviction(
                keysToEvict.Count,
                garbageEvicted,
                lruEvicted,
                metadataTrimmed,
                context.Store.Count,
                context.ApproximateStoreBytes,
                stopwatch.ElapsedMilliseconds,
                backlog
            );
        }

        keysToEvict.Clear();

        if (backlog)
            context.ScheduleFollowUpCollect();
    }

    private bool ShouldTrimMetadata(KahunaConfiguration config)
    {
        if (config.MetadataTrimInterval <= 0)
            return false;

        return collectCycleCount % config.MetadataTrimInterval == 0;
    }

    private static bool MightNeedMetadataTrim(KeyValueEntry entry, KahunaConfiguration config)
    {
        if (entry.Revisions is not null && entry.Revisions.Count > config.RevisionRetention)
            return true;

        if (entry.MvccEntries is not null && entry.MvccEntries.Count > 0)
            return true;

        return false;
    }

    private int TrimMetadataCandidates(List<string> candidates, HLCTimestamp currentTime, int revisionRetention)
    {
        int trimmed = 0;

        foreach (string key in candidates)
        {
            if (!context.Store.TryGetValue(key, out KeyValueEntry? entry))
                continue;

            (int revCount, long revBytes) = TrimRevisions(entry, revisionRetention);
            (int mvccCount, long mvccBytes) = TrimMvccEntries(entry, currentTime);

            trimmed += revCount + mvccCount;

            long bytesFreed = revBytes + mvccBytes;
            if (bytesFreed != 0)
                context.AdjustEstimatedEntryBytes(entry, -bytesFreed);
        }

        return trimmed;
    }

    private static (int count, long bytesFreed) TrimRevisions(KeyValueEntry entry, int revisionRetention)
    {
        if (entry.Revisions is null || entry.Revisions.Count <= revisionRetention)
            return (0, 0);

        List<long> staleRevisions = entry.Revisions.Keys
            .OrderByDescending(static revision => revision)
            .Skip(revisionRetention)
            .ToList();

        long bytesFreed = 0;
        foreach (long revision in staleRevisions)
        {
            if (entry.Revisions.Remove(revision, out KeyValueRevisionEntry removed))
                bytesFreed += KeyValueStoreAccounting.EstimateRevisionRemovedBytes(entry.Revisions.Count == 0, removed.Value);
        }

        return (staleRevisions.Count, bytesFreed);
    }

    private static (int count, long bytesFreed) TrimMvccEntries(KeyValueEntry entry, HLCTimestamp currentTime)
    {
        if (entry.MvccEntries is null || entry.MvccEntries.Count == 0)
            return (0, 0);

        List<HLCTimestamp> staleTransactions = [];

        foreach ((HLCTimestamp transactionId, KeyValueMvccEntry mvccEntry) in entry.MvccEntries)
        {
            if (mvccEntry.Expires == HLCTimestamp.Zero)
                continue;

            if ((mvccEntry.Expires - currentTime) > TimeSpan.Zero)
                continue;

            staleTransactions.Add(transactionId);
        }

        long bytesFreed = 0;
        foreach (HLCTimestamp transactionId in staleTransactions)
        {
            if (entry.MvccEntries.Remove(transactionId, out KeyValueMvccEntry? removedMvcc))
                bytesFreed += KeyValueStoreAccounting.MvccEntryRemovedBytes(false, removedMvcc.Value);
        }

        // Reclaim the dictionary overhead if the last entry was just removed.
        if (staleTransactions.Count > 0 && entry.MvccEntries.Count == 0)
            bytesFreed += KeyValueStoreAccounting.DictionaryOverheadBytes;

        return (staleTransactions.Count, bytesFreed);
    }

    private long EstimateEvictionBytes(HashSet<string> keys)
    {
        long bytes = 0;

        foreach (string key in keys)
        {
            if (context.Store.TryGetValue(key, out KeyValueEntry? entry))
                bytes += (key.Length * sizeof(char)) + entry.CachedBytes;
        }

        return bytes;
    }

    private static bool IsProjectedOverBudget(int projectedCount, long projectedBytes, KahunaConfiguration config)
    {
        return projectedCount > config.MaxEntriesPerActor
            || projectedBytes > config.MaxBytesPerActor;
    }

}
