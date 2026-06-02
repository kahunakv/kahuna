
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
                ref lruSampleCursorKey
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
                    projectedBytes -= KeyValueStoreAccounting.EstimateEntryBytes(key, entry);
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

            long bytesBefore = KeyValueStoreAccounting.EstimateEntryBytes(key, entry);
            trimmed += TrimRevisions(entry, revisionRetention);
            trimmed += TrimMvccEntries(entry, currentTime);
            long bytesAfter = KeyValueStoreAccounting.EstimateEntryBytes(key, entry);
            context.AdjustEstimatedEntryBytes(bytesAfter - bytesBefore);
        }

        return trimmed;
    }

    private static int TrimRevisions(KeyValueEntry entry, int revisionRetention)
    {
        if (entry.Revisions is null || entry.Revisions.Count <= revisionRetention)
            return 0;

        List<long> staleRevisions = entry.Revisions.Keys
            .OrderByDescending(static revision => revision)
            .Skip(revisionRetention)
            .ToList();

        foreach (long revision in staleRevisions)
            entry.Revisions.Remove(revision);

        return staleRevisions.Count;
    }

    private static int TrimMvccEntries(KeyValueEntry entry, HLCTimestamp currentTime)
    {
        if (entry.MvccEntries is null || entry.MvccEntries.Count == 0)
            return 0;

        List<HLCTimestamp> staleTransactions = [];

        foreach ((HLCTimestamp transactionId, KeyValueMvccEntry mvccEntry) in entry.MvccEntries)
        {
            if (mvccEntry.Expires == HLCTimestamp.Zero)
                continue;

            if ((mvccEntry.Expires - currentTime) > TimeSpan.Zero)
                continue;

            staleTransactions.Add(transactionId);
        }

        foreach (HLCTimestamp transactionId in staleTransactions)
            entry.MvccEntries.Remove(transactionId);

        return staleTransactions.Count;
    }

    private long EstimateEvictionBytes(HashSet<string> keys)
    {
        long bytes = 0;

        foreach (string key in keys)
        {
            if (context.Store.TryGetValue(key, out KeyValueEntry? entry))
                bytes += KeyValueStoreAccounting.EstimateEntryBytes(key, entry);
        }

        return bytes;
    }

    private static bool IsProjectedOverBudget(int projectedCount, long projectedBytes, KahunaConfiguration config)
    {
        return projectedCount > config.MaxEntriesPerActor
            || projectedBytes > config.MaxBytesPerActor;
    }
}
