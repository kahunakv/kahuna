
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
/// <summary>Per-cycle eviction statistics captured by TryCollectHandler.Execute.</summary>
internal readonly record struct CollectCycleStats(
    int TombstoneEvicted,
    int ExpiryEvicted,
    int LruEvicted,
    int LruVisited,
    int TotalEvicted,
    long ElapsedMs
);

internal sealed class TryCollectHandler : BaseHandler
{
    private readonly HashSet<string> keysToEvict = [];
    private CollectCycleStats lastCycleStats;

    public TryCollectHandler(KeyValueContext context) : base(context)
    {

    }

    public bool IsOverBudget() => context.IsOverStoreBudget();

    /// <summary>Stats from the most recent Execute() call. Zero-initialised before the first call.</summary>
    public CollectCycleStats LastCycleStats => lastCycleStats;

    public void Execute()
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        int tombstoneEvicted = 0;
        int expiryEvicted = 0;
        int lruEvicted = 0;
        int evicted = 0;
        int batchMax = context.CollectBatchMax;
        KahunaConfiguration config = context.Configuration;
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

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

        // Step 1a: drain tombstone queue — Deleted/Undefined entries.
        // Dirty or intent-held tombstones are collected in deferredTombstones and re-enqueued
        // AFTER the drain loop so they are not re-processed in the same cycle.
        // Intent-held entries will also be re-added by the commit/rollback handler, but the
        // duplicate is harmless — lazy re-validation discards stale queue entries on the next pop.
        //
        // Snapshot the queue depth at entry so deferred (non-evicted) entries don't cause the loop
        // to process more than min(batchMax, snapshot) items per cycle. Without the snapshot the
        // loop would drain the entire queue on every cycle — O(tombstone-backlog) — because
        // deferred entries never increment `evicted`.
        List<string>? deferredTombstones = null;
        int tombstoneDrainLimit = Math.Min(batchMax, context.TombstoneQueue.Count);
        int tombstoneDrained = 0;
        while (evicted < batchMax && tombstoneDrained < tombstoneDrainLimit && context.TombstoneQueue.TryDequeue(out string? tombstoneKey))
        {
            tombstoneDrained++;
            if (!context.Store.TryGetValue(tombstoneKey, out KeyValueEntry? tombstoneEntry))
                continue; // already evicted by another path

            if (tombstoneEntry.State is not (KeyValueState.Deleted or KeyValueState.Undefined))
                continue; // stale queue entry — entry was re-set after the tombstone was enqueued

            if (HasLiveWriteIntent(tombstoneEntry, currentTime) || tombstoneEntry.ReplicationIntent is not null)
            {
                deferredTombstones ??= [];
                deferredTombstones.Add(tombstoneKey);
                continue;
            }

            if (tombstoneEntry.IsDirty(safetyWindowMs, currentTime))
            {
                deferredTombstones ??= [];
                deferredTombstones.Add(tombstoneKey);
                continue;
            }

            keysToEvict.Add(tombstoneKey);
            evicted++;
            tombstoneEvicted++;
        }

        if (deferredTombstones is not null)
            foreach (string key in deferredTombstones)
                context.TombstoneQueue.Enqueue(key);

        // Step 1b: drain expiry heap — entries whose TTL has elapsed.
        // Pop while the earliest-expiring entry is past its deadline, then re-validate:
        //   • key still in store
        //   • Expires on the live entry matches the heap priority (not stale after an Extend)
        //   • entry is not dirty or intent-held
        // Dirty or intent-held expired entries are deferred into deferredExpiry and re-enqueued
        // AFTER the loop — re-enqueueing inside the loop would cause an immediate re-pop (the
        // entry's expiry is already ≤ now), resulting in an infinite spin on the same key.
        List<(string Key, HLCTimestamp Expiry)>? deferredExpiry = null;
        while (evicted < batchMax)
        {
            if (!context.ExpiryHeap.TryPeek(out string? expiredKey, out HLCTimestamp heapExpiry))
                break; // heap empty

            if ((heapExpiry - currentTime) > TimeSpan.Zero)
                break; // earliest entry has not yet elapsed; nothing later can have either

            context.ExpiryHeap.Dequeue();

            if (!context.Store.TryGetValue(expiredKey, out KeyValueEntry? expiredEntry))
                continue; // evicted by another path

            if (expiredEntry.Expires != heapExpiry)
                continue; // stale: TryExtend pushed the deadline forward

            if (expiredEntry.Expires == HLCTimestamp.Zero || (expiredEntry.Expires - currentTime) > TimeSpan.Zero)
                continue; // defensive: expiry cleared or not yet due

            if (HasLiveWriteIntent(expiredEntry, currentTime) || expiredEntry.ReplicationIntent is not null)
            {
                deferredExpiry ??= [];
                deferredExpiry.Add((expiredKey, heapExpiry));
                continue;
            }

            if (expiredEntry.IsDirty(safetyWindowMs, currentTime))
            {
                deferredExpiry ??= [];
                deferredExpiry.Add((expiredKey, heapExpiry));
                continue;
            }

            keysToEvict.Add(expiredKey);
            evicted++;
            expiryEvicted++;
        }

        if (deferredExpiry is not null)
            foreach ((string key, HLCTimestamp expiry) in deferredExpiry)
                context.ExpiryHeap.Enqueue(key, expiry);

        // Intentional asymmetry: IsOverStoreBudget (the entry gate) includes heap/queue node
        // overhead so that stale-node accumulation triggers collection. IsProjectedOverBudget
        // (the LRU loop guard) uses raw store bytes only — heap bloat should be relieved by the
        // drain loops above, not by LRU-evicting live entries to compensate for phantom bytes.
        long projectedBytes = context.ApproximateStoreBytes - EstimateEvictionBytes(keysToEvict);
        int projectedCount = context.Store.Count - keysToEvict.Count;

        // Step 2: intrusive O(1) LRU — walk from head (coldest) toward tail (hottest), evicting
        // eligible entries until under budget or batchMax reached. Dirty and intent-held entries
        // are skipped (advanced past), not evicted. Because the list is not modified until
        // RemoveStoreEntry runs after the loop, LruNext pointers remain stable during the walk.
        int lruVisited = 0;
        KeyValueEntry? lruCandidate = context.LruHead;
        while (IsProjectedOverBudget(projectedCount, projectedBytes, config)
            && evicted < batchMax
            && lruCandidate is not null)
        {
            lruVisited++;
            KeyValueEntry? next = lruCandidate.LruNext;
            string? candidateKey = lruCandidate.StoreKey;

            if (candidateKey is not null
                && !keysToEvict.Contains(candidateKey)
                && !HasLiveWriteIntent(lruCandidate, currentTime)
                && lruCandidate.ReplicationIntent is null
                && !lruCandidate.IsDirty(safetyWindowMs, currentTime))
            {
                keysToEvict.Add(candidateKey);
                evicted++;
                lruEvicted++;
                projectedCount--;
                projectedBytes -= (candidateKey.Length * sizeof(char)) + lruCandidate.CachedBytes;
            }

            lruCandidate = next;
        }

#if DEBUG
        // The LRU walk must be O(evicted), not O(Store.Count). Visiting more than
        // lruEvicted + batchMax nodes means the walk ran through the whole list without
        // finding enough eligible entries — acceptable under heavy dirty/intent pressure, but
        // visiting more than the whole store indicates a bug (cycle, phantom node).
        System.Diagnostics.Debug.Assert(
            lruVisited <= context.Store.Count,
            $"LRU walk visited {lruVisited} entries but store has only {context.Store.Count}");
#endif

        foreach (string key in keysToEvict)
            context.RemoveStoreEntry(key);

        bool backlog = IsProjectedOverBudget(projectedCount, projectedBytes, config) && evicted >= batchMax;

        if (keysToEvict.Count > 0)
        {
            context.Logger.LogKeyValueEviction(
                keysToEvict.Count,
                tombstoneEvicted,
                expiryEvicted,
                lruEvicted,
                context.Store.Count,
                context.ApproximateStoreBytes,
                stopwatch.ElapsedMilliseconds,
                backlog
            );
        }

        lastCycleStats = new(tombstoneEvicted, expiryEvicted, lruEvicted, lruVisited, evicted, stopwatch.ElapsedMilliseconds);
        keysToEvict.Clear();

        if (backlog)
            context.ScheduleFollowUpCollect();
    }

    /// <summary>
    /// Reports whether the entry holds a write intent that is still live, clearing the intent in
    /// place when it has expired. The op handlers (TryGet/TrySet/TryExists/…) already treat an
    /// expired intent as gone and null it lazily on access; the collector must apply the same rule
    /// or a cold key whose owning transaction was abandoned would pin the entry against eviction
    /// forever (and re-enqueue itself on the tombstone/expiry path every cycle). An intent with
    /// Expires == Zero is an unprepared lock/intent with no determined deadline and is treated as live.
    /// </summary>
    private static bool HasLiveWriteIntent(KeyValueEntry entry, HLCTimestamp currentTime)
    {
        KeyValueWriteIntent? intent = entry.WriteIntent;

        if (intent is null)
            return false;

        if (intent.Expires != HLCTimestamp.Zero && (intent.Expires - currentTime) <= TimeSpan.Zero)
        {
            entry.WriteIntent = null;
            return false;
        }

        return true;
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
