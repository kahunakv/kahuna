
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
/// approximate LRU eviction runs when the actor exceeds entry or byte budgets; idle-TTL eviction
/// runs when <see cref="KahunaConfiguration.CacheEntryTtl"/> is configured.
/// </summary>
/// <summary>Per-cycle eviction statistics captured by TryCollectHandler.Execute.</summary>
internal readonly record struct CollectCycleStats(
    int TombstoneEvicted,
    int ExpiryEvicted,
    int ExpiryInspected,
    int LruEvicted,
    int LruVisited,
    int IdleEvicted,
    int TotalEvicted,
    bool Backlog,
    long ElapsedMs
);

internal sealed class TryCollectHandler : BaseHandler
{
    private readonly HashSet<string> keysToEvict = [];
    private CollectCycleStats lastCycleStats;

    // Resume point for the bounded LRU walk. Persisted across collect cycles so a large pinned
    // (dirty/intent-held) prefix is not re-inspected from the head every cycle: each cycle advances
    // the cursor by at most the inspection budget and self-schedules a follow-up until the walk
    // reaches the end. Null = start from the coldest entry. A cursor whose entry was evicted
    // (StoreKey cleared) is abandoned and the walk restarts from the head.
    private KeyValueEntry? lruCursor;

    // Resumable cursor over predicate/range lock bucket keys for the periodic expired-lock sweep.
    // Rebuilt from the live lock dictionaries once exhausted; advanced by at most the inspection
    // budget per cycle so a large lock table is never swept in a single mailbox turn.
    private List<string>? lockSweepKeys;
    private int lockSweepPos;

    // Resume point for the bounded MVCC read-snapshot sweep. A transactional read of a non-expiring key
    // leaves a zero-expiry MVCC snapshot behind, and TrimExpiredMvccEntries only fires when that key is
    // next committed/rolled-back/unlocked — so a read-only key that is never written again would retain the
    // orphaned snapshot indefinitely. This sweep walks the store and trims dead-session snapshots even for
    // cold keys, advancing by at most the inspection budget per cycle and resuming here next cycle. Null =
    // start from the coldest entry; a cursor whose entry was evicted (StoreKey cleared) restarts from head.
    private KeyValueEntry? mvccSweepCursor;

    // Resume point for the bounded idle-TTL sweep. When CacheEntryTtl is configured, this walk
    // evicts clean entries that have not been touched within the TTL window, independent of budget.
    // Null = start from the coldest entry. A cursor whose entry was evicted is abandoned and the
    // next cycle restarts from the head.
    private KeyValueEntry? lruIdleCursor;

    public TryCollectHandler(KeyValueContext context) : base(context)
    {

    }

    public bool IsOverBudget() => context.IsOverStoreBudget();

    /// <summary>Stats from the most recent Execute() call. Zero-initialised before the first call.</summary>
    public CollectCycleStats LastCycleStats => lastCycleStats;

    public void Execute()
    {
        // Resolve any orphaned phase-two entries (worker died / completion dropped) before the sweep of
        // resident entries — the periodic collect is the reliable trigger that fires even when idle.
        SweepExpiredPhaseTwos();

        Stopwatch stopwatch = Stopwatch.StartNew();
        int tombstoneEvicted = 0;
        int expiryEvicted = 0;
        int lruEvicted = 0;
        int evicted = 0;
        int batchMax = context.CollectBatchMax;
        // Cap on how many entries a single cycle may *inspect* (not just evict) in the expiry and LRU
        // loops, mirroring the tombstone-drain snapshot cap. Without it, a backlog of expired-but-dirty
        // entries (which never increment `evicted`) or an all-pinned store would let one cycle scan
        // O(backlog) / O(store) on the mailbox thread, parking the actor. Work beyond the budget is
        // carried to a self-scheduled follow-up collect.
        int inspectionMax = batchMax;
        KahunaConfiguration config = context.Configuration;
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // A dirty entry (Revision > FlushedRevision) is never evicted until the background writer
        // acknowledges its flush — there is no time-based override. This trades a bounded memory
        // pin under a stalled backend for the guarantee that a committed-but-unflushed revision's
        // only cached copy is never dropped.

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

            if (tombstoneEntry.IsDirty())
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
        int expiryInspected = 0;
        while (evicted < batchMax && expiryInspected < inspectionMax)
        {
            if (!context.ExpiryHeap.TryPeek(out string? expiredKey, out HLCTimestamp heapExpiry))
                break; // heap empty

            if ((heapExpiry - currentTime) > TimeSpan.Zero)
                break; // earliest entry has not yet elapsed; nothing later can have either

            context.ExpiryHeap.Dequeue();
            expiryInspected++;

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

            if (expiredEntry.IsDirty())
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
        // eligible entries until under budget, the eviction budget, or the inspection budget is
        // reached. Dirty and intent-held entries are skipped (advanced past), not evicted. Because
        // the list is not modified until RemoveStoreEntry runs after the loop, LruNext pointers
        // remain stable during the walk.
        //
        // Resume from the persisted cursor (the point the previous cycle stopped at) so a large
        // pinned prefix is not re-walked every cycle; fall back to the coldest entry when the cursor
        // was evicted/cleared or the store is already under budget.
        int lruVisited = 0;
        KeyValueEntry? lruCandidate =
            IsProjectedOverBudget(projectedCount, projectedBytes, config)
            && lruCursor is not null && lruCursor.StoreKey is not null
                ? lruCursor
                : context.LruHead;
        lruCursor = null;

        while (IsProjectedOverBudget(projectedCount, projectedBytes, config)
            && evicted < batchMax
            && lruVisited < inspectionMax
            && lruCandidate is not null)
        {
            lruVisited++;
            KeyValueEntry? next = lruCandidate.LruNext;
            string? candidateKey = lruCandidate.StoreKey;

            if (candidateKey is not null
                && !keysToEvict.Contains(candidateKey)
                && !HasLiveWriteIntent(lruCandidate, currentTime)
                && lruCandidate.ReplicationIntent is null
                && !lruCandidate.IsDirty())
            {
                keysToEvict.Add(candidateKey);
                evicted++;
                lruEvicted++;
                projectedCount--;
                projectedBytes -= (candidateKey.Length * sizeof(char)) + lruCandidate.CachedBytes;
            }

            lruCandidate = next;
        }

        // If the walk stopped with the store still over budget and more nodes left to inspect
        // (cut short by the eviction or inspection budget, not by reaching the end), remember where
        // to resume so the next cycle continues forward instead of re-scanning the pinned prefix.
        if (lruCandidate is not null && IsProjectedOverBudget(projectedCount, projectedBytes, config))
            lruCursor = lruCandidate;

#if DEBUG
        // The LRU walk is bounded to the inspection budget per cycle, never O(Store.Count).
        System.Diagnostics.Debug.Assert(
            lruVisited <= inspectionMax,
            $"LRU walk visited {lruVisited} entries but the per-cycle inspection budget is {inspectionMax}");
#endif

        // Step 2b: idle-TTL eviction — evict clean entries that have not been accessed within
        // CacheEntryTtl, independent of budget pressure. The LRU list is ordered coldest → hottest,
        // so we walk from the head and stop as soon as we reach an entry used within the TTL window
        // (everything hotter is at least as recent). Dirty and intent-held entries are skipped with
        // the same guards as the budget-LRU walk.
        int idleEvicted = SweepIdleEntries(currentTime, inspectionMax);

        // Step 3: sweep abandoned predicate/range locks. A lock is otherwise cleared only by a
        // matching release, so a transaction that acquires and never releases (crash, abandoned
        // client) would pin the record on a cold key space forever. Bounded by the inspection budget
        // and resumed via lockSweepKeys so the sweep never scans the whole lock table in one turn.
        SweepExpiredPredicateLocks(currentTime, inspectionMax);

        // Step 4: expire in-flight resumable reads whose deadline has passed. A hung or slow backend
        // read otherwise leaves its coalesced waiters parked indefinitely; resolving them with a
        // retryable result lets the callers retry, and marking the continuation cancelled makes a
        // late stage-3 completion a no-op. Only registered (coalesceable) reads are swept here.
        SweepExpiredReads(currentTime);

        foreach (string key in keysToEvict)
            context.RemoveStoreEntry(key);

        // Step 5: reclaim dead-session MVCC read snapshots on cold keys. TrimExpiredMvccEntries otherwise
        // only fires when a key is next written/unlocked, so a key that is read once under a transaction and
        // never touched again would keep its orphaned zero-expiry snapshot forever. Bounded by the inspection
        // budget and resumed via mvccSweepCursor so the whole store is never swept in one mailbox turn; the
        // sweep completes over successive natural collect cycles (reclamation is eventual, so it does not
        // force a follow-up).
        SweepDeadMvccSnapshots(currentTime, inspectionMax);

        // Self-schedule a follow-up when work was carried past this cycle's budget: the LRU walk was
        // cut short with the store still over budget (lruCursor set), or the expiry loop hit its
        // inspection cap while still making progress (more evictable expired entries likely remain).
        // A completed full pass (cursor null) does not re-schedule, so an all-pinned store cannot spin.
        bool backlog = lruCursor is not null
            || (expiryInspected >= inspectionMax && expiryEvicted > 0);

        if (keysToEvict.Count > 0)
        {
            context.Logger.LogKeyValueEviction(
                keysToEvict.Count,
                tombstoneEvicted,
                expiryEvicted,
                lruEvicted,
                idleEvicted,
                context.Store.Count,
                context.ApproximateStoreBytes,
                stopwatch.ElapsedMilliseconds,
                backlog
            );
        }

        lastCycleStats = new(tombstoneEvicted, expiryEvicted, expiryInspected, lruEvicted, lruVisited, idleEvicted, evicted + idleEvicted, backlog, stopwatch.ElapsedMilliseconds);
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

    /// <summary>
    /// Prunes expired prefix and range locks, inspecting at most <paramref name="budget"/> bucket keys
    /// this cycle and resuming from where the previous cycle stopped. The cursor snapshot is rebuilt
    /// from the live dictionaries only once it is exhausted, so a lock table larger than the budget is
    /// swept incrementally across successive collect cycles instead of all at once on the mailbox thread.
    /// </summary>
    private void SweepExpiredPredicateLocks(HLCTimestamp currentTime, int budget)
    {
        if (context.LocksByPrefix.Count == 0 && context.LocksByRange.Count == 0)
        {
            lockSweepKeys = null;
            lockSweepPos = 0;
            return;
        }

        if (lockSweepKeys is null || lockSweepPos >= lockSweepKeys.Count)
        {
            lockSweepKeys = new List<string>(context.LocksByPrefix.Count + context.LocksByRange.Count);
            foreach (string key in context.LocksByPrefix.Keys)
                lockSweepKeys.Add(key);
            foreach (string key in context.LocksByRange.Keys)
                lockSweepKeys.Add(key);
            lockSweepPos = 0;
        }

        int inspected = 0;
        while (lockSweepPos < lockSweepKeys.Count && inspected < budget)
        {
            string bucket = lockSweepKeys[lockSweepPos++];
            inspected++;

            if (context.LocksByPrefix.TryGetValue(bucket, out KeyValueWriteIntent? prefixIntent)
                && prefixIntent.Expires != HLCTimestamp.Zero
                && prefixIntent.Expires - currentTime <= TimeSpan.Zero)
                context.LocksByPrefix.Remove(bucket);

            if (context.LocksByRange.TryGetValue(bucket, out List<KeyValueRangeLock>? rangeLocks)
                && RangeLockChecks.PruneExpired(rangeLocks, currentTime, int.MaxValue))
                context.LocksByRange.Remove(bucket);
        }
    }

    /// <summary>
    /// Expires registered in-flight reads whose deadline has passed: resolves their waiters with a
    /// retryable result and marks each continuation cancelled so a late completion is dropped. The
    /// expired entries are collected first, then expired, because <see cref="ReadContinuation.Expire"/>
    /// removes the continuation from <c>PendingReads</c> (mutating it mid-enumeration would throw).
    /// </summary>
    private void SweepExpiredReads(HLCTimestamp currentTime)
    {
        if (context.PendingReads.Count == 0)
            return;

        List<ReadContinuation>? expired = null;
        foreach (KeyValuePair<(string, long, bool), ReadContinuation> kv in context.PendingReads)
        {
            if (kv.Value.IsExpired(currentTime))
                (expired ??= []).Add(kv.Value);
        }

        if (expired is null)
            return;

        foreach (ReadContinuation cont in expired)
            cont.Expire(context, KeyValueStaticResponses.MustRetryResponse);
    }

    /// <summary>
    /// Walks the LRU list from the coldest end, evicting clean entries whose idle age exceeds
    /// <see cref="KahunaConfiguration.CacheEntryTtl"/>. Runs independently of budget pressure;
    /// skips dirty and intent-held entries with the same guards as the budget-LRU walk.
    /// Because the list is ordered coldest → hottest, the walk stops early at the first entry
    /// used within the TTL window — every subsequent entry is at least as recently used.
    /// Resumes from <c>lruIdleCursor</c> across collect cycles so a large store is not
    /// scanned in a single mailbox turn.
    /// </summary>
    private int SweepIdleEntries(HLCTimestamp currentTime, int budget)
    {
        TimeSpan ttl = context.Configuration.CacheEntryTtl;
        if (ttl <= TimeSpan.Zero)
            return 0;

        KeyValueEntry? candidate =
            lruIdleCursor is not null && lruIdleCursor.StoreKey is not null
                ? lruIdleCursor
                : context.LruHead;
        lruIdleCursor = null;

        int idleEvicted = 0;
        int inspected = 0;
        while (candidate is not null && inspected < budget)
        {
            inspected++;
            KeyValueEntry? next = candidate.LruNext;
            string? candidateKey = candidate.StoreKey;

            // The LRU list is coldest → hottest (TouchEntry moves accessed entries to the tail), so
            // once we reach an entry used within the TTL window all subsequent entries are equally or
            // more recently used and we can stop. This assumes LastUsed is monotone with list order;
            // if an access ever recorded a stale LastUsed, some idle entries past this point are simply
            // deferred to a later cycle (this only ever under-evicts, never drops a live entry).
            if ((currentTime - candidate.LastUsed) <= ttl)
                break;

            if (candidateKey is not null
                && !keysToEvict.Contains(candidateKey)
                && !HasLiveWriteIntent(candidate, currentTime)
                && candidate.ReplicationIntent is null
                && !candidate.IsDirty())
            {
                keysToEvict.Add(candidateKey);
                idleEvicted++;
            }

            candidate = next;
        }

        // Stopped on the inspection budget mid-list: resume here next cycle.
        if (candidate is not null)
            lruIdleCursor = candidate;

        return idleEvicted;
    }

    /// <summary>
    /// Walks the store from the persisted cursor (coldest first), trimming dead-session zero-expiry MVCC read
    /// snapshots from at most <paramref name="budget"/> entries this cycle and resuming from where it stopped.
    /// The walk follows the intrusive LRU list and does not modify it, so <c>LruNext</c> pointers stay stable;
    /// <see cref="TrimExpiredMvccEntries"/> only mutates an entry's MvccEntries. When the walk reaches the end
    /// the cursor is cleared so the next cycle restarts from the head.
    /// </summary>
    private void SweepDeadMvccSnapshots(HLCTimestamp currentTime, int budget)
    {
        KeyValueEntry? candidate =
            mvccSweepCursor is not null && mvccSweepCursor.StoreKey is not null
                ? mvccSweepCursor
                : context.LruHead;
        mvccSweepCursor = null;

        int inspected = 0;
        while (candidate is not null && inspected < budget)
        {
            inspected++;
            KeyValueEntry? next = candidate.LruNext;

            if (candidate.MvccEntries is { Count: > 0 })
                TrimExpiredMvccEntries(candidate, currentTime);

            candidate = next;
        }

        // Stopped mid-list on the inspection budget: resume here next cycle rather than re-scanning the head.
        if (candidate is not null)
            mvccSweepCursor = candidate;
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
