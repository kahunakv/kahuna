using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Redis-style approximate LRU: examine a bounded, randomly positioned slice of the store
/// and pick the oldest <see cref="KeyValueEntry.LastUsed"/> candidates from that window.
/// A persistent cursor plus random jitter avoids always scanning from the lexicographically
/// smallest keys. Each call performs at most two O(log n) seeks and O(jitter + scanMax)
/// eligible-key visits.
/// </summary>
internal static class KeyValueCollectSampler
{
    internal static List<string> SampleOldestVictims(
        BTree<string, KeyValueEntry> store,
        HashSet<string> excludedKeys,
        HLCTimestamp currentTime,
        int sampleSize,
        int scanMax,
        int maxVictims,
        ref string? cursorKey,
        int jitterOverride = -1,
        long safetyWindowMs = 0
    )
    {
        if (store.Count == 0 || scanMax <= 0 || sampleSize <= 0 || maxVictims <= 0)
            return [];

        if (cursorKey is not null && !store.ContainsKey(cursorKey))
            cursorKey = null;

        int jitter = jitterOverride >= 0
            ? jitterOverride
            : scanMax > 1 ? Random.Shared.Next(0, scanMax) : 0;

        List<(string Key, HLCTimestamp LastUsed)> pool = new(sampleSize);
        SampleState state = new(jitter, scanMax, sampleSize, currentTime, excludedKeys, pool, safetyWindowMs);

        WalkRing(store, cursorKey, ref state);

        if (state.Examined == 0)
        {
            state.FirstSkippedKey = null;
            WalkRing(store, cursorKey, ref state);
        }

        if (state.LastExamined is not null)
            cursorKey = state.LastExamined;

        if (pool.Count == 0)
            return [];

        pool.Sort((left, right) => CompareIdleAge(currentTime, right.LastUsed, left.LastUsed));

        int take = Math.Min(Math.Min(pool.Count, sampleSize), maxVictims);
        List<string> victims = new(take);

        for (int i = 0; i < take; i++)
            victims.Add(pool[i].Key);

        return victims;
    }

    private static void WalkRing(
        BTree<string, KeyValueEntry> store,
        string? afterKey,
        ref SampleState state
    )
    {
        int ringEligible = 0;

        foreach (KeyValuePair<string, KeyValueEntry> item in EnumerateCircularExclusiveAfter(store, afterKey))
        {
            if (state.Examined >= state.ScanMax)
                break;

            if (!IsEligible(item, state.ExcludedKeys, state.CurrentTime, state.SafetyWindowMs))
                continue;

            ringEligible++;
            state.LastExamined = item.Key;

            if (state.JitterRemaining > 0)
            {
                if (state.FirstSkippedKey is not null && item.Key == state.FirstSkippedKey)
                    state.JitterRemaining = 0;
                else
                {
                    if (state.FirstSkippedKey is null)
                        state.FirstSkippedKey = item.Key;

                    state.JitterRemaining--;
                    if (state.JitterRemaining > 0)
                        continue;
                }
            }

            state.Examined++;
            Consider(state.Pool, state.SampleSize, state.CurrentTime, item.Key, item.Value.LastUsed);
        }

        if (state.JitterRemaining > 0 && ringEligible > 0)
            state.JitterRemaining %= ringEligible;
    }

    /// <summary>
    /// Circular key order starting strictly after <paramref name="afterKey"/>, or the full tree
    /// when <paramref name="afterKey"/> is null. When <paramref name="afterKey"/> is set, a
    /// second seeked segment wraps from the tree minimum.
    /// </summary>
    private static IEnumerable<KeyValuePair<string, KeyValueEntry>> EnumerateCircularExclusiveAfter(
        BTree<string, KeyValueEntry> store,
        string? afterKey
    )
    {
        if (afterKey is null)
        {
            foreach (KeyValuePair<string, KeyValueEntry> item in store.GetItems())
                yield return item;

            yield break;
        }

        foreach (KeyValuePair<string, KeyValueEntry> item in store.GetByRange(
            afterKey,
            startInclusive: false,
            end: null,
            endInclusive: true,
            limit: int.MaxValue))
        {
            yield return item;
        }

        foreach (KeyValuePair<string, KeyValueEntry> item in store.GetItems("", afterKey))
        {
            if (item.Key == afterKey)
                yield break;

            yield return item;
        }
    }

    private static bool IsEligible(KeyValuePair<string, KeyValueEntry> item, HashSet<string> excludedKeys, HLCTimestamp currentTime, long safetyWindowMs)
    {
        if (excludedKeys.Contains(item.Key))
            return false;

        if (item.Value.WriteIntent is not null || item.Value.ReplicationIntent is not null)
            return false;

        if (safetyWindowMs > 0 && item.Value.IsDirty(safetyWindowMs, currentTime))
            return false;

        return true;
    }

    private static int CompareIdleAge(HLCTimestamp currentTime, HLCTimestamp left, HLCTimestamp right)
    {
        return (currentTime - left).CompareTo(currentTime - right);
    }

    private static void Consider(
        List<(string Key, HLCTimestamp LastUsed)> pool,
        int capacity,
        HLCTimestamp currentTime,
        string key,
        HLCTimestamp lastUsed
    )
    {
        if (pool.Count < capacity)
        {
            pool.Add((key, lastUsed));
            return;
        }

        int mostRecentIndex = 0;

        for (int i = 1; i < pool.Count; i++)
        {
            if (CompareIdleAge(currentTime, pool[i].LastUsed, pool[mostRecentIndex].LastUsed) < 0)
                mostRecentIndex = i;
        }

        if (CompareIdleAge(currentTime, lastUsed, pool[mostRecentIndex].LastUsed) > 0)
            pool[mostRecentIndex] = (key, lastUsed);
    }

    private struct SampleState
    {
        public int JitterRemaining;
        public int Examined;
        public int ScanMax;
        public int SampleSize;
        public HLCTimestamp CurrentTime;
        public HashSet<string> ExcludedKeys;
        public List<(string Key, HLCTimestamp LastUsed)> Pool;
        public string? FirstSkippedKey;
        public string? LastExamined;
        public long SafetyWindowMs;

        public SampleState(
            int jitter,
            int scanMax,
            int sampleSize,
            HLCTimestamp currentTime,
            HashSet<string> excludedKeys,
            List<(string Key, HLCTimestamp LastUsed)> pool,
            long safetyWindowMs = 0
        )
        {
            JitterRemaining = jitter;
            Examined = 0;
            ScanMax = scanMax;
            SampleSize = sampleSize;
            CurrentTime = currentTime;
            ExcludedKeys = excludedKeys;
            Pool = pool;
            FirstSkippedKey = null;
            LastExamined = null;
            SafetyWindowMs = safetyWindowMs;
        }
    }
}
