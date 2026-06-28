using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the eviction policy in <c>KeyWriteFrequencyTracker.RecordWrite</c>.
///
/// <para><b>Problem:</b> when the dictionary exceeds 4 096 entries, the current implementation scans
/// every entry to find the minimum-count key to evict. Under uniform high-cardinality write traffic
/// (e.g. replication of many distinct keys) this becomes an O(4 096) scan on <em>every</em>
/// over-cap write.</para>
///
/// <para><b>Strategies compared:</b>
/// <list type="bullet">
///   <item><b>FullScan</b> — current production: iterate all entries, evict the global minimum.</item>
///   <item><b>Sampled</b> — pick a small random sample (16 entries by default) and evict the minimum
///     among the sample. O(1) per over-cap write; slightly degrades split-centroid accuracy but keeps
///     the hot tail because low-count keys dominate the sample at the boundary.</item>
///   <item><b>BatchEvict</b> — allow the dictionary to grow to 1.5× the cap before scanning once and
///     removing the bottom half. Amortises the O(N) scan over many writes.</item>
/// </list>
/// </para>
///
/// <para><b>Workloads:</b>
/// <list type="bullet">
///   <item><b>HotKeys</b> — 64 hot keys repeated; cap never exceeded; measures base write cost.</item>
///   <item><b>HighCardinality</b> — 100 000 unique keys; cap exceeded on every new key after 4 096;
///     maximises eviction frequency.</item>
///   <item><b>Mixed9010</b> — 90 % writes to 64 hot keys, 10 % to new unique keys; realistic skewed
///     traffic that also stresses the eviction path.</item>
/// </list>
/// </para>
/// </summary>
[MemoryDiagnoser]
public class KeyWriteFrequencyTrackerBenchmark
{
    private const int MaxEntries = 4_096;
    private const int SampleSize = 16;
    private const int BatchOverflow = MaxEntries / 2; // grow to 1.5× before batch-evict

    [Params(Workload.HotKeys, Workload.HighCardinality, Workload.Mixed9010)]
    public Workload WorkloadType;

    private string[] _keys = null!;

    public enum Workload { HotKeys, HighCardinality, Mixed9010 }

    [GlobalSetup]
    public void Setup()
    {
        _keys = WorkloadType switch
        {
            Workload.HotKeys => BuildHotKeys(64, 10_000),
            Workload.HighCardinality => BuildUniqueKeys(100_000),
            Workload.Mixed9010 => BuildMixed90_10(64, 100_000, 100_000),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    // ── benchmarks ──────────────────────────────────────────────────────────────

    [Benchmark(Baseline = true)]
    public void FullScan()
    {
        ConcurrentDictionary<string, long> counts = new(StringComparer.Ordinal);

        foreach (string key in _keys)
        {
            counts.AddOrUpdate(key, 1L, static (_, v) => v + 1);

            if (counts.Count <= MaxEntries)
                continue;

            string? evict = null;
            long minCount = long.MaxValue;

            foreach (KeyValuePair<string, long> pair in counts)
            {
                if (pair.Value < minCount)
                {
                    minCount = pair.Value;
                    evict = pair.Key;
                }
            }

            if (evict is not null)
                counts.TryRemove(evict, out _);
        }
    }

    [Benchmark]
    public void Sampled()
    {
        ConcurrentDictionary<string, long> counts = new(StringComparer.Ordinal);
        int overCapCounter = 0;

        foreach (string key in _keys)
        {
            counts.AddOrUpdate(key, 1L, static (_, v) => v + 1);

            if (counts.Count <= MaxEntries)
                continue;

            // Sample SampleSize entries using a stride walk; evict the min among the sample.
            string? evict = null;
            long minCount = long.MaxValue;
            int stride = Math.Max(1, counts.Count / SampleSize);
            int idx = overCapCounter % stride; // vary start to avoid always hitting the same bucket
            int seen = 0;

            foreach (KeyValuePair<string, long> pair in counts)
            {
                if (idx-- > 0) continue;
                idx = stride - 1;

                if (pair.Value < minCount)
                {
                    minCount = pair.Value;
                    evict = pair.Key;
                }

                if (++seen >= SampleSize)
                    break;
            }

            overCapCounter++;

            if (evict is not null)
                counts.TryRemove(evict, out _);
        }
    }

    [Benchmark]
    public void BatchEvict()
    {
        ConcurrentDictionary<string, long> counts = new(StringComparer.Ordinal);

        foreach (string key in _keys)
        {
            counts.AddOrUpdate(key, 1L, static (_, v) => v + 1);

            if (counts.Count <= MaxEntries + BatchOverflow)
                continue;

            // Collect all entries, sort, remove the bottom half.
            List<KeyValuePair<string, long>> all = [.. counts];
            all.Sort(static (a, b) => a.Value.CompareTo(b.Value));

            int removeCount = all.Count - MaxEntries;
            for (int i = 0; i < removeCount; i++)
                counts.TryRemove(all[i].Key, out _);
        }
    }

    // ── workload builders ────────────────────────────────────────────────────────

    private static string[] BuildHotKeys(int hotCount, int totalWrites)
    {
        string[] hot = Enumerable.Range(0, hotCount).Select(i => $"hot-key-{i:D6}").ToArray();
        string[] result = new string[totalWrites];
        for (int i = 0; i < totalWrites; i++)
            result[i] = hot[i % hotCount];
        return result;
    }

    private static string[] BuildUniqueKeys(int count) =>
        Enumerable.Range(0, count).Select(i => $"unique-{i:D9}").ToArray();

    private static string[] BuildMixed90_10(int hotCount, int coldCount, int totalWrites)
    {
        string[] hot = Enumerable.Range(0, hotCount).Select(i => $"hot-{i:D6}").ToArray();
        string[] cold = Enumerable.Range(0, coldCount).Select(i => $"cold-{i:D9}").ToArray();

        string[] result = new string[totalWrites];
        for (int i = 0; i < totalWrites; i++)
        {
            // 90% hot, 10% cold (unique cold keys to stress eviction)
            if (i % 10 != 0)
                result[i] = hot[i % hotCount];
            else
                result[i] = cold[(i / 10) % coldCount];
        }

        return result;
    }
}
