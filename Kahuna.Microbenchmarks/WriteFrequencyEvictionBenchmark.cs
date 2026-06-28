using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the P4 eviction path in <c>KeyWriteFrequencyTracker.RecordWrite</c>.
///
/// <para><b>Old</b> = on every over-cap write, call <c>ConcurrentDictionary.Count</c> (acquires all
/// bucket locks) and full-scan all <see cref="MaxEntries"/> entries to find the lowest-count victim.
/// <b>New</b> = an <c>Interlocked</c> approximate counter gates the cap, and eviction reads a small
/// O(<c>EvictionSampleSize</c>) window and evicts its minimum. Both variants below are faithful copies
/// of the respective <c>RecordWrite</c> implementations.</para>
///
/// <para>Patterns (each ingests <see cref="Ops"/> writes into a fresh tracker; reported per write):
/// <list type="bullet">
///   <item><b>Uniform</b>: all keys distinct — the dictionary sits at the cap and <i>every</i> write
///     after the first <see cref="MaxEntries"/> triggers an eviction. This is the case P4 targets.</item>
///   <item><b>Skew9010</b>: 90% of writes hit a small hot set, 10% are distinct cold keys — evictions
///     fire at a lower rate, on cold keys.</item>
///   <item><b>Hot64</b>: 64 keys only — never reaches the cap, so eviction never runs (baseline write
///     cost; the two should match).</item>
/// </list></para>
///
/// <para><b>Result</b> (net10.0, Release; per write):
/// <list type="bullet">
///   <item>Hot64 (no eviction): 163 ns → 24 ns, 0 B (6.7×) — the gain is removing the per-write
///     <c>ConcurrentDictionary.Count</c> (all-bucket-lock) check; the new path only touches an
///     <c>Interlocked</c> counter on genuine inserts.</item>
///   <item>Skew9010: 4,348 ns → 74 ns (59×) — full scan on every cold-key eviction vs a small window.</item>
///   <item>Uniform (eviction on every write): 18,768 ns → 6,114 ns (3.1×) — the ~12 µs full scan is
///     gone; the residual ~6 µs is the inherent <c>ConcurrentDictionary</c> insert + remove churn of
///     ingesting all-distinct keys at the cap, which neither variant avoids.</item>
/// </list>
/// Allocation is at parity with the old path at every size (using <c>TryAdd</c> for add-detection
/// avoids the closure an <c>AddOrUpdate</c> add-factory would allocate on every call).</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class WriteFrequencyEvictionBenchmark
{
    private const int MaxEntries = 4_096;
    private const int EvictionSampleSize = 16;
    private const int Ops = 50_000;

    [Params("Uniform", "Skew9010", "Hot64")]
    public string Pattern = "";

    private string[] _keys = null!;

    [GlobalSetup]
    public void Setup()
    {
        _keys = new string[Ops];

        switch (Pattern)
        {
            case "Uniform":
                for (int i = 0; i < Ops; i++)
                    _keys[i] = $"key-{i:D8}";
                break;

            case "Hot64":
                for (int i = 0; i < Ops; i++)
                    _keys[i] = $"hot-{i % 64:D2}";
                break;

            case "Skew9010":
                // 90% to 64 hot keys, 10% to ever-growing distinct cold keys (~5000 distinct cold,
                // well over the cap, so the cold tail drives eviction).
                int cold = 0;
                for (int i = 0; i < Ops; i++)
                    _keys[i] = (i % 10 == 0)
                        ? $"cold-{cold++:D8}"
                        : $"hot-{i % 64:D2}";
                break;
        }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Ops)]
    public int OldFullScan()
    {
        OldTracker tracker = new();
        foreach (string key in _keys)
            tracker.RecordWrite(key);
        return tracker.Count;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int NewSampled()
    {
        NewTracker tracker = new();
        foreach (string key in _keys)
            tracker.RecordWrite(key);
        return tracker.Count;
    }

    /// <summary>Faithful copy of the pre-P4 RecordWrite: Count check + full-scan eviction.</summary>
    private sealed class OldTracker
    {
        private readonly ConcurrentDictionary<string, long> _counts = new(StringComparer.Ordinal);

        public int Count => _counts.Count;

        public void RecordWrite(string key)
        {
            _counts.AddOrUpdate(key, 1L, static (_, v) => v + 1);

            if (_counts.Count <= MaxEntries)
                return;

            string? evict = null;
            long minCount = long.MaxValue;

            foreach (KeyValuePair<string, long> pair in _counts)
            {
                if (pair.Value < minCount)
                {
                    minCount = pair.Value;
                    evict = pair.Key;
                }
            }

            if (evict is not null)
                _counts.TryRemove(evict, out _);
        }
    }

    /// <summary>Faithful copy of the post-P4 RecordWrite: approximate counter + sampled-window eviction.</summary>
    private sealed class NewTracker
    {
        private readonly ConcurrentDictionary<string, long> _counts = new(StringComparer.Ordinal);
        private int _approxCount;
        private int _evictCounter;

        public int Count => _counts.Count;

        public void RecordWrite(string key)
        {
            if (!_counts.TryAdd(key, 1L))
            {
                _counts.AddOrUpdate(key, 1L, static (_, v) => v + 1);
                return;
            }

            if (Interlocked.Increment(ref _approxCount) <= MaxEntries)
                return;

            int counter = Interlocked.Increment(ref _evictCounter);
            int skip = (counter & int.MaxValue) % EvictionSampleSize;

            string? evict = null;
            long minCount = long.MaxValue;
            int seen = 0;

            foreach (KeyValuePair<string, long> pair in _counts)
            {
                if (skip > 0)
                {
                    skip--;
                    continue;
                }

                if (pair.Value < minCount)
                {
                    minCount = pair.Value;
                    evict = pair.Key;
                }

                if (++seen >= EvictionSampleSize)
                    break;
            }

            if (evict is not null && _counts.TryRemove(evict, out _))
                Interlocked.Decrement(ref _approxCount);
        }
    }
}
