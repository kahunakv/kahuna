using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the <c>RangeMap.Descriptors</c> property.
///
/// <para><b>Old</b> = <c>bySpace.Values.SelectMany(r => r).ToArray()</c> re-executed on every
/// access — a full LINQ flatten + heap allocation each call, paid by every periodic split/merge
/// scan, checkpoint, snapshot load, and key-space registry sync that reads the descriptor list.
/// </para>
///
/// <para><b>New</b> = flattened array built once in the constructor and returned directly.
/// Subsequent <c>Descriptors</c> accesses are a field read — O(1), zero allocation.</para>
///
/// <para>Parametrised by descriptor count (1/100/1000/10000), each spread across two key spaces
/// to exercise the <c>SelectMany</c> path. Old and New are stand-alone faithful copies of the
/// two implementations so the benchmark does not require a project reference to <c>Kahuna.Core</c>.</para>
///
/// <para><b>Result</b> (net10.0, Release):
/// Placeholder — run <c>dotnet run -c Release --project Kahuna.Microbenchmarks</c> to populate.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class RangeMapDescriptorsBenchmark
{
    [Params(1, 100, 1000, 10_000)]
    public int DescriptorCount;

    private OldRangeMap _old = null!;
    private NewRangeMap _new = null!;

    [GlobalSetup]
    public void Setup()
    {
        Desc[] descriptors = BuildDescriptors(DescriptorCount);
        _old = new OldRangeMap(descriptors);
        _new = new NewRangeMap(descriptors);
    }

    [Benchmark(Baseline = true)]
    public Desc[] OldDescriptors() => _old.Descriptors;

    [Benchmark]
    public Desc[] NewDescriptors() => _new.Descriptors;

    // ── stand-in descriptor ──────────────────────────────────────────────────────

    public sealed class Desc
    {
        public string KeySpace { get; init; } = "";
        public string? StartKey { get; init; }
        public string? EndKey { get; init; }
        public int PartitionId { get; init; }
    }

    private static Desc[] BuildDescriptors(int count)
    {
        Desc[] result = new Desc[count];
        for (int i = 0; i < count; i++)
        {
            // Alternate between two key spaces to exercise SelectMany over multiple buckets.
            string ks = (i % 2 == 0) ? "ks:a" : "ks:b";
            string? start = i < 2 ? null : $"k{i:D9}";
            string? end = (i >= count - 2) ? null : $"k{i + 1:D9}";
            result[i] = new Desc { KeySpace = ks, StartKey = start, EndKey = end, PartitionId = i };
        }
        return result;
    }

    // ── old implementation: recomputes on every access ───────────────────────────

    public sealed class OldRangeMap
    {
        private readonly Dictionary<string, Desc[]> _bySpace;

        public OldRangeMap(IEnumerable<Desc> descriptors)
        {
            _bySpace = descriptors
                .GroupBy(static d => d.KeySpace, StringComparer.Ordinal)
                .ToDictionary(
                    static g => g.Key,
                    static g => g.ToArray(),
                    StringComparer.Ordinal);
        }

        public Desc[] Descriptors =>
            _bySpace.Values.SelectMany(static r => r).ToArray();
    }

    // ── new implementation: cached in constructor ─────────────────────────────────

    public sealed class NewRangeMap
    {
        private readonly Dictionary<string, Desc[]> _bySpace;
        private readonly Desc[] _descriptors;

        public NewRangeMap(IEnumerable<Desc> descriptors)
        {
            _bySpace = descriptors
                .GroupBy(static d => d.KeySpace, StringComparer.Ordinal)
                .ToDictionary(
                    static g => g.Key,
                    static g => g.ToArray(),
                    StringComparer.Ordinal);

            _descriptors = [.. _bySpace.Values.SelectMany(static r => r)];
        }

        public Desc[] Descriptors => _descriptors;
    }
}
