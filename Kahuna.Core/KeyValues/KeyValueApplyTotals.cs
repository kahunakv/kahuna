using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues;

/// <summary>The kind of a log entry that the ordered apply path of the key-value subsystem applied; the <c>type</c>
/// tag of <c>kahuna.kv.apply.entries</c> and <c>kahuna.kv.apply.ms</c>.</summary>
internal enum KeyValueApplyEntryType
{
    /// <summary>A key-value record.</summary>
    Kv,

    /// <summary>A transaction record delta: an init, a decision or another record transition.</summary>
    Record,

    /// <summary>A prepared-intent delta that carries at least one prepare.</summary>
    Prepare,

    /// <summary>A prepared-intent delta that carries no prepare: resolves and removes of a settle.</summary>
    Settle,

    /// <summary>The record init, the prepare and the decision of a one-phase commit in one entry.</summary>
    Bundle,

    /// <summary>A completion receipt.</summary>
    Receipt,

    /// <summary>Any other log type: the range map, the snapshot floor, a history hold, a progress tick.</summary>
    Other
}

/// <summary>
/// Totals of the ordered apply path of one node, by <see cref="KeyValueApplyEntryType"/>: the entries applied and the
/// time their apply took. The time is the synchronous part of the apply of one entry, read from two timestamps around
/// it, and it is measured only while a listener reads <c>kahuna.kv.apply.ms</c>. The entries are always counted.
///
/// <para>The totals of each partition are kept apart, because the applies of one partition are serialized and the
/// applies of different partitions can run at the same time on different threads. The observable counters sum them
/// when a listener collects. A total divided by the entries of its type is the mean apply time of that type.</para>
/// </summary>
internal sealed class KeyValueApplyTotals : IDisposable
{
    internal const int TypeCount = (int)KeyValueApplyEntryType.Other + 1;

    private static readonly double MsPerTick = 1000.0 / Stopwatch.Frequency;

    private static readonly KeyValuePair<string, object?>[] TypeTags = BuildTypeTags();

    // Instance-owned: the callbacks capture the totals of this node, so the instruments must end with the node.
    private readonly Meter meter = new("Kahuna", "1.0");

    private readonly ObservableCounter<double> applyMs;

    private readonly Func<IEnumerable<PartitionTotals>> partitions;

    /// <summary>Test-only: times every apply, also with no listener on the time counter.</summary>
    internal bool TimeAlwaysForTesting { get; set; }

    /// <param name="partitions">The totals of every partition of the node, read when a listener collects.</param>
    internal KeyValueApplyTotals(Func<IEnumerable<PartitionTotals>> partitions)
    {
        this.partitions = partitions;

        meter.CreateObservableCounter(
            "kahuna.kv.apply.entries", ObserveEntries, unit: "{entries}",
            description: "Log entries the ordered apply path of the key-value subsystem applied on this node, tagged by entry type.");

        applyMs = meter.CreateObservableCounter(
            "kahuna.kv.apply.ms", ObserveMilliseconds, unit: "ms",
            description: "Time the ordered apply path of the key-value subsystem spent in the synchronous apply of each entry, tagged by entry type.");
    }

    /// <summary>Whether the apply of an entry reads the clock: a listener reads the time counter, or a test asks.</summary>
    internal bool TimingEnabled => applyMs.Enabled || TimeAlwaysForTesting;

    /// <summary>The <c>type</c> tag value of an entry type.</summary>
    internal static string TypeName(KeyValueApplyEntryType type) => type switch
    {
        KeyValueApplyEntryType.Kv => "kv",
        KeyValueApplyEntryType.Record => "record",
        KeyValueApplyEntryType.Prepare => "prepare",
        KeyValueApplyEntryType.Settle => "settle",
        KeyValueApplyEntryType.Bundle => "bundle",
        KeyValueApplyEntryType.Receipt => "receipt",
        _ => "other"
    };

    /// <summary>The entries of <paramref name="type"/> that this node applied.</summary>
    internal long Entries(KeyValueApplyEntryType type)
    {
        long total = 0;
        foreach (PartitionTotals totals in partitions())
            total += Interlocked.Read(ref totals.EntriesByType[(int)type]);
        return total;
    }

    /// <summary>The milliseconds that the timed applies of <paramref name="type"/> took on this node.</summary>
    internal double Milliseconds(KeyValueApplyEntryType type)
    {
        long ticks = 0;
        foreach (PartitionTotals totals in partitions())
            ticks += Interlocked.Read(ref totals.TicksByType[(int)type]);
        return ticks * MsPerTick;
    }

    private static KeyValuePair<string, object?>[] BuildTypeTags()
    {
        KeyValuePair<string, object?>[] tags = new KeyValuePair<string, object?>[TypeCount];
        for (int i = 0; i < TypeCount; i++)
            tags[i] = new("type", TypeName((KeyValueApplyEntryType)i));
        return tags;
    }

    private List<Measurement<long>> ObserveEntries()
    {
        Span<long> sums = stackalloc long[TypeCount];
        foreach (PartitionTotals totals in partitions())
        {
            for (int i = 0; i < TypeCount; i++)
                sums[i] += Interlocked.Read(ref totals.EntriesByType[i]);
        }

        List<Measurement<long>> measurements = new(TypeCount);
        for (int i = 0; i < TypeCount; i++)
        {
            if (sums[i] != 0)
                measurements.Add(new(sums[i], TypeTags[i]));
        }

        return measurements;
    }

    private List<Measurement<double>> ObserveMilliseconds()
    {
        Span<long> sums = stackalloc long[TypeCount];
        foreach (PartitionTotals totals in partitions())
        {
            for (int i = 0; i < TypeCount; i++)
                sums[i] += Interlocked.Read(ref totals.TicksByType[i]);
        }

        List<Measurement<double>> measurements = new(TypeCount);
        for (int i = 0; i < TypeCount; i++)
        {
            if (sums[i] != 0)
                measurements.Add(new(sums[i] * MsPerTick, TypeTags[i]));
        }

        return measurements;
    }

    public void Dispose() => meter.Dispose();

    /// <summary>The totals of one partition. Its applies are serialized, so the adds never contend.</summary>
    internal sealed class PartitionTotals
    {
        internal readonly long[] EntriesByType = new long[TypeCount];

        internal readonly long[] TicksByType = new long[TypeCount];

        /// <summary>Counts one applied entry, and its apply time when <paramref name="ticks"/> is not zero.</summary>
        internal void Count(KeyValueApplyEntryType type, long ticks)
        {
            Interlocked.Increment(ref EntriesByType[(int)type]);
            if (ticks != 0)
                Interlocked.Add(ref TicksByType[(int)type], ticks);
        }
    }
}
