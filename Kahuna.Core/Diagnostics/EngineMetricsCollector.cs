/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;

namespace Kahuna.Server.Diagnostics;

/// <summary>How a metric's samples combine, which decides the fields a snapshot row fills.</summary>
public enum EngineMetricKind
{
    /// <summary>Additive total since the collector started; reports only a total.</summary>
    Counter,

    /// <summary>Distribution of recorded values; reports count, sum, min, max and the most recent value.</summary>
    Histogram,

    /// <summary>Instantaneous value sampled when the snapshot is taken; reports only that value.</summary>
    Gauge,
}

/// <summary>
/// One aggregated instrument + tag-set.
/// <see cref="Total"/>/<see cref="Min"/>/<see cref="Max"/>/<see cref="Last"/> are null where the
/// <see cref="EngineMetricKind"/> does not define them.
/// </summary>
public sealed record EngineMetricRow(
    string Source,
    string Metric,
    string Tags,
    EngineMetricKind Kind,
    long Count,
    double? Total,
    double? Min,
    double? Max,
    double? Last);

/// <summary>Identity of an aggregate: the meter, the instrument, and its canonical tag string.</summary>
internal readonly record struct EngineMetricKey(string Source, string Metric, EngineMetricKind Kind, string Tags);

/// <summary>
/// A measurement's identity as it arrives in the callback, before its tags have been rendered to a
/// string. It is a <c>ref struct</c> because it borrows the callback's tag span: it exists so a
/// repeat measurement can be looked up without materializing that string, and must never outlive
/// the callback.
/// </summary>
internal readonly ref struct EngineMeasurementKey(
    string source,
    string metric,
    EngineMetricKind kind,
    ReadOnlySpan<KeyValuePair<string, object?>> tags)
{
    public readonly string Source = source;
    public readonly string Metric = metric;
    public readonly EngineMetricKind Kind = kind;
    public readonly ReadOnlySpan<KeyValuePair<string, object?>> Tags = tags;
}

/// <summary>
/// Observes the Kahuna and Kommander <see cref="Meter"/>s in-process and aggregates their
/// measurements, so the operator dashboard can report them. Both libraries publish their
/// instruments whether or not an exporter is configured, so without a listener the node's own
/// request rate, WAL batching, executor queue depth and commit latency are readable only after
/// someone wires up OpenTelemetry or Prometheus. This collector is that listener — no exporter, no
/// scrape endpoint, and no change to either library.
///
/// <para><b>Process-wide, not per-node.</b> Both libraries publish through <c>static</c> meters, so
/// two embedded nodes in one process feed the same instruments and every listener sees the union of
/// their traffic. That is right for a production single-node process, but it means a test running
/// beside other tests must assert on metric <i>presence</i> and monotonicity, never on exact
/// counts.</para>
///
/// <para><b>Window.</b> Counters and histograms accumulate from the moment this collector starts —
/// process start in practice — and there is no reset. Gauges hold no history: they are sampled
/// during <see cref="Snapshot"/> and report only that instant. An observable <i>counter</i> is
/// reported as a gauge whose value is its cumulative total, since that is what the instrument
/// publishes.</para>
///
/// <para><b>Hot path.</b> Measurement callbacks fire inside the Raft executor, the WAL writer and
/// the key-value write aggregator, so the recording path allocates nothing once a tag-set has been
/// seen: the canonical tag string is built only on first sight, through an alternate lookup that
/// hashes and compares the incoming tag span directly against the stored string. Dispose stops the
/// listener — the test suite builds and tears down many nodes, and a leaked listener would keep
/// observing forever.</para>
/// </summary>
public sealed class EngineMetricsCollector : IDisposable
{
    /// <summary>Meter name published by Kommander (<c>KommanderMetrics.MeterName</c>).</summary>
    public const string KommanderMeterName = "Kommander";

    /// <summary>Meter name published by Kahuna's metric holders.</summary>
    public const string KahunaMeterName = "Kahuna";

    /// <summary>Tag-set size that fits the sort buffer without falling back to the heap.</summary>
    private const int MaxStackTags = 16;

    private static readonly EngineMetricKeyComparer Comparer = new();

    private readonly ConcurrentDictionary<EngineMetricKey, Aggregate> aggregates = new(Comparer);

    private readonly ConcurrentDictionary<EngineMetricKey, Aggregate>.AlternateLookup<EngineMeasurementKey> lookup;

    private readonly MeterListener listener;

    private volatile bool disposed;

    /// <summary>
    /// Starts observing immediately: instruments published before this point are replayed by
    /// <see cref="MeterListener.Start"/>, so construction order relative to the node does not matter.
    /// </summary>
    public EngineMetricsCollector()
    {
        lookup = aggregates.GetAlternateLookup<EngineMeasurementKey>();

        listener = new MeterListener
        {
            InstrumentPublished = static (instrument, listener) =>
            {
                string meter = instrument.Meter.Name;

                if (!string.Equals(meter, KommanderMeterName, StringComparison.Ordinal) &&
                    !string.Equals(meter, KahunaMeterName, StringComparison.Ordinal))
                    return;

                listener.EnableMeasurementEvents(
                    instrument,
                    new InstrumentDescriptor(meter.ToLowerInvariant(), instrument.Name, KindOf(instrument)));
            },
        };

        // Every numeric type MeterListener can deliver. Kahuna and Kommander between them publish
        // Counter<long>, Histogram<int>, Histogram<long> and Histogram<double>; registering only the
        // double callback would silently drop the integer instruments (raft.wal.batch_size,
        // kahuna.kv.write.batch_bytes) rather than fail visibly.
        listener.SetMeasurementEventCallback<byte>((i, m, t, s) => Record(m, t, s));
        listener.SetMeasurementEventCallback<short>((i, m, t, s) => Record(m, t, s));
        listener.SetMeasurementEventCallback<int>((i, m, t, s) => Record(m, t, s));
        listener.SetMeasurementEventCallback<long>((i, m, t, s) => Record(m, t, s));
        listener.SetMeasurementEventCallback<float>((i, m, t, s) => Record(m, t, s));
        listener.SetMeasurementEventCallback<double>((i, m, t, s) => Record(m, t, s));
        listener.SetMeasurementEventCallback<decimal>((i, m, t, s) => Record((double)m, t, s));

        listener.Start();
    }

    /// <summary>
    /// Samples every observable instrument and returns the aggregates, ordered by source, metric and
    /// tags (ordinal) so two invocations diff cleanly. Sampling happens here rather than on a timer,
    /// so a gauge reports the moment the caller asked.
    /// </summary>
    public IReadOnlyList<EngineMetricRow> Snapshot()
    {
        if (!disposed)
            listener.RecordObservableInstruments();

        List<EngineMetricRow> rows = new(aggregates.Count);

        foreach (KeyValuePair<EngineMetricKey, Aggregate> entry in aggregates)
        {
            EngineMetricKey key = entry.Key;
            Aggregate aggregate = entry.Value;

            lock (aggregate)
            {
                if (!aggregate.HasValue)
                    continue;

                rows.Add(key.Kind switch
                {
                    EngineMetricKind.Counter => new EngineMetricRow(
                        key.Source, key.Metric, key.Tags, key.Kind,
                        Count: (long)aggregate.Sum, Total: aggregate.Sum,
                        Min: null, Max: null, Last: null),

                    EngineMetricKind.Gauge => new EngineMetricRow(
                        key.Source, key.Metric, key.Tags, key.Kind,
                        Count: 1, Total: null,
                        Min: null, Max: null, Last: aggregate.Last),

                    _ => new EngineMetricRow(
                        key.Source, key.Metric, key.Tags, key.Kind,
                        Count: aggregate.Count, Total: aggregate.Sum,
                        Min: aggregate.Min, Max: aggregate.Max, Last: aggregate.Last),
                });
            }
        }

        rows.Sort(static (a, b) =>
        {
            int cmp = string.CompareOrdinal(a.Source, b.Source);
            if (cmp != 0)
                return cmp;

            cmp = string.CompareOrdinal(a.Metric, b.Metric);
            return cmp != 0 ? cmp : string.CompareOrdinal(a.Tags, b.Tags);
        });

        return rows;
    }

    public void Dispose()
    {
        if (disposed)
            return;

        disposed = true;
        listener.Dispose();
    }

    /// <summary>
    /// Folds one measurement into its aggregate. The alternate lookup handles the steady state without
    /// building a string; only a tag-set never seen before takes the materializing path.
    /// </summary>
    private void Record(double value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        if (state is not InstrumentDescriptor descriptor)
            return;

        EngineMeasurementKey measurement = new(descriptor.Source, descriptor.Metric, descriptor.Kind, tags);

        if (!lookup.TryGetValue(measurement, out Aggregate? aggregate))
            aggregate = aggregates.GetOrAdd(Comparer.Create(measurement), static _ => new Aggregate());

        lock (aggregate)
        {
            aggregate.HasValue = true;
            aggregate.Last = value;

            if (descriptor.Kind == EngineMetricKind.Gauge)
                return;

            aggregate.Count++;
            aggregate.Sum += value;

            if (value < aggregate.Min)
                aggregate.Min = value;

            if (value > aggregate.Max)
                aggregate.Max = value;
        }
    }

    /// <summary>
    /// Classifies an instrument once, at publish time, so the measurement callback never re-derives it.
    /// Anything observable that is not a histogram is treated as a gauge — including an observable
    /// counter, whose published value is its cumulative total and is reported as such.
    /// </summary>
    private static EngineMetricKind KindOf(Instrument instrument) => instrument switch
    {
        Histogram<byte> or Histogram<short> or Histogram<int> or Histogram<long>
            or Histogram<float> or Histogram<double> or Histogram<decimal> => EngineMetricKind.Histogram,
        _ when instrument.IsObservable => EngineMetricKind.Gauge,
        _ => EngineMetricKind.Counter,
    };

    /// <summary>Per-instrument constants resolved at publish time and handed back on every measurement.</summary>
    private sealed record InstrumentDescriptor(string Source, string Metric, EngineMetricKind Kind);

    /// <summary>
    /// Mutable running state for one instrument + tag-set. Guarded by locking the instance itself:
    /// count/sum/min/max must move together, and contention is per tag-set rather than global.
    /// </summary>
    private sealed class Aggregate
    {
        public bool HasValue;
        public long Count;
        public double Sum;
        public double Min = double.PositiveInfinity;
        public double Max = double.NegativeInfinity;
        public double Last;
    }

    // ── Canonical tag rendering ───────────────────────────────────────────────
    //
    // The canonical form is "k1=v1,k2=v2" with keys sorted ordinally, so two call sites that emit the
    // same tags in different orders land on one row. Hashing, comparing, and building all walk the tag
    // span through the same writer below, which is what guarantees the hash of a span matches the hash
    // of the string it would have produced.

    /// <summary>Receives the canonical rendering in chunks, so it can be hashed, compared, or built.</summary>
    private interface ICanonicalSink
    {
        void Write(ReadOnlySpan<char> chunk);
    }

    /// <summary>
    /// Hashes the rendering without building a string. Deliberately a rolling FNV-1a over characters
    /// rather than <see cref="HashCode"/>: the same logical text must hash identically whether it
    /// arrives as one chunk (an already-canonical string) or as many (a tag span rendered piecewise),
    /// and <c>HashCode.AddBytes</c> is sensitive to where the chunk boundaries fall.
    /// </summary>
    private struct HashSink : ICanonicalSink
    {
        private const uint OffsetBasis = 2166136261;
        private const uint Prime = 16777619;

        private uint hash = OffsetBasis;

        public HashSink()
        {
        }

        public readonly int Value => (int)hash;

        public void Write(ReadOnlySpan<char> chunk)
        {
            uint current = hash;

            foreach (char c in chunk)
            {
                current = (current ^ (byte)c) * Prime;
                current = (current ^ (byte)(c >> 8)) * Prime;
            }

            hash = current;
        }
    }

    /// <summary>Compares the rendering against an existing canonical string as it is produced.</summary>
    private struct MatchSink(string target) : ICanonicalSink
    {
        private readonly string target = target;
        private int position;

        public bool Matched { get; private set; } = true;

        public readonly bool Complete => Matched && position == target.Length;

        public void Write(ReadOnlySpan<char> chunk)
        {
            if (!Matched)
                return;

            if (position + chunk.Length > target.Length ||
                !target.AsSpan(position, chunk.Length).SequenceEqual(chunk))
            {
                Matched = false;
                return;
            }

            position += chunk.Length;
        }
    }

    /// <summary>Materializes the rendering. Used only the first time a tag-set is seen.</summary>
    private readonly struct BuilderSink(StringBuilder builder) : ICanonicalSink
    {
        public void Write(ReadOnlySpan<char> chunk) => builder.Append(chunk);
    }

    /// <summary>
    /// Writes <paramref name="tags"/> to <paramref name="sink"/> in canonical form. Sorting is an
    /// insertion sort over a stack-allocated index buffer: tag-sets here carry one or two tags, and
    /// this runs on every measurement.
    /// </summary>
    private static void WriteCanonical<TSink>(ReadOnlySpan<KeyValuePair<string, object?>> tags, ref TSink sink)
        where TSink : struct, ICanonicalSink
    {
        if (tags.IsEmpty)
            return;

        Span<int> order = stackalloc int[MaxStackTags];

        if (tags.Length > MaxStackTags)
            order = new int[tags.Length];

        order = order[..tags.Length];

        for (int i = 0; i < order.Length; i++)
            order[i] = i;

        for (int i = 1; i < order.Length; i++)
        {
            int current = order[i];
            int j = i - 1;

            while (j >= 0 && string.CompareOrdinal(tags[order[j]].Key, tags[current].Key) > 0)
            {
                order[j + 1] = order[j];
                j--;
            }

            order[j + 1] = current;
        }

        for (int i = 0; i < order.Length; i++)
        {
            if (i > 0)
                sink.Write(",");

            KeyValuePair<string, object?> tag = tags[order[i]];
            sink.Write(tag.Key);
            sink.Write("=");
            WriteValue(tag.Value, ref sink);
        }
    }

    /// <summary>
    /// Renders a tag value. Numbers and booleans format straight into a stack buffer through
    /// <see cref="ISpanFormattable"/> — the value is already boxed by the metrics API, so this adds no
    /// allocation. Invariant culture keeps a decimal tag identical across locales, which matters
    /// because the rendering is part of a dictionary key.
    /// </summary>
    private static void WriteValue<TSink>(object? value, ref TSink sink)
        where TSink : struct, ICanonicalSink
    {
        switch (value)
        {
            case null:
                return;

            case string text:
                sink.Write(text);
                return;

            case ISpanFormattable formattable:
                Span<char> buffer = stackalloc char[64];

                if (formattable.TryFormat(buffer, out int written, default, CultureInfo.InvariantCulture))
                {
                    sink.Write(buffer[..written]);
                    return;
                }

                break;
        }

        sink.Write(value.ToString() ?? "");
    }

    /// <summary>
    /// Equality for aggregate keys, plus the alternate form that lets a raw tag span be looked up
    /// against stored canonical strings. The two must agree exactly: both hash the same canonical
    /// character sequence, which they get by sharing <see cref="WriteCanonical"/>.
    /// </summary>
    private sealed class EngineMetricKeyComparer
        : IEqualityComparer<EngineMetricKey>, IAlternateEqualityComparer<EngineMeasurementKey, EngineMetricKey>
    {
        public bool Equals(EngineMetricKey x, EngineMetricKey y)
            => x.Kind == y.Kind
               && string.Equals(x.Source, y.Source, StringComparison.Ordinal)
               && string.Equals(x.Metric, y.Metric, StringComparison.Ordinal)
               && string.Equals(x.Tags, y.Tags, StringComparison.Ordinal);

        public int GetHashCode(EngineMetricKey key)
        {
            HashSink sink = new();
            WriteIdentity(ref sink, key.Source, key.Metric, key.Kind);
            sink.Write(key.Tags);
            return sink.Value;
        }

        public bool Equals(EngineMeasurementKey alternate, EngineMetricKey key)
        {
            if (alternate.Kind != key.Kind
                || !string.Equals(alternate.Source, key.Source, StringComparison.Ordinal)
                || !string.Equals(alternate.Metric, key.Metric, StringComparison.Ordinal))
                return false;

            MatchSink sink = new(key.Tags);
            WriteCanonical(alternate.Tags, ref sink);
            return sink.Complete;
        }

        public int GetHashCode(EngineMeasurementKey alternate)
        {
            HashSink sink = new();
            WriteIdentity(ref sink, alternate.Source, alternate.Metric, alternate.Kind);
            WriteCanonical(alternate.Tags, ref sink);
            return sink.Value;
        }

        /// <summary>
        /// Writes the non-tag part of a key, separated so that shifting a character between source and
        /// metric changes the hash. Both <c>GetHashCode</c> overloads funnel through it, which is what
        /// makes a span lookup land in the same bucket as the string key it matches.
        /// </summary>
        private static void WriteIdentity(ref HashSink sink, string source, string metric, EngineMetricKind kind)
        {
            sink.Write(source);
            sink.Write("\0");
            sink.Write(metric);
            sink.Write("\0");
            sink.Write([(char)kind]);
            sink.Write("\0");
        }

        public EngineMetricKey Create(EngineMeasurementKey alternate)
        {
            if (alternate.Tags.IsEmpty)
                return new EngineMetricKey(alternate.Source, alternate.Metric, alternate.Kind, "");

            StringBuilder builder = new();
            BuilderSink sink = new(builder);
            WriteCanonical(alternate.Tags, ref sink);

            return new EngineMetricKey(alternate.Source, alternate.Metric, alternate.Kind, builder.ToString());
        }
    }
}
