/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics.Metrics;

namespace Kahuna.Benchmark;

/// <summary>
/// Totals the client's routing counters for the run.
///
/// <para>
/// A throughput comparison between routing modes is only evidence if the run proves the mode was
/// actually in effect. A "learned" run whose cache never hit is a round-robin run under another
/// name, and would report a difference of zero for the wrong reason. These totals make that visible
/// in the benchmark's own output.
/// </para>
/// </summary>
internal sealed class RoutingCounters : IDisposable
{
    private readonly MeterListener listener = new();

    private readonly Dictionary<string, long> totals = new(StringComparer.Ordinal);

    private readonly object gate = new();

    public RoutingCounters()
    {
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "Kahuna.Client.Routing")
                meterListener.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            // The reason dimension is part of the name here: a rejected hint is only useful when the
            // reason travels with it, and there are a handful of fixed reasons, not an open set.
            string name = instrument.Name;

            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "reason")
                    name = name + "[" + tag.Value + "]";
            }

            lock (gate)
                totals[name] = totals.TryGetValue(name, out long current) ? current + measurement : measurement;
        });

        listener.Start();
    }

    /// <summary>Flushes the pending measurements and returns the totals, largest first.</summary>
    public List<KeyValuePair<string, long>> Snapshot()
    {
        listener.RecordObservableInstruments();

        lock (gate)
        {
            List<KeyValuePair<string, long>> rows = [.. totals];
            rows.Sort(static (a, b) => b.Value.CompareTo(a.Value));
            return rows;
        }
    }

    public void Dispose() => listener.Dispose();
}
