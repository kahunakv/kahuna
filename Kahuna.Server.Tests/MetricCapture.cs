using System.Diagnostics.Metrics;

namespace Kahuna.Server.Tests;

/// <summary>
/// Collects every measurement the named instruments on the shared <c>"Kahuna"</c> meter emit while the capture
/// is alive, keyed by instrument name and by the value of one chosen tag, so a test can assert what a dashboard
/// would show per tag value (per admission class, per loop outcome, per call kind).
///
/// <para>The meter is process-wide and the test project runs classes in parallel, so another test's activity can
/// add samples to the same instrument. Assertions therefore check for the presence of the expected samples
/// (<c>Contains</c>, <c>&gt;=</c>), never for an exact total, unless the instrument is one only the test itself
/// can move.</para>
/// </summary>
internal sealed class MetricCapture : IDisposable
{
    private readonly MeterListener listener = new();
    private readonly object gate = new();
    private readonly Dictionary<(string Instrument, string Tag), List<double>> samples = [];
    private readonly string tagName;

    /// <param name="tagName">The tag whose value keys the samples; a measurement without it is keyed by "".</param>
    /// <param name="instruments">Instrument names to capture.</param>
    public MetricCapture(string tagName, params string[] instruments)
    {
        this.tagName = tagName;
        HashSet<string> wanted = [.. instruments];

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Kahuna" && wanted.Contains(instrument.Name))
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument.Name, tags, value));
        listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Record(instrument.Name, tags, value));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument.Name, tags, value));
        listener.Start();
    }

    private void Record(string instrument, ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
    {
        string tag = "";
        foreach (KeyValuePair<string, object?> pair in tags)
        {
            if (pair.Key == tagName)
                tag = pair.Value?.ToString() ?? "";
        }

        lock (gate)
        {
            if (!samples.TryGetValue((instrument, tag), out List<double>? list))
                samples[(instrument, tag)] = list = [];

            list.Add(value);
        }
    }

    /// <summary>Every sample recorded for <paramref name="instrument"/> under <paramref name="tag"/>, in order.</summary>
    public IReadOnlyList<double> Samples(string instrument, string tag = "")
    {
        lock (gate)
            return samples.TryGetValue((instrument, tag), out List<double>? list) ? [.. list] : [];
    }

    /// <summary>Sum of every sample for <paramref name="instrument"/> under <paramref name="tag"/> (a counter's total).</summary>
    public double Total(string instrument, string tag = "")
    {
        double total = 0;
        foreach (double sample in Samples(instrument, tag))
            total += sample;
        return total;
    }

    public void Dispose() => listener.Dispose();
}
