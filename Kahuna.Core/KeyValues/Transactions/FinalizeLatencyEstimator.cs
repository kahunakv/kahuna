namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// A rolling estimate of durable-finalize latency, used to derive the decision deadline as
/// <c>commit_ts + clamp(k × p99, floor, ceiling)</c>. Latency here is a local elapsed-time measurement (a
/// duration used to size a timeout margin), not the ordering of a distributed event, so it is sampled with a
/// monotonic stopwatch and never with a wall clock; the deadline value itself is an HLC offset.
///
/// <para>Keeps the most recent <c>window</c> samples in a ring buffer and recomputes the p99 every
/// <c>recomputeEvery</c> samples, caching it so the per-transaction freeze reads an O(1) value. During warmup
/// (before the first recompute) the p99 reads zero, so the deadline falls back to the configured floor.</para>
/// </summary>
internal sealed class FinalizeLatencyEstimator
{
    private readonly double[] samples;
    private readonly int recomputeEvery;
    private readonly object gate = new();

    private int next;             // ring write cursor
    private int filled;           // number of valid slots (== samples.Length once wrapped)
    private int sinceRecompute;
    private long cachedP99Ms;

    public FinalizeLatencyEstimator(int window = 512, int recomputeEvery = 32)
    {
        if (window < 1) window = 1;
        if (recomputeEvery < 1) recomputeEvery = 1;
        samples = new double[window];
        this.recomputeEvery = recomputeEvery;
    }

    /// <summary>Records one observed finalize latency in milliseconds.</summary>
    public void Record(double latencyMs)
    {
        if (latencyMs < 0) latencyMs = 0;

        lock (gate)
        {
            samples[next] = latencyMs;
            next = (next + 1) % samples.Length;
            if (filled < samples.Length) filled++;

            if (++sinceRecompute >= recomputeEvery)
            {
                Recompute();
                sinceRecompute = 0;
            }
        }
    }

    /// <summary>The cached p99 latency in milliseconds; zero until the first recompute (warmup).</summary>
    public long P99Ms
    {
        get { lock (gate) return cachedP99Ms; }
    }

    private void Recompute()
    {
        if (filled == 0)
        {
            cachedP99Ms = 0;
            return;
        }

        double[] copy = new double[filled];
        Array.Copy(samples, copy, filled);
        Array.Sort(copy);

        int idx = (int)Math.Ceiling(0.99 * filled) - 1;
        if (idx < 0) idx = 0;
        if (idx >= filled) idx = filled - 1;

        cachedP99Ms = (long)Math.Ceiling(copy[idx]);
    }
}
