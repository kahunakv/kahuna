namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// A rolling estimate of durable-finalize latency, used to derive the decision deadline as
/// <c>commit_ts + clamp(k × max(p99, p99-per-intent × intents), floor, ceiling)</c>. Latency here is a local
/// elapsed-time measurement (a duration used to size a timeout margin), not the ordering of a distributed event,
/// so it is sampled with a monotonic stopwatch and never with a wall clock; the deadline value itself is an HLC
/// offset.
///
/// <para>Two estimates are kept, because a finalize costs a fixed part (the Raft barriers) plus a part that grows
/// with the manifest (every intent is prepared, decided and materialized). A margin sized from the total p99 alone
/// is sized for the transactions the node has seen lately, so a manifest many times larger than those — a bulk
/// delete after a stream of small writes — ran past a deadline that nothing about it had informed, and the record's
/// deadline gate presumed it aborted while its coordinator was alive and still preparing. The per-intent p99 prices
/// this manifest by its own size. It is fed only by samples of at least <see cref="PerIntentSampleMinIntents"/>
/// intents, where the fixed cost is amortized; while no such sample is in the window every sample feeds it, which
/// over-prices an intent (the fixed cost is divided among a few) and so errs toward a wider margin until a large
/// finalize is observed — the ceiling bounds that.</para>
///
/// <para>Keeps the most recent <c>window</c> samples in a ring buffer. The p99s are recomputed on every sample
/// until <c>recomputeEvery</c> samples exist, and every <c>recomputeEvery</c> samples after that, and cached so
/// the per-transaction freeze reads an O(1) value. A node whose first finalizes are few would otherwise size
/// every one of them from the floor alone, blind to the latency it had already measured.</para>
/// </summary>
internal sealed class FinalizeLatencyEstimator
{
    /// <summary>
    /// The smallest manifest whose sample feeds the per-intent estimate on its own. Below it the fixed cost of a
    /// finalize dominates, and dividing it among a handful of intents would price every intent of a large
    /// manifest at that fixed cost.
    /// </summary>
    internal const int PerIntentSampleMinIntents = 64;

    private readonly double[] samples;
    // The manifest size of each sample, parallel to <see cref="samples"/>.
    private readonly int[] intentCounts;
    // Scratch for percentile recomputation, owned by the instance and only touched under the gate, so a
    // recompute sorts in place without allocating a copy of the window each time.
    private readonly double[] sortScratch;
    private readonly int recomputeEvery;
    private readonly object gate = new();

    private int next;             // ring write cursor
    private int filled;           // number of valid slots (== samples.Length once wrapped)
    private int sinceRecompute;
    private long cachedP99Ms;
    private double cachedP99PerIntentMs;

    public FinalizeLatencyEstimator(int window = 512, int recomputeEvery = 32)
    {
        if (window < 1) window = 1;
        if (recomputeEvery < 1) recomputeEvery = 1;
        samples = new double[window];
        intentCounts = new int[window];
        sortScratch = new double[window];
        this.recomputeEvery = recomputeEvery;
    }

    /// <summary>
    /// Records one observed finalize latency in milliseconds together with the size of the manifest it finalized.
    /// </summary>
    public void Record(double latencyMs, int intentCount)
    {
        if (latencyMs < 0) latencyMs = 0;
        if (intentCount < 1) intentCount = 1;

        lock (gate)
        {
            samples[next] = latencyMs;
            intentCounts[next] = intentCount;
            next = (next + 1) % samples.Length;
            if (filled < samples.Length) filled++;

            // Every sample counts while the window is young: the cadence exists to amortize the sort over a full
            // window, and a window of a few samples sorts for nothing.
            if (filled <= recomputeEvery || ++sinceRecompute >= recomputeEvery)
            {
                Recompute();
                sinceRecompute = 0;
            }
        }
    }

    /// <summary>The cached p99 of the total finalize latency in milliseconds; zero until the first sample.</summary>
    public long P99Ms
    {
        get { lock (gate) return cachedP99Ms; }
    }

    /// <summary>The cached p99 of the finalize latency per intent, in milliseconds; zero until the first sample.</summary>
    public double P99PerIntentMs
    {
        get { lock (gate) return cachedP99PerIntentMs; }
    }

    /// <summary>
    /// The decision-deadline margin for a finalize of <paramref name="intentCount"/> intents:
    /// <c>clamp(multiplier × max(p99, p99-per-intent × intentCount), floor, ceiling)</c>. The two estimates are
    /// read together under the gate, so a recompute cannot land between them. A ceiling below the floor reads
    /// as the floor, and before the first sample the floor applies.
    /// </summary>
    public long DeadlineMarginMs(int intentCount, int multiplier, long floorMs, long ceilingMs)
    {
        if (intentCount < 1) intentCount = 1;

        long ceiling = Math.Max(ceilingMs, floorMs);

        double sized;
        lock (gate)
            sized = Math.Max(cachedP99Ms, cachedP99PerIntentMs * intentCount);

        double derived = Math.Ceiling(multiplier * sized);
        return (long)Math.Clamp(derived, floorMs, ceiling);
    }

    private void Recompute()
    {
        if (filled == 0)
        {
            cachedP99Ms = 0;
            cachedP99PerIntentMs = 0;
            return;
        }

        // Sort only the filled prefix; the ring itself must keep its insertion order for overwrites.
        Array.Copy(samples, sortScratch, filled);
        cachedP99Ms = (long)Math.Ceiling(P99(sortScratch, filled));

        int count = 0;
        for (int i = 0; i < filled; i++)
        {
            if (intentCounts[i] >= PerIntentSampleMinIntents)
                sortScratch[count++] = samples[i] / intentCounts[i];
        }

        // No large finalize in the window yet: every sample prices an intent, fixed cost included.
        if (count == 0)
        {
            for (int i = 0; i < filled; i++)
                sortScratch[count++] = samples[i] / intentCounts[i];
        }

        cachedP99PerIntentMs = P99(sortScratch, count);
    }

    /// <summary>Sorts the first <paramref name="count"/> values of <paramref name="values"/> in place and returns their p99.</summary>
    private static double P99(double[] values, int count)
    {
        Array.Sort(values, 0, count);

        int idx = (int)Math.Ceiling(0.99 * count) - 1;
        if (idx < 0) idx = 0;
        if (idx >= count) idx = count - 1;

        return values[idx];
    }
}
