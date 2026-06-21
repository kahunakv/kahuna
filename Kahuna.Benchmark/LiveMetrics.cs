
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using HdrHistogram;

namespace Kahuna.Benchmark;

/// <summary>
/// Thread-safe live metrics sampled by the live-display task once per second.
/// Workers call <see cref="BeginOp"/>/<see cref="EndOp"/> around each request;
/// the live display calls <see cref="SnapshotInterval"/> to read and reset the
/// per-second p99.
///
/// <see cref="Recorder.RecordValue"/> is lock-free from writer threads — the hot
/// path on every worker incurs no lock, no fence beyond the Interlocked counters.
/// <see cref="SnapshotInterval"/> calls <see cref="Recorder.GetIntervalHistogram()"/>,
/// which atomically flips the internal double-buffer; writers racing with the flip
/// are handled inside Recorder with a single internal CAS, not a mutex.
/// </summary>
internal sealed class LiveMetrics
{
    private long _successCount;
    private long _inFlight;
    private readonly Recorder _recorder;
    private readonly long _maxMicros;

    public LiveMetrics(long maxMicros)
    {
        _maxMicros = maxMicros;
        // Use a factory so the internal histogram stays LongHistogram — the
        // HistogramBase returned by GetIntervalHistogram() can then be queried
        // for percentiles directly without a cast.
        _recorder = new Recorder(1L, maxMicros, 3,
            (_, l, h, d) => new LongHistogram(l, h, d));
    }

    public void BeginOp() => Interlocked.Increment(ref _inFlight);

    public void EndOp(OpOutcome outcome, long elapsedMicros)
    {
        Interlocked.Decrement(ref _inFlight);
        if (outcome == OpOutcome.Success)
        {
            Interlocked.Increment(ref _successCount);
            _recorder.RecordValue(Math.Clamp(elapsedMicros, 1, _maxMicros));
        }
    }

    /// <summary>Decrements in-flight without recording a result (op cancelled mid-flight).</summary>
    public void AbortOp() => Interlocked.Decrement(ref _inFlight);

    public long TotalSuccess => Interlocked.Read(ref _successCount);
    public long InFlight     => Interlocked.Read(ref _inFlight);

    /// <summary>
    /// Atomically swaps the interval histogram and returns (p99, count) for the
    /// just-completed window. Concurrent <see cref="EndOp"/> calls need no
    /// coordination — the swap is handled inside <see cref="Recorder"/>.
    /// </summary>
    public (long p99Micros, long count) SnapshotInterval()
    {
        HistogramBase snap = _recorder.GetIntervalHistogram();
        long count = snap.TotalCount;
        long p99   = count > 0 ? snap.GetValueAtPercentile(99) : 0;
        return (p99, count);
    }
}
