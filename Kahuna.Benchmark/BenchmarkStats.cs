
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using HdrHistogram;

namespace Kahuna.Benchmark;

internal enum OpOutcome { Success, Error, Timeout, Miss }

internal enum OperationType { Get, Set, Delete, SetMany, DeleteMany, Transaction, Lock, Sequence, Script }

/// <summary>
/// Per-worker latency histograms and counters. Not thread-safe — only the owning
/// worker writes to it. Call <see cref="MergeInto"/> on the main thread after all
/// workers have completed.
/// </summary>
internal sealed class WorkerStats
{
    private static readonly OperationType[] AllOps = Enum.GetValues<OperationType>();

    // Upper bound of the histogram in µs. Derived from --timeout so the ceiling
    // always covers the achievable latency range (timeout × 1.5, minimum 1 s).
    private readonly long _maxMicros;

    private readonly Dictionary<OperationType, LongHistogram> _histograms;
    private readonly Dictionary<OperationType, long[]> _counts; // [0]=success [1]=errors [2]=timeouts [3]=misses
    private readonly Dictionary<OperationType, long> _sumMicros; // for mean computation

    public WorkerStats(long maxMicros)
    {
        _maxMicros = maxMicros;
        _histograms = new(AllOps.Length);
        _counts = new(AllOps.Length);
        _sumMicros = new(AllOps.Length);
        foreach (OperationType op in AllOps)
        {
            _histograms[op] = new LongHistogram(_maxMicros, 3);
            _counts[op] = new long[4];
            _sumMicros[op] = 0;
        }
    }

    public void Record(OperationType op, OpOutcome outcome, long elapsedMicros)
    {
        switch (outcome)
        {
            case OpOutcome.Success:
                long clamped = Math.Clamp(elapsedMicros, 1, _maxMicros);
                _histograms[op].RecordValue(clamped);
                _counts[op][0]++;
                _sumMicros[op] += clamped;
                break;
            case OpOutcome.Error:
                _counts[op][1]++;
                break;
            case OpOutcome.Timeout:
                _counts[op][2]++;
                break;
            case OpOutcome.Miss:
                // Not-found reads are counted separately; excluded from the latency
                // histogram so percentiles reflect only successful hits.
                _counts[op][3]++;
                break;
        }
    }

    public void MergeInto(WorkerStats target)
    {
        foreach (OperationType op in AllOps)
        {
            target._histograms[op].Add(_histograms[op]);
            target._counts[op][0] += _counts[op][0];
            target._counts[op][1] += _counts[op][1];
            target._counts[op][2] += _counts[op][2];
            target._counts[op][3] += _counts[op][3];
            target._sumMicros[op] += _sumMicros[op];
        }
    }

    public long MaxMicros => _maxMicros;

    public long GetSuccess(OperationType op)  => _counts[op][0];
    public long GetErrors(OperationType op)   => _counts[op][1];
    public long GetTimeouts(OperationType op) => _counts[op][2];
    public long GetMisses(OperationType op)   => _counts[op][3];
    public LongHistogram GetHistogram(OperationType op) => _histograms[op];

    public double GetMeanMicros(OperationType op) =>
        _counts[op][0] > 0 ? (double)_sumMicros[op] / _counts[op][0] : 0;

    /// <summary>
    /// Computes the histogram ceiling from the per-request timeout: timeout × 1.5,
    /// minimum 1 000 000 µs (1 s), to ensure the range is always fully covered.
    /// </summary>
    public static long CeilingMicrosFromTimeout(int timeoutSeconds) =>
        Math.Max((long)(timeoutSeconds * 1_500_000L), 1_000_000L);

    public IEnumerable<OperationType> ActiveOps =>
        AllOps.Where(op => _counts[op][0] + _counts[op][1] + _counts[op][2] + _counts[op][3] > 0);
}

/// <summary>
/// Merged aggregate stats for the completed measurement window.
/// </summary>
internal sealed class BenchmarkStats
{
    public WorkerStats Aggregate { get; }
    public double ElapsedSeconds { get; }
    public BenchmarkOptions Options { get; }

    public BenchmarkStats(WorkerStats aggregate, double elapsedSeconds, BenchmarkOptions options)
    {
        Aggregate = aggregate;
        ElapsedSeconds = elapsedSeconds;
        Options = options;
    }

    public static BenchmarkStats Merge(IReadOnlyList<WorkerStats> workers, double elapsedSeconds, BenchmarkOptions options)
    {
        long ceiling = WorkerStats.CeilingMicrosFromTimeout(options.Timeout);
        WorkerStats aggregate = new(ceiling);
        foreach (WorkerStats w in workers)
            w.MergeInto(aggregate);
        return new BenchmarkStats(aggregate, elapsedSeconds, options);
    }

    public long TotalSuccess => Aggregate.ActiveOps.Sum(op => Aggregate.GetSuccess(op));
    public long TotalErrors  => Aggregate.ActiveOps.Sum(op => Aggregate.GetErrors(op));
    public long TotalMisses  => Aggregate.ActiveOps.Sum(op => Aggregate.GetMisses(op));
    public double AchievedRps => ElapsedSeconds > 0 ? TotalSuccess / ElapsedSeconds : 0;
}
