
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using HdrHistogram;

namespace Kahuna.Benchmark;

/// <summary>
/// Formats and emits <see cref="BenchmarkStats"/> in the requested output mode.
/// Console output is implemented here (Phase B basic + Phase C Spectre table).
/// JSON and CSV reporters are added in Phase C.
/// </summary>
internal static class BenchmarkReporter
{
    public static void PrintConsole(BenchmarkStats stats)
    {
        Console.WriteLine();
        Console.WriteLine($"Kahuna Benchmark — {stats.Options.Workload}, {stats.ElapsedSeconds:F1}s, " +
                          $"concurrency={stats.Options.Concurrency}, " +
                          $"target={(stats.Options.Rate > 0 ? stats.Options.Rate + " req/s" : "unbounded")}");
        Console.WriteLine();

        const string header = " {0,-12} {1,10} {2,8} {3,7} {4,7} {5,7} {6,7} {7,7} {8,7} {9,7}";
        const string row    = " {0,-12} {1,10:N0} {2,8:F0} {3,7} {4,7} {5,7} {6,7} {7,7} {8,7} {9,7}";

        Console.WriteLine(string.Format(header,
            "Operation", "Count", "req/s", "p50", "p90", "p99", "p99.9", "max", "errors", "misses"));
        Console.WriteLine(new string('─', 98));

        long totalSuccess = 0;
        long totalErrors  = 0;
        long totalMisses  = 0;

        WorkerStats agg = stats.Aggregate;
        foreach (OperationType op in agg.ActiveOps)
        {
            long success = agg.GetSuccess(op);
            long errors  = agg.GetErrors(op) + agg.GetTimeouts(op);
            long misses  = agg.GetMisses(op);
            double rps   = stats.ElapsedSeconds > 0 ? success / stats.ElapsedSeconds : 0;

            LongHistogram h = agg.GetHistogram(op);
            Console.WriteLine(string.Format(row,
                op.ToString().ToLowerInvariant(),
                success,
                rps,
                FormatMs(h, 50),
                FormatMs(h, 90),
                FormatMs(h, 99),
                FormatMs(h, 99.9),
                FormatMsMax(h),
                errors,
                misses));

            totalSuccess += success;
            totalErrors  += errors;
            totalMisses  += misses;
        }

        Console.WriteLine(new string('─', 98));

        // Aggregate across all active op types using the merged histogram.
        LongHistogram aggH = MergeAllHistograms(agg);
        double totalRps = stats.ElapsedSeconds > 0 ? totalSuccess / stats.ElapsedSeconds : 0;
        Console.WriteLine(string.Format(row,
            "TOTAL",
            totalSuccess,
            totalRps,
            FormatMs(aggH, 50),
            FormatMs(aggH, 90),
            FormatMs(aggH, 99),
            FormatMs(aggH, 99.9),
            FormatMsMax(aggH),
            totalErrors,
            totalMisses));

        Console.WriteLine();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static string FormatMs(LongHistogram h, double percentile)
    {
        if (h.TotalCount == 0)
            return "—";
        double micros = h.GetValueAtPercentile(percentile);
        return FormatMicros(micros);
    }

    private static string FormatMsMax(LongHistogram h)
    {
        if (h.TotalCount == 0)
            return "—";
        // HighestTrackableValue gives the configured max; use GetMaxValue extension if available,
        // otherwise derive from the p100 bucket.
        double micros = h.GetValueAtPercentile(100);
        return FormatMicros(micros);
    }

    private static string FormatMicros(double micros)
    {
        double ms = micros / 1000.0;
        return ms >= 1000 ? $"{ms / 1000:F1}s"
             : ms >= 1    ? $"{ms:F1}ms"
             :              $"{micros:F0}µs";
    }

    private static LongHistogram MergeAllHistograms(WorkerStats agg)
    {
        LongHistogram merged = new(agg.MaxMicros, 3);
        foreach (OperationType op in agg.ActiveOps)
            merged.Add(agg.GetHistogram(op));
        return merged;
    }
}
