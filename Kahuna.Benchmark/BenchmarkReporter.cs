
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;
using HdrHistogram;
using Spectre.Console;

namespace Kahuna.Benchmark;

internal static class BenchmarkReporter
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = true };

    public static async Task ReportAsync(BenchmarkStats stats, BenchmarkOptions opts)
    {
        if (opts.Format.Equals("console", StringComparison.OrdinalIgnoreCase))
        {
            PrintConsoleTable(stats);
            return;
        }

        BenchmarkOutput output = BuildOutput(stats, opts);
        bool toFile = !string.IsNullOrEmpty(opts.Output);

        // Console.Out must not be disposed; only close file streams.
        TextWriter writer = toFile
            ? new StreamWriter(opts.Output!, append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false))
            : Console.Out;

        try
        {
            if (opts.Format.Equals("json", StringComparison.OrdinalIgnoreCase))
                await WriteJsonAsync(output, writer);
            else
                WriteCsv(output, writer);
        }
        finally
        {
            if (toFile)
                await writer.DisposeAsync();
        }
    }

    // ── console (Spectre table) ───────────────────────────────────────────────

    private static void PrintConsoleTable(BenchmarkStats stats)
    {
        Console.WriteLine();

        WorkerStats agg     = stats.Aggregate;
        double      elapsed = stats.ElapsedSeconds;

        Table table = new Table()
            .Border(TableBorder.Simple)
            .AddColumn("Operation")
            .AddColumn(new TableColumn("Count").RightAligned())
            .AddColumn(new TableColumn("req/s").RightAligned())
            .AddColumn(new TableColumn("p50").RightAligned())
            .AddColumn(new TableColumn("p90").RightAligned())
            .AddColumn(new TableColumn("p95").RightAligned())
            .AddColumn(new TableColumn("p99").RightAligned())
            .AddColumn(new TableColumn("p99.9").RightAligned())
            .AddColumn(new TableColumn("max").RightAligned())
            .AddColumn(new TableColumn("mean").RightAligned())
            .AddColumn(new TableColumn("errors").RightAligned())
            .AddColumn(new TableColumn("misses").RightAligned());

        long          totalSuccess = 0, totalErrors = 0, totalMisses = 0;
        LongHistogram totalHist   = new(agg.MaxMicros, 3);
        double        wMeanSum    = 0;

        foreach (OperationType op in agg.ActiveOps)
        {
            long   success    = agg.GetSuccess(op);
            long   errors     = agg.GetErrors(op) + agg.GetTimeouts(op);
            long   misses     = agg.GetMisses(op);
            double rps        = elapsed > 0 ? success / elapsed : 0;
            double meanMicros = agg.GetMeanMicros(op);
            LongHistogram h   = agg.GetHistogram(op);

            table.AddRow(
                op.ToString().ToLowerInvariant(),
                $"{success:N0}",
                $"{rps:N0}",
                FmtMs(h, 50),
                FmtMs(h, 90),
                FmtMs(h, 95),
                FmtMs(h, 99),
                FmtMs(h, 99.9),
                FmtMsMax(h),
                FmtMicros(meanMicros),
                $"{errors:N0}",
                $"{misses:N0}");

            totalSuccess += success;
            totalErrors  += errors;
            totalMisses  += misses;
            totalHist.Add(h);
            wMeanSum += meanMicros * success;
        }

        double totalRps       = elapsed > 0 ? totalSuccess / elapsed : 0;
        double totalMeanMicros = totalSuccess > 0 ? wMeanSum / totalSuccess : 0;

        table.AddRow(
            "[bold]TOTAL[/]",
            $"[bold]{totalSuccess:N0}[/]",
            $"[bold]{totalRps:N0}[/]",
            FmtMs(totalHist, 50),
            FmtMs(totalHist, 90),
            FmtMs(totalHist, 95),
            FmtMs(totalHist, 99),
            FmtMs(totalHist, 99.9),
            FmtMsMax(totalHist),
            FmtMicros(totalMeanMicros),
            $"{totalErrors:N0}",
            $"{totalMisses:N0}");

        AnsiConsole.Write(table);
        Console.WriteLine();

        // Break the "errors" column down by cause so a non-zero count is actionable — a Kahuna
        // response code (MustRetry, Aborted, …) points at contention/leadership; a transport type
        // (RpcException, IOException, …) points at the network or a node being unreachable.
        IReadOnlyList<(string Category, long Count)> errorBreakdown = WorkloadGenerator.SnapshotErrorCategories();
        if (errorBreakdown.Count > 0)
        {
            AnsiConsole.MarkupLine("[bold]error breakdown[/]");
            foreach ((string category, long count) in errorBreakdown)
                Console.WriteLine($"  {count,10:N0}  {category}");
            Console.WriteLine();
        }
    }

    // ── JSON ─────────────────────────────────────────────────────────────────

    private static async Task WriteJsonAsync(BenchmarkOutput output, TextWriter writer)
    {
        string json = JsonSerializer.Serialize(output, JsonOpts);
        await writer.WriteAsync(json);
        await writer.FlushAsync();
    }

    // ── CSV ──────────────────────────────────────────────────────────────────

    private static void WriteCsv(BenchmarkOutput output, TextWriter outerWriter)
    {
        // Write into a StringBuilder so the CsvWriter can be fully disposed (flushed)
        // before we hand the complete string to the outer TextWriter.
        StringBuilder sb = new();
        using (StringWriter sw = new(sb))
        using (CsvWriter csv = new(sw, new CsvConfiguration(CultureInfo.InvariantCulture)))
        {
            csv.WriteField("Operation");
            csv.WriteField("Count");
            csv.WriteField("Rps");
            csv.WriteField("P50Ms");
            csv.WriteField("P90Ms");
            csv.WriteField("P95Ms");
            csv.WriteField("P99Ms");
            csv.WriteField("P999Ms");
            csv.WriteField("MaxMs");
            csv.WriteField("MeanMs");
            csv.WriteField("Errors");
            csv.WriteField("Timeouts");
            csv.WriteField("Misses");
            csv.NextRecord();

            foreach (OpStats r in output.Operations)
                WriteCsvRow(csv, r);

            WriteCsvRow(csv, output.Aggregate);
        }

        outerWriter.Write(sb.ToString());
        outerWriter.Flush();
    }

    private static void WriteCsvRow(CsvWriter csv, OpStats r)
    {
        csv.WriteField(r.Operation);
        csv.WriteField(r.Count);
        csv.WriteField(Math.Round(r.Rps, 2));
        csv.WriteField(Math.Round(r.P50Ms, 3));
        csv.WriteField(Math.Round(r.P90Ms, 3));
        csv.WriteField(Math.Round(r.P95Ms, 3));
        csv.WriteField(Math.Round(r.P99Ms, 3));
        csv.WriteField(Math.Round(r.P999Ms, 3));
        csv.WriteField(Math.Round(r.MaxMs, 3));
        csv.WriteField(Math.Round(r.MeanMs, 3));
        csv.WriteField(r.Errors);
        csv.WriteField(r.Timeouts);
        csv.WriteField(r.Misses);
        csv.NextRecord();
    }

    // ── data builder (shared by JSON and CSV) ─────────────────────────────────

    private static BenchmarkOutput BuildOutput(BenchmarkStats stats, BenchmarkOptions opts)
    {
        string[] endpoints = opts.ConnectionSource
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        WorkerStats agg     = stats.Aggregate;
        double      elapsed = stats.ElapsedSeconds;

        List<OpStats> opRows  = [];
        long totalSuccess = 0, totalErrors = 0, totalTimeouts = 0, totalMisses = 0;
        LongHistogram totalHist = new(agg.MaxMicros, 3);
        double wMeanSum = 0;

        foreach (OperationType op in agg.ActiveOps)
        {
            long   success    = agg.GetSuccess(op);
            long   errors     = agg.GetErrors(op);
            long   timeouts   = agg.GetTimeouts(op);
            long   misses     = agg.GetMisses(op);
            double rps        = elapsed > 0 ? success / elapsed : 0;
            double meanMicros = agg.GetMeanMicros(op);
            LongHistogram h   = agg.GetHistogram(op);

            opRows.Add(new OpStats
            {
                Operation = op.ToString().ToLowerInvariant(),
                Count     = success,
                Rps       = Math.Round(rps, 2),
                P50Ms     = ToMs(h, 50),
                P90Ms     = ToMs(h, 90),
                P95Ms     = ToMs(h, 95),
                P99Ms     = ToMs(h, 99),
                P999Ms    = ToMs(h, 99.9),
                MaxMs     = ToMs(h, 100),
                MeanMs    = Math.Round(meanMicros / 1000.0, 3),
                Errors    = errors,
                Timeouts  = timeouts,
                Misses    = misses,
            });

            totalSuccess  += success;
            totalErrors   += errors;
            totalTimeouts += timeouts;
            totalMisses   += misses;
            totalHist.Add(h);
            wMeanSum += meanMicros * success;
        }

        double totalRps        = elapsed > 0 ? totalSuccess / elapsed : 0;
        double totalMeanMicros = totalSuccess > 0 ? wMeanSum / totalSuccess : 0;

        return new BenchmarkOutput
        {
            Parameters = new RunParameters
            {
                Workload    = opts.Workload,
                Duration    = opts.Duration,
                Warmup      = opts.Warmup,
                Concurrency = opts.Concurrency,
                Rate        = opts.Rate,
                KeySpace    = opts.KeySpace,
                ValueSize   = opts.ValueSize,
                Durability  = opts.Durability,
                Endpoints   = endpoints,
                Seed        = opts.Seed,
                Timeout     = opts.Timeout,
            },
            ElapsedSec = Math.Round(elapsed, 3),
            Operations = opRows,
            Aggregate  = new OpStats
            {
                Operation = "TOTAL",
                Count     = totalSuccess,
                Rps       = Math.Round(totalRps, 2),
                P50Ms     = ToMs(totalHist, 50),
                P90Ms     = ToMs(totalHist, 90),
                P95Ms     = ToMs(totalHist, 95),
                P99Ms     = ToMs(totalHist, 99),
                P999Ms    = ToMs(totalHist, 99.9),
                MaxMs     = ToMs(totalHist, 100),
                MeanMs    = Math.Round(totalMeanMicros / 1000.0, 3),
                Errors    = totalErrors,
                Timeouts  = totalTimeouts,
                Misses    = totalMisses,
            },
        };
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static string FmtMs(LongHistogram h, double percentile)
    {
        if (h.TotalCount == 0) return "—";
        return FmtMicros(h.GetValueAtPercentile(percentile));
    }

    private static string FmtMsMax(LongHistogram h)
    {
        if (h.TotalCount == 0) return "—";
        return FmtMicros(h.GetValueAtPercentile(100));
    }

    private static string FmtMicros(double micros)
    {
        double ms = micros / 1000.0;
        return ms >= 1000 ? $"{ms / 1000:F1}s"
             : ms >= 1    ? $"{ms:F1}ms"
             :              $"{micros:F0}µs";
    }

    private static double ToMs(LongHistogram h, double percentile) =>
        h.TotalCount > 0 ? Math.Round(h.GetValueAtPercentile(percentile) / 1000.0, 3) : 0;
}
