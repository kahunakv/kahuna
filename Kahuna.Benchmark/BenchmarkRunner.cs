
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Threading.Channels;
using Kahuna.Client;
using Spectre.Console;

namespace Kahuna.Benchmark;

/// <summary>
/// Orchestrates seed → warmup → measurement, dispatching either a closed-loop
/// (<see cref="BenchmarkOptions.Rate"/> == 0) or open-loop (rate &gt; 0) runner.
/// </summary>
internal static class BenchmarkRunner
{
    public static async Task RunAsync(KahunaClient client, BenchmarkOptions opts, TextWriter diag)
    {
        using CancellationTokenSource globalCts = new();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; globalCts.Cancel(); };

        // ── seed ──────────────────────────────────────────────────────────────
        diag.WriteLine("Seeding key-space…");
        try
        {
            using CancellationTokenSource seedCts =
                CancellationTokenSource.CreateLinkedTokenSource(globalCts.Token);
            seedCts.CancelAfter(TimeSpan.FromSeconds(30));
            await WorkloadGenerator.SeedAsync(client, opts, seedCts.Token, diag);
        }
        catch (OperationCanceledException) when (globalCts.IsCancellationRequested)
        {
            diag.WriteLine("Aborted during seeding.");
            return;
        }
        catch (OperationCanceledException)
        {
            // Seed timeout expired (30 s). Proceed with whatever keys were written;
            // workers will encounter misses for unseeded keys but the report tracks that.
            diag.WriteLine("Seeding timed out; proceeding with partial key-space.");
        }

        // ── warmup ────────────────────────────────────────────────────────────
        if (opts.Warmup > 0 && !globalCts.IsCancellationRequested)
        {
            diag.WriteLine($"Warming up for {opts.Warmup}s…");
            WorkloadGenerator.ResetKeyCounter();

            using CancellationTokenSource warmupCts =
                CancellationTokenSource.CreateLinkedTokenSource(globalCts.Token);
            warmupCts.CancelAfter(TimeSpan.FromSeconds(opts.Warmup));

            try
            {
                if (opts.Rate > 0)
                    await RunOpenLoopPhaseAsync(client, opts, warmupCts.Token);
                else
                    await RunClosedLoopPhaseAsync(client, opts, warmupCts.Token);
            }
            catch (OperationCanceledException) when (!globalCts.IsCancellationRequested)
            {
                // Expected: warmup deadline fired.
            }
        }

        if (globalCts.IsCancellationRequested)
        {
            diag.WriteLine("Aborted during warmup.");
            return;
        }

        // ── measurement ───────────────────────────────────────────────────────
        diag.WriteLine($"Running measurement for {opts.Duration}s…");
        WorkloadGenerator.ResetKeyCounter();
        WorkloadGenerator.ResetErrorCategories();

        long ceiling   = WorkerStats.CeilingMicrosFromTimeout(opts.Timeout);
        bool isConsole = opts.Format.Equals("console", StringComparison.OrdinalIgnoreCase);

        // LiveMetrics is only needed when the live display is running.
        // For json/csv runs, pass null so workers skip EndOp entirely —
        // no lock, no histogram write, no observer effect on measured latency.
        LiveMetrics? liveMetrics = isConsole ? new(ceiling) : null;

        using CancellationTokenSource measureCts =
            CancellationTokenSource.CreateLinkedTokenSource(globalCts.Token);
        measureCts.CancelAfter(TimeSpan.FromSeconds(opts.Duration));

        Stopwatch measureSw = Stopwatch.StartNew();
        using CancellationTokenRegistration _reg =
            measureCts.Token.Register(() => measureSw.Stop());

        using CancellationTokenSource liveCts = new();

        Task liveTask = isConsole
            ? RunLiveDisplayAsync(liveMetrics!, opts.Duration, measureSw, liveCts.Token)
            : Task.CompletedTask;

        // Workers swallow cancellation internally and always return their accumulated
        // stats — the runners complete normally on deadline or Ctrl+C.
        IReadOnlyList<WorkerStats> workerStats = opts.Rate > 0
            ? await RunOpenLoopPhaseAsync(client, opts, measureCts.Token, liveMetrics)
            : await RunClosedLoopPhaseAsync(client, opts, measureCts.Token, liveMetrics);

        await liveCts.CancelAsync();
        await liveTask;

        double elapsed = measureSw.Elapsed.TotalSeconds;

        BenchmarkStats stats = BenchmarkStats.Merge(workerStats, elapsed, opts);
        await BenchmarkReporter.ReportAsync(stats, opts);
    }

    // ── live display ─────────────────────────────────────────────────────────

    // Updates a single status line in place using CR + "erase line" (ESC[2K). This deliberately
    // avoids Spectre's Live region: a one-line status doesn't need multi-line region accounting,
    // and Live was moving the cursor up and clobbering surrounding output (build warnings, the
    // command echo) when its region estimate didn't match the real terminal. CR-rewrite only ever
    // touches the current line, so it can't corrupt anything above it.
    private static async Task RunLiveDisplayAsync(
        LiveMetrics metrics, int durationSec, Stopwatch measureSw, CancellationToken ct)
    {
        // No usable terminal (piped/redirected or no ANSI) — skip the live line; the final
        // table is the authoritative output and raw escapes would just become noise.
        if (Console.IsOutputRedirected || !AnsiConsole.Profile.Capabilities.Ansi)
            return;

        const string clearLine = "\r\u001b[2K"; // CR + ESC[2K: return to col 0 and erase the whole line
        long prevSuccess = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                try { await Task.Delay(1000, ct); }
                catch (OperationCanceledException) { break; }

                long total    = metrics.TotalSuccess;
                long rps      = total - prevSuccess;
                prevSuccess   = total;
                long inFlight = metrics.InFlight;
                (long p99Micros, _) = metrics.SnapshotInterval();
                int  elapsedSec = (int)measureSw.Elapsed.TotalSeconds;

                string p99Str = p99Micros > 0 ? FormatMicros(p99Micros) : "—";

                Console.Out.Write(
                    $"{clearLine}  elapsed {elapsedSec,3}s/{durationSec}s  " +
                    $"in-flight {inFlight,5}  rps {rps,8:N0}  p99 {p99Str,8}");
                Console.Out.Flush();
            }
        }
        finally
        {
            // Wipe the status line and return the cursor to column 0 so the final table
            // starts on a clean line instead of after the last status text.
            Console.Out.Write(clearLine);
            Console.Out.Flush();
        }
    }

    // ── closed-loop runner (B1) ───────────────────────────────────────────────

    private static async Task<IReadOnlyList<WorkerStats>> RunClosedLoopPhaseAsync(
        KahunaClient client,
        BenchmarkOptions opts,
        CancellationToken ct,
        LiveMetrics? liveMetrics = null)
    {
        WorkerStats[] stats  = new WorkerStats[opts.Concurrency];
        Task[]        workers = new Task[opts.Concurrency];

        for (int i = 0; i < opts.Concurrency; i++)
        {
            int workerIdx = i;
            stats[workerIdx] = new WorkerStats(WorkerStats.CeilingMicrosFromTimeout(opts.Timeout));
            Random workerRng = opts.Seed == 0
                ? new Random(Environment.TickCount + workerIdx)
                : new Random(opts.Seed + workerIdx);

            var op = WorkloadGenerator.Create(client, opts, workerRng);
            WorkerStats workerStats = stats[workerIdx];

            workers[i] = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        liveMetrics?.BeginOp();
                        (OperationType opType, OpOutcome outcome, long elapsedMicros) = await op(ct);
                        liveMetrics?.EndOp(outcome, elapsedMicros);
                        workerStats.Record(opType, outcome, elapsedMicros);
                    }
                    catch (OperationCanceledException)
                    {
                        liveMetrics?.AbortOp();
                        break;
                    }
                }
            });
        }

        await Task.WhenAll(workers);
        return stats;
    }

    // ── open-loop runner (B2) — coordinated-omission corrected ───────────────

    /// <summary>
    /// Schedules requests at the target rate by assigning each an <em>intended start
    /// time</em>. Latency is measured from that intended start rather than the actual
    /// dispatch, so queuing delay caused by a saturated server inflates the measured
    /// tail latency rather than being silently hidden (coordinated-omission fix).
    ///
    /// Pacing — two-phase per producer iteration:
    ///   1. Batch-emit all overdue tickets (nextIntended ≤ now) in a tight loop.
    ///      At high rates the OS timer resolution (~1 ms) means tickets are released
    ///      in bursts; CO timestamps still advance at 1/rate-second steps so
    ///      coordinated-omission accounting stays correct.
    ///   2. Hybrid wait: Task.Delay for the coarse portion (interval − 1 ms), then
    ///      Thread.SpinWait for the sub-millisecond tail, achieving µs-level pacing
    ///      accuracy at the cost of one thread-pool thread spinning.
    /// </summary>
    private static async Task<IReadOnlyList<WorkerStats>> RunOpenLoopPhaseAsync(
        KahunaClient client,
        BenchmarkOptions opts,
        CancellationToken ct,
        LiveMetrics? liveMetrics = null)
    {
        int rate     = opts.Rate;
        int capacity = Math.Max(rate * 2, opts.Concurrency * 4);

        Channel<long> dispatch = Channel.CreateBounded<long>(new BoundedChannelOptions(capacity)
        {
            FullMode    = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true
        });

        WorkerStats[] stats = new WorkerStats[opts.Concurrency];

        Task producer = Task.Run(async () =>
        {
            long ticksPerRequest = Stopwatch.Frequency / rate;
            long oneMilliTicks   = Stopwatch.Frequency / 1_000;
            long nextIntended    = Stopwatch.GetTimestamp();

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    // Phase 1: batch-emit all overdue tickets.
                    long now = Stopwatch.GetTimestamp();
                    while (nextIntended <= now && !ct.IsCancellationRequested)
                    {
                        await dispatch.Writer.WriteAsync(nextIntended, ct);
                        nextIntended += ticksPerRequest;
                    }

                    // Phase 2: hybrid wait until nextIntended.
                    long waitTicks = nextIntended - Stopwatch.GetTimestamp();
                    if (waitTicks > 0)
                    {
                        long coarseTicks = waitTicks - oneMilliTicks;
                        if (coarseTicks > 0)
                        {
                            int sleepMs = (int)(coarseTicks * 1_000 / Stopwatch.Frequency);
                            if (sleepMs > 0)
                                await Task.Delay(sleepMs, ct);
                        }
                        while (Stopwatch.GetTimestamp() < nextIntended && !ct.IsCancellationRequested)
                            Thread.SpinWait(20);
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally { dispatch.Writer.TryComplete(); }
        });

        Task[] consumers = new Task[opts.Concurrency];
        for (int i = 0; i < opts.Concurrency; i++)
        {
            int workerIdx = i;
            stats[workerIdx] = new WorkerStats(WorkerStats.CeilingMicrosFromTimeout(opts.Timeout));
            Random workerRng = opts.Seed == 0
                ? new Random(Environment.TickCount + workerIdx)
                : new Random(opts.Seed + workerIdx);

            var op = WorkloadGenerator.Create(client, opts, workerRng);
            WorkerStats workerStats = stats[workerIdx];

            consumers[i] = Task.Run(async () =>
            {
                await foreach (long intendedStartTs in dispatch.Reader.ReadAllAsync(CancellationToken.None))
                {
                    if (ct.IsCancellationRequested)
                        break;

                    liveMetrics?.BeginOp();

                    OperationType opType  = OperationType.Get;
                    OpOutcome     outcome = OpOutcome.Error;
                    try
                    {
                        (opType, outcome, _) = await op(ct);
                    }
                    catch (OperationCanceledException)
                    {
                        liveMetrics?.AbortOp();
                        break;
                    }

                    // Latency measured from the *intended* start: queuing delay is
                    // included, so tail latency reflects true server lag.
                    long elapsedMicros = (long)((Stopwatch.GetTimestamp() - intendedStartTs)
                                                * 1_000_000.0 / Stopwatch.Frequency);
                    liveMetrics?.EndOp(outcome, elapsedMicros);
                    workerStats.Record(opType, outcome, elapsedMicros);
                }
            });
        }

        await Task.WhenAll(consumers);
        await producer;
        return stats;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static string FormatMicros(long micros)
    {
        double ms = micros / 1000.0;
        return ms >= 1000 ? $"{ms / 1000:F1}s"
             : ms >= 1    ? $"{ms:F1}ms"
             :              $"{micros}µs";
    }
}
