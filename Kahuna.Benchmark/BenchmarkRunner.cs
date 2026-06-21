
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Threading.Channels;
using Kahuna.Client;

namespace Kahuna.Benchmark;

/// <summary>
/// Orchestrates the warmup + measurement window, dispatching either a closed-loop
/// (<see cref="BenchmarkOptions.Rate"/> == 0) or open-loop (rate &gt; 0) runner.
/// </summary>
internal static class BenchmarkRunner
{
    public static async Task RunAsync(KahunaClient client, BenchmarkOptions opts)
    {
        using CancellationTokenSource globalCts = new();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            globalCts.Cancel();
        };

        // ── seed ──────────────────────────────────────────────────────────────
        Console.WriteLine("Seeding key-space…");
        try
        {
            using CancellationTokenSource seedCts = CancellationTokenSource.CreateLinkedTokenSource(globalCts.Token);
            seedCts.CancelAfter(TimeSpan.FromSeconds(30));
            await WorkloadGenerator.SeedAsync(client, opts, seedCts.Token);
        }
        catch (OperationCanceledException) when (globalCts.IsCancellationRequested)
        {
            Console.WriteLine("Aborted during seeding.");
            return;
        }

        // ── warmup ────────────────────────────────────────────────────────────
        if (opts.Warmup > 0 && !globalCts.IsCancellationRequested)
        {
            Console.WriteLine($"Warming up for {opts.Warmup}s…");
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
            Console.WriteLine("Aborted during warmup.");
            return;
        }

        // ── measurement ───────────────────────────────────────────────────────
        Console.WriteLine($"Running measurement for {opts.Duration}s…");
        WorkloadGenerator.ResetKeyCounter();

        using CancellationTokenSource measureCts =
            CancellationTokenSource.CreateLinkedTokenSource(globalCts.Token);
        measureCts.CancelAfter(TimeSpan.FromSeconds(opts.Duration));

        Stopwatch measureSw = Stopwatch.StartNew();

        // Stop the clock the instant the deadline (or Ctrl+C) fires — before the
        // open-loop drain flushes buffered tickets. The drain still runs so in-flight
        // ops complete and their results are counted, but elapsed is pinned to the
        // true measurement boundary, not the end of the drain.
        using CancellationTokenRegistration _ = measureCts.Token.Register(() => measureSw.Stop());

        // Workers swallow cancellation internally and return their accumulated stats,
        // so the runners always complete normally — no catch needed here.
        IReadOnlyList<WorkerStats> workerStats = opts.Rate > 0
            ? await RunOpenLoopPhaseAsync(client, opts, measureCts.Token)
            : await RunClosedLoopPhaseAsync(client, opts, measureCts.Token);

        double elapsed = measureSw.Elapsed.TotalSeconds;

        BenchmarkStats stats = BenchmarkStats.Merge(workerStats, elapsed, opts);
        BenchmarkReporter.PrintConsole(stats);
    }

    // ── closed-loop runner (B1) ───────────────────────────────────────────────

    private static async Task<IReadOnlyList<WorkerStats>> RunClosedLoopPhaseAsync(
        KahunaClient client,
        BenchmarkOptions opts,
        CancellationToken ct)
    {
        WorkerStats[] stats = new WorkerStats[opts.Concurrency];
        Task[] workers = new Task[opts.Concurrency];

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
                        (OperationType opType, OpOutcome outcome, long elapsedMicros) = await op(ct);
                        workerStats.Record(opType, outcome, elapsedMicros);
                    }
                    catch (OperationCanceledException)
                    {
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
    /// </summary>
    private static async Task<IReadOnlyList<WorkerStats>> RunOpenLoopPhaseAsync(
        KahunaClient client,
        BenchmarkOptions opts,
        CancellationToken ct)
    {
        int rate = opts.Rate;

        // Channel capacity: allow up to 2 seconds of backlog so the producer is
        // never immediately stalled. A deeper backlog inflates latency correctly.
        int capacity = Math.Max(rate * 2, opts.Concurrency * 4);
        Channel<long> dispatch = Channel.CreateBounded<long>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true
        });

        WorkerStats[] stats = new WorkerStats[opts.Concurrency];

        // Producer: emits intended-start timestamps at 1/rate-second intervals.
        //
        // Pacing strategy — two-phase per iteration to work correctly at any rate:
        //
        //   1. Batch-emit: issue ALL overdue tickets immediately (nextIntended ≤ now).
        //      At high rates (e.g. 50 000 req/s, 20 µs interval) the OS timer
        //      resolution (~1 ms) means we wake up every ~1 ms and release ~50 tickets
        //      at once. CO timestamps still advance in 20 µs steps, so coordinated-
        //      omission accounting remains correct.
        //
        //   2. Hybrid wait: once caught up, sleep for (interval - 1 ms) with
        //      Task.Delay so the thread is cooperative, then spin on
        //      Stopwatch.GetTimestamp() for the sub-millisecond remainder.
        //      For intervals < 1 ms (rate > ~1 000) step 2 is pure spin; the producer
        //      burns one thread-pool thread but achieves µs-level pacing accuracy.
        Task producer = Task.Run(async () =>
        {
            long ticksPerRequest = Stopwatch.Frequency / rate;

            // One tick = 1 / Stopwatch.Frequency seconds.
            // 1 ms in ticks:
            long oneMilliTicks = Stopwatch.Frequency / 1_000;

            long nextIntended = Stopwatch.GetTimestamp();

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    // ── Phase 1: batch-emit all overdue tickets ────────────────
                    long now = Stopwatch.GetTimestamp();
                    while (nextIntended <= now && !ct.IsCancellationRequested)
                    {
                        await dispatch.Writer.WriteAsync(nextIntended, ct);
                        nextIntended += ticksPerRequest;
                    }

                    // ── Phase 2: hybrid wait until nextIntended ────────────────
                    long waitTicks = nextIntended - Stopwatch.GetTimestamp();
                    if (waitTicks > 0)
                    {
                        // Coarse sleep: give up the thread for all but the last ms.
                        long coarseTicks = waitTicks - oneMilliTicks;
                        if (coarseTicks > 0)
                        {
                            int sleepMs = (int)(coarseTicks * 1_000 / Stopwatch.Frequency);
                            if (sleepMs > 0)
                                await Task.Delay(sleepMs, ct);
                        }
                        // Spin for the sub-millisecond tail (or the whole interval
                        // when ticksPerRequest < oneMilliTicks).
                        while (Stopwatch.GetTimestamp() < nextIntended && !ct.IsCancellationRequested)
                            Thread.SpinWait(20);
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                dispatch.Writer.TryComplete();
            }
        });

        // Consumers: read an intended-start ticket, measure latency from it.
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

                    // Latency is measured from the *intended* start: any queuing
                    // delay is included, making tail latency reflect true server lag.
                    long coStartTs = intendedStartTs;

                    OperationType opType = OperationType.Get;
                    OpOutcome outcome = OpOutcome.Error;
                    try
                    {
                        (opType, outcome, _) = await op(ct);
                    }
                    catch (OperationCanceledException) { break; }

                    long elapsedMicros = (long)((Stopwatch.GetTimestamp() - coStartTs) * 1_000_000.0
                                                / Stopwatch.Frequency);
                    workerStats.Record(opType, outcome, elapsedMicros);
                }
            });
        }

        await Task.WhenAll(consumers);
        await producer;
        return stats;
    }
}
