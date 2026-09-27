using BenchmarkDotNet.Attributes;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Cost of <see cref="TerminalOutcomeWindow.Retain"/> on the commit path of every finalized transaction.
///
/// <para><b>Shape:</b> the window is pre-filled to its cap (the default 10 000), so every retain also evicts
/// one entry — the steady state of a server committing thousands of transactions per second, where the window
/// fills in about a second. <see cref="Callers"/> dedicated threads are released at once and each retains
/// <see cref="PerCaller"/> distinct ids, so the numbers include the cost of callers contending for the
/// window. The threading diagnoser reports monitor contentions: a retain path that takes a shared monitor
/// shows them here, and a convoy on that monitor shows as a collapse in throughput as callers rise.</para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class TerminalOutcomeWindowBenchmark
{
    private const int Max = 10_000;
    private const int PerCaller = 2_000;

    private static readonly FinalizeOutcome Committed = new(KeyValueResponseType.Committed, "anchor");

    [Params(1, 16, 128)]
    public int Callers;

    private TerminalOutcomeWindow window = null!;
    private Thread[] threads = null!;
    private ManualResetEventSlim start = null!;

    [IterationSetup]
    public void Setup()
    {
        window = new();

        for (long i = 0; i < Max; i++)
            window.Retain(new HLCTimestamp(0, i, 0), Committed, new HLCTimestamp(0, i, 0), Max);

        start = new(false);
        threads = new Thread[Callers];

        for (int c = 0; c < Callers; c++)
        {
            long first = Max + (long)c * PerCaller;
            threads[c] = new(() =>
            {
                start.Wait();

                for (long i = first; i < first + PerCaller; i++)
                {
                    HLCTimestamp id = new(0, i, 0);
                    window.Retain(id, Committed, id, Max);
                }
            });
            threads[c].Start();
        }
    }

    [Benchmark]
    public void Retain()
    {
        start.Set();

        foreach (Thread thread in threads)
            thread.Join();
    }

    [IterationCleanup]
    public void Cleanup() => start.Dispose();
}
