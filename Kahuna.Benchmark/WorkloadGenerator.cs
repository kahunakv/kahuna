
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;

namespace Kahuna.Benchmark;

/// <summary>
/// Produces async operation functions for each supported workload type.
/// Each call to <see cref="Create"/> returns a closure bound to its own value
/// buffer and RNG so workers do not share mutable state.
/// </summary>
internal static class WorkloadGenerator
{
    private static int _keyCounter;

    /// <summary>
    /// Pre-seeds the server with data the benchmark needs to be meaningful.
    /// For <c>get</c>: writes a sample of keys so reads can find them.
    /// For <c>sequence</c>: creates the shared sequence if it does not exist.
    /// </summary>
    public static async Task SeedAsync(KahunaClient client, BenchmarkOptions opts, CancellationToken ct, TextWriter? diag = null)
    {
        if (opts.Workload.Equals("sequence", StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                await client.CreateSequence("bench:seq:0", initialValue: 0, increment: 1,
                    cancellationToken: ct);
            }
            catch
            {
                // Already exists — fine.
            }
        }

        if (opts.Workload.Equals("get", StringComparison.OrdinalIgnoreCase) ||
            opts.Workload.Equals("mixed", StringComparison.OrdinalIgnoreCase))
        {
            KeyValueDurability dur = ParseKvDurability(opts.Durability);
            // Cap at 100 000 to keep startup time bounded; for key-spaces larger than
            // that, workers will still hit some unseeded keys but the miss counter in
            // the report makes this visible rather than conflating it with hits.
            int seedCount = Math.Min(opts.KeySpace, 100_000);
            int parallelism = Math.Min(opts.Concurrency, 64);

            (diag ?? Console.Out).WriteLine($"  Seeding {seedCount:N0} keys (parallelism={parallelism})…");

            await Parallel.ForEachAsync(
                Enumerable.Range(0, seedCount),
                new ParallelOptions { MaxDegreeOfParallelism = parallelism, CancellationToken = ct },
                async (idx, innerCt) =>
                {
                    byte[] val = new byte[opts.ValueSize];
                    Random.Shared.NextBytes(val);
                    try
                    {
                        await client.SetKeyValue($"bench:{idx}", val,
                            (int)TimeSpan.FromHours(5).TotalMilliseconds,
                            KeyValueFlags.Set, dur, innerCt);
                    }
                    catch { }
                });
        }
    }

    /// <summary>
    /// Returns an operation function for the given <paramref name="opts"/> workload.
    /// The closure is self-contained: safe to call concurrently from many workers,
    /// each with its own <paramref name="rng"/> instance.
    /// </summary>
    public static Func<CancellationToken, ValueTask<(OperationType op, OpOutcome outcome, long elapsedMicros)>>
        Create(KahunaClient client, BenchmarkOptions opts, Random rng)
    {
        KeyValueDurability kvDur = ParseKvDurability(opts.Durability);
        LockDurability lockDur = ParseLockDurability(opts.Durability);

        byte[] valuePayload = new byte[opts.ValueSize];
        rng.NextBytes(valuePayload);

        string workloadNorm = opts.Workload.ToLowerInvariant();
        int keySpace = opts.KeySpace;
        int readPct = opts.ReadPct;
        int timeoutSeconds = opts.Timeout;

        KahunaTransactionScript? txScript = null;
        if (workloadNorm == "script" && !string.IsNullOrWhiteSpace(opts.Script))
        {
            string scriptText = File.ReadAllText(opts.Script);
            txScript = client.LoadTransactionScript(scriptText);
        }

        return async (outerCt) =>
        {
            int keyIndex = (Interlocked.Increment(ref _keyCounter) & 0x7FFFFFFF) % keySpace;
            string key = $"bench:{keyIndex}";

            string resolved = workloadNorm == "mixed"
                ? (rng.Next(100) < readPct ? "get" : "set")
                : workloadNorm;

            OperationType opType = resolved switch
            {
                "get"      => OperationType.Get,
                "set"      => OperationType.Set,
                "lock"     => OperationType.Lock,
                "sequence" => OperationType.Sequence,
                "script"   => OperationType.Script,
                _          => OperationType.Get
            };

            using CancellationTokenSource perReqCts =
                CancellationTokenSource.CreateLinkedTokenSource(outerCt);
            perReqCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));
            CancellationToken ct = perReqCts.Token;

            long startTs = Stopwatch.GetTimestamp();
            try
            {
                switch (resolved)
                {
                    case "get":
                        KahunaKeyValue getResult = await client.GetKeyValue(key, kvDur, cancellationToken: ct);
                        if (!getResult.Success)
                            return (opType, OpOutcome.Miss, ElapsedMicros(startTs));
                        break;

                    case "set":
                        rng.NextBytes(valuePayload);
                        await client.SetKeyValue(key, valuePayload,
                            (int)TimeSpan.FromHours(5).TotalMilliseconds,
                            KeyValueFlags.Set, kvDur, ct);
                        break;

                    case "lock":
                        await using (KahunaLock lk = await client.GetOrCreateLock(
                            key, TimeSpan.FromSeconds(5), lockDur, ct))
                        {
                            if (!lk.IsAcquired)
                                throw new KahunaException($"Lock not acquired: {key}", LockResponseType.Busy);
                        }
                        break;

                    case "sequence":
                        await client.NextSequenceValue("bench:seq:0",
                            durability: SequenceDurability.Persistent, cancellationToken: ct);
                        break;

                    case "script":
                        await txScript!.Run(cancellationToken: ct);
                        break;
                }

                return (opType, OpOutcome.Success, ElapsedMicros(startTs));
            }
            catch (OperationCanceledException) when (!outerCt.IsCancellationRequested)
            {
                // Per-request timeout fired; outer loop is still running.
                return (opType, OpOutcome.Timeout, ElapsedMicros(startTs));
            }
            catch (OperationCanceledException)
            {
                // Outer cancellation (warmup→measure boundary or Ctrl+C) — not a timeout.
                throw;
            }
            catch
            {
                return (opType, OpOutcome.Error, ElapsedMicros(startTs));
            }
        };
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    public static void ResetKeyCounter() => Interlocked.Exchange(ref _keyCounter, 0);

    private static long ElapsedMicros(long startTimestamp) =>
        (long)((Stopwatch.GetTimestamp() - startTimestamp) * 1_000_000.0 / Stopwatch.Frequency);

    private static KeyValueDurability ParseKvDurability(string s) =>
        s.Equals("ephemeral", StringComparison.OrdinalIgnoreCase)
            ? KeyValueDurability.Ephemeral
            : KeyValueDurability.Persistent;

    private static LockDurability ParseLockDurability(string s) =>
        s.Equals("ephemeral", StringComparison.OrdinalIgnoreCase)
            ? LockDurability.Ephemeral
            : LockDurability.Persistent;
}
