
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
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
    /// Categorised counts of exceptions thrown by operations during the measurement window. The
    /// per-op "errors" column only tells you how many failed; this tells you <em>why</em> — a Kahuna
    /// response code (e.g. MustRetry / Aborted) or a transport/exception type (e.g. RpcException,
    /// IOException). Populated from the op closures' catch path, snapshotted by the reporter.
    /// </summary>
    private static readonly ConcurrentDictionary<string, long> _errorCategories = new();

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

        // get/mixed need existing keys to read; delete/delete-many need existing keys to remove
        // (otherwise every op is a miss). set-many/txn write their own keys, so no seeding.
        if (opts.Workload.Equals("get", StringComparison.OrdinalIgnoreCase) ||
            opts.Workload.Equals("mixed", StringComparison.OrdinalIgnoreCase) ||
            opts.Workload.Equals("delete", StringComparison.OrdinalIgnoreCase) ||
            opts.Workload.Equals("delete-many", StringComparison.OrdinalIgnoreCase))
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
                        await client.SetKeyValue($"{opts.KeyPrefix}{idx}", val,
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
        int batchSize = Math.Max(1, opts.BatchSize);
        int keysPerTxn = Math.Max(1, opts.KeysPerTxn);
        string keyPrefix = opts.KeyPrefix;
        KeyValueTransactionLocking txnLocking = ParseTxnLocking(opts.TxnLocking);
        int txnTimeoutMs = opts.Timeout * 1000;

        KahunaTransactionScript? txScript = null;
        if (workloadNorm == "script" && !string.IsNullOrWhiteSpace(opts.Script))
        {
            string scriptText = File.ReadAllText(opts.Script);
            txScript = client.LoadTransactionScript(scriptText);
        }

        return async (outerCt) =>
        {
            int keyIndex = (Interlocked.Increment(ref _keyCounter) & 0x7FFFFFFF) % keySpace;
            string key = $"{keyPrefix}{keyIndex}";

            string resolved = workloadNorm == "mixed"
                ? (rng.Next(100) < readPct ? "get" : "set")
                : workloadNorm;

            OperationType opType = resolved switch
            {
                "get"         => OperationType.Get,
                "set"         => OperationType.Set,
                "delete"      => OperationType.Delete,
                "set-many"    => OperationType.SetMany,
                "delete-many" => OperationType.DeleteMany,
                "txn"         => OperationType.Transaction,
                "lock"        => OperationType.Lock,
                "sequence"    => OperationType.Sequence,
                "script"      => OperationType.Script,
                _             => OperationType.Get
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

                    case "delete":
                        KahunaKeyValue delResult = await client.DeleteKeyValue(key, kvDur, ct);
                        if (!delResult.Success)
                            return (opType, OpOutcome.Miss, ElapsedMicros(startTs));
                        break;

                    case "set-many":
                    {
                        // One batched request mutating `batchSize` distinct keys — exercises the
                        // partition-batched write path. Consecutive ops take non-overlapping key tiles
                        // (base * batchSize) so throughput isn't distorted by cross-op key aliasing.
                        rng.NextBytes(valuePayload);
                        List<KahunaSetKeyValueRequestItem> setItems = new(batchSize);
                        for (int j = 0; j < batchSize; j++)
                        {
                            int bk = (int)(((long)keyIndex * batchSize + j) % keySpace);
                            setItems.Add(new KahunaSetKeyValueRequestItem
                            {
                                Key = $"{keyPrefix}{bk}",
                                Value = valuePayload,
                                ExpiresMs = (int)TimeSpan.FromHours(5).TotalMilliseconds,
                                Flags = KeyValueFlags.Set,
                                Durability = kvDur
                            });
                        }
                        // The batch is one benchmark op, but SetManyKeyValues reports a per-key result
                        // (a key can come back non-Set on a write-intent conflict or error) and never
                        // throws for those. Count the whole op as an error if any key failed, so partial
                        // batch failures surface instead of being silently counted as success.
                        List<KahunaKeyValue> setResults = await client.SetManyKeyValues(setItems, ct);
                        int setRejected = setResults.Count(r => !r.Success);
                        if (setRejected > 0)
                        {
                            // Per-key rejection inside the batch (a key came back non-Set — typically a
                            // write-intent conflict with a concurrent batch). The client does not throw
                            // for this, so record it here. Count is key-level (how many keys were
                            // rejected), while the op's error is batch-level (this batch had ≥1).
                            RecordErrorCategory("SetMany:key-rejected", setRejected);
                            return (opType, OpOutcome.Error, ElapsedMicros(startTs));
                        }
                        break;
                    }

                    case "delete-many":
                    {
                        List<KahunaDeleteKeyValueRequestItem> delItems = new(batchSize);
                        for (int j = 0; j < batchSize; j++)
                        {
                            int bk = (int)(((long)keyIndex * batchSize + j) % keySpace);
                            delItems.Add(new KahunaDeleteKeyValueRequestItem
                            {
                                Key = $"{keyPrefix}{bk}",
                                Durability = kvDur
                            });
                        }
                        // Per-key results, same as set-many. A non-Deleted key is almost always a
                        // key that did not exist (the client's coarse Success bool can't separate that
                        // from a hard error), so treat any non-success key as a miss — consistent with
                        // how single-key delete/get report an absent key — rather than an error.
                        List<KahunaKeyValue> delManyResults = await client.DeleteManyKeyValues(delItems, ct);
                        if (delManyResults.Any(r => !r.Success))
                            return (opType, OpOutcome.Miss, ElapsedMicros(startTs));
                        break;
                    }

                    case "txn":
                    {
                        // Interactive (client-driven) transaction: open a session, write
                        // `keysPerTxn` keys under the chosen locking mode, then commit — the
                        // full working-set 2PC path (distinct from `script`, which runs a
                        // self-contained server-side script transaction).
                        KahunaTransactionOptions txnOpts = new()
                        {
                            Timeout = txnTimeoutMs,
                            Locking = txnLocking,
                            AutoCommit = false
                        };
                        await using KahunaTransactionSession session =
                            await client.StartTransactionSession(txnOpts, ct);

                        rng.NextBytes(valuePayload);
                        for (int j = 0; j < keysPerTxn; j++)
                        {
                            int tk = (int)(((long)keyIndex * keysPerTxn + j) % keySpace);
                            await session.SetKeyValue($"{keyPrefix}{tk}", valuePayload,
                                (int)TimeSpan.FromHours(5).TotalMilliseconds,
                                KeyValueFlags.Set, kvDur, ct);
                        }

                        if (!await session.Commit(ct))
                            return (opType, OpOutcome.Error, ElapsedMicros(startTs));
                        break;
                    }

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
            catch (Exception ex)
            {
                RecordError(ex);
                return (opType, OpOutcome.Error, ElapsedMicros(startTs));
            }
        };
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    public static void ResetKeyCounter() => Interlocked.Exchange(ref _keyCounter, 0);

    /// <summary>Clears the error-category tally (called at the start of the measurement window so the
    /// breakdown reflects measurement, not warmup).</summary>
    public static void ResetErrorCategories() => _errorCategories.Clear();

    /// <summary>Returns the error categories seen during measurement, most frequent first.</summary>
    public static IReadOnlyList<(string Category, long Count)> SnapshotErrorCategories() =>
        _errorCategories
            .Select(kv => (kv.Key, kv.Value))
            .OrderByDescending(t => t.Value)
            .ToList();

    /// <summary>Increments the tally for a named error category.</summary>
    private static void RecordErrorCategory(string category, long count = 1) =>
        _errorCategories.AddOrUpdate(category, count, (_, c) => c + count);

    /// <summary>Buckets a thrown exception into a human-readable category: a Kahuna response code when
    /// the client surfaced one, otherwise the exception type name.</summary>
    private static void RecordError(Exception ex) =>
        RecordErrorCategory(ex switch
        {
            KahunaException ke => $"Kahuna:{ke.KeyValueErrorCode}",
            _ => ex.GetType().Name
        });

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

    private static KeyValueTransactionLocking ParseTxnLocking(string s) =>
        s.Equals("optimistic", StringComparison.OrdinalIgnoreCase)
            ? KeyValueTransactionLocking.Optimistic
            : KeyValueTransactionLocking.Pessimistic;
}
