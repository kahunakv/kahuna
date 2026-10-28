using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A bank-shaped workload across a split cascade: optimistic read-modify-write transactions keep committing on a
/// ranged key space while the space is split several times. Every acknowledged commit must stay visible after
/// the cascade — through a point read on every node and through the ordered range scan — and each key's final
/// counter must equal the number of commits acknowledged on it (a stale-base commit that was acknowledged would
/// leave the counter below the count).
/// </summary>
public sealed class TestSplitCascadeTransactionalVisibility : BaseCluster
{
    private const string Space = "t:bk";

    private const int KeyCount = 40;

    private const int Workers = 8;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ITestOutputHelper output;

    public TestSplitCascadeTransactionalVisibility(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string KeyOf(int index) => $"{Space}/{index:D4}";

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(int partition, (IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeaderIfHosted(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }

    /// <summary>One optimistic read-modify-write: read the counter, write counter + 1, commit. Returns the
    /// committed value, or null when the attempt did not commit (the caller retries).</summary>
    private static async Task<long?> TryIncrement(KahunaManager session, string key, int worker, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = $"{key}/w{worker}",
                Locking = KeyValueTransactionLocking.Optimistic,
                AsyncRelease = true,
                Timeout = 30_000
            }, ct);

        if (startType != KeyValueResponseType.Set)
            return null;

        try
        {
            (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await session.LocateAndTryGetValue(
                handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            if (readType != KeyValueResponseType.Get || entry?.Value is null)
            {
                await session.LocateAndRollbackTransaction(handle, ct);
                return null;
            }

            long next = long.Parse(Encoding.UTF8.GetString(entry.Value)) + 1;

            (KeyValueResponseType writeType, _, _) = await session.LocateAndTrySetKeyValue(
                handle.TransactionId, key, Encoding.UTF8.GetBytes(next.ToString()), null, -1,
                KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            if (writeType != KeyValueResponseType.Set)
            {
                await session.LocateAndRollbackTransaction(handle, ct);
                return null;
            }

            (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(handle, ct);
            return commitType == KeyValueResponseType.Committed ? next : null;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static async Task<long?> ReadCounter(KahunaManager kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1, timeoutMs: 30_000);

        return type == KeyValueResponseType.Get && entry?.Value is not null ? long.Parse(Encoding.UTF8.GetString(entry.Value)) : null;
    }

    [Fact]
    public async Task OptimisticReadModifyWrite_AcrossASplitCascade_EveryAcknowledgedWriteStaysVisible()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes = [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];
        KahunaManager[] managers = [.. nodes.Select(n => n.Item2)];

        try
        {
            foreach (KahunaManager kahuna in managers)
                kahuna.RegisterKeyRange(Space);

            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);
            Assert.True(await metaLeader.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = RangeMapStore.FirstDataPartitionId, Generation = 1 }], ct));

            foreach (KahunaManager kahuna in managers)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, KeyOf(0))?.Generation == 1);

            for (int i = 0; i < KeyCount; i++)
            {
                KeyValueTransactionResult seeded = await RetryOnMustRetry(
                    k1, Encoding.UTF8.GetBytes($"BEGIN SET `{KeyOf(i)}` '0' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, seeded.Type);
            }

            long[] ackedCount = new long[KeyCount];
            long[] ackedValue = new long[KeyCount];
            object[] keyLocks = [.. Enumerable.Range(0, KeyCount).Select(_ => new object())];

            using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            long attempts = 0, commits = 0;
            bool paused = false;

            Task[] workers = new Task[Workers];
            for (int w = 0; w < Workers; w++)
            {
                int worker = w;
                workers[w] = Task.Run(async () =>
                {
                    Random random = new(worker * 7919 + 17);
                    KahunaManager session = managers[worker % managers.Length];

                    while (!stop.IsCancellationRequested)
                    {
                        while (Volatile.Read(ref paused) && !stop.IsCancellationRequested)
                            await Task.Delay(5, ct);

                        int index = random.Next(KeyCount);
                        Interlocked.Increment(ref attempts);

                        long? committed = await TryIncrement(session, KeyOf(index), worker, ct);
                        if (committed is null)
                        {
                            await Task.Delay(random.Next(1, 8), ct);
                            continue;
                        }

                        Interlocked.Increment(ref commits);
                        lock (keyLocks[index])
                        {
                            ackedCount[index]++;
                            if (committed.Value > ackedValue[index])
                                ackedValue[index] = committed.Value;
                        }
                    }
                }, ct);
            }

            // Let the workload warm up, then cascade: each split moves the upper half of a range to a new partition.
            await Task.Delay(1_500, ct);

            string[] splitPoints = [KeyOf(20), KeyOf(10), KeyOf(30), KeyOf(5), KeyOf(25)];
            List<string> splitLog = [];

            foreach (string splitKey in splitPoints)
            {
                SplitOutcome outcome = await SplitWithRetries(nodes, Space, splitKey, p => Volatile.Write(ref paused, p), ct);

                splitLog.Add($"{splitKey}: {outcome.Status}");
                Assert.True(outcome.IsSuccess, $"split at {splitKey} failed: {outcome.Status}");

                await Task.Delay(1_000, ct);
            }

            // Keep writing after the cascade, then stop the workers.
            await Task.Delay(3_000, ct);
            stop.Cancel();

            try
            {
                await Task.WhenAll(workers);
            }
            catch (OperationCanceledException)
            {
                // the workers observe the stop token through their delays
            }

            output.WriteLine($"attempts {attempts}, commits {commits}, splits: {string.Join("; ", splitLog)}");
            output.WriteLine($"late vetoes {DurableTransactionMetrics.StaleBaseVetoesLateCount}, fence refusals {DurableTransactionMetrics.ReplicaFenceRefusalsCount}, unattested {DurableTransactionMetrics.ReplicaFenceUnattestedCount}");

            Assert.True(commits > 0, "the workload committed nothing");

            // Every node resolves each key's counter to its last acknowledged value, and that value equals the
            // number of acknowledged commits — a counter below the count is an acknowledged stale-base commit.
            List<string> failures = [];

            for (int i = 0; i < KeyCount; i++)
            {
                string key = KeyOf(i);
                int partition = managers[0].RangeMapStore.Current.Find(Space, key)?.PartitionId ?? -1;

                if (ackedValue[i] != ackedCount[i])
                    failures.Add($"{key}@P{partition}: acked value {ackedValue[i]} but {ackedCount[i]} acked commits (a stale-base commit was acknowledged)");

                for (int n = 0; n < managers.Length; n++)
                {
                    long? visible = await ReadCounter(managers[n], key, ct);
                    if (visible != ackedValue[i])
                        failures.Add($"{key}@P{partition} on node {n}: visible {visible?.ToString() ?? "null"} but acked {ackedValue[i]} ({ackedCount[i]} commits)");
                }
            }

            // The ordered range scan — the final ledger scan's path — must show the same.
            KeyValueGetByRangeResult scan = await RetryOnMustRetryAsync(
                () => managers[0].LocateAndGetByRange(HLCTimestamp.Zero, Space, null, true, null, true, KeyCount * 4, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Type, timeoutMs: 30_000);

            Dictionary<string, long> scanned = [];
            foreach ((string key, ReadOnlyKeyValueEntry entry) in scan.Items)
            {
                if (entry.Value is not null)
                    scanned[key] = long.Parse(Encoding.UTF8.GetString(entry.Value));
            }

            for (int i = 0; i < KeyCount; i++)
            {
                string key = KeyOf(i);
                if (!scanned.TryGetValue(key, out long value))
                    failures.Add($"{key}: missing from the range scan ({scan.Type}, {scan.Items.Count} items)");
                else if (value != ackedValue[i])
                    failures.Add($"{key}: range scan shows {value} but acked {ackedValue[i]} ({ackedCount[i]} commits)");
            }

            foreach (string failure in failures)
                output.WriteLine(failure);

            Assert.True(failures.Count == 0, $"{failures.Count} visibility failures after the cascade:\n{string.Join("\n", failures)}");
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── the bank transfer shape: statement-snapshot reads, two rows, index entries in a second space ──

    private const string Rows = "t:bk2";

    private const string Index = "t:bki";

    private const int RowCount = 200;

    private const int BankWorkers = 4;

    private const long InitialBalance = 1_000;

    private static string RowKey(int index) => $"{Rows}/{index:D4}";

    private static string IndexKey(long balance, int index) => $"{Index}/{balance:D6}/{index:D4}";

    private static byte[] EncodeRow(long balance, long version) => Encoding.UTF8.GetBytes($"{balance}|{version}");

    private static (long Balance, long Version) DecodeRow(byte[] value)
    {
        string[] parts = Encoding.UTF8.GetString(value).Split('|');
        return (long.Parse(parts[0]), long.Parse(parts[1]));
    }

    /// <summary>
    /// One transfer of one unit between two rows, the way CamusDB's optimistic read-committed UPDATE runs it:
    /// both rows are read under one statement snapshot (a local HLC timestamp, statement-snapshot mode,
    /// registered reads), each row is rewritten with its balance and version advanced, each row's index entry
    /// is deleted at the old balance and set at the new one, then the session commits. Returns the versions
    /// written, or null when the attempt did not commit.
    /// </summary>
    private static async Task<(long VersionA, long VersionB)?> TryTransfer(
        IRaft raft, KahunaManager session, int a, int b, int worker, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = $"{RowKey(a)}/w{worker}",
                Locking = KeyValueTransactionLocking.Optimistic,
                AsyncRelease = true,
                Timeout = 30_000
            }, ct);

        if (startType != KeyValueResponseType.Set)
            return null;

        try
        {
            HLCTimestamp statement = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            (long balanceA, long versionA)? rowA = await ReadRowUnderStatement(session, handle, RowKey(a), statement, ct);
            (long balanceA2, long versionA2)? rowB = await ReadRowUnderStatement(session, handle, RowKey(b), statement, ct);

            if (rowA is null || rowB is null)
            {
                await session.LocateAndRollbackTransaction(handle, ct);
                return null;
            }

            (long balanceFromA, long versionFromA) = rowA.Value;
            (long balanceFromB, long versionFromB) = rowB.Value;

            if (!await WriteRowAndIndex(session, handle, a, balanceFromA, balanceFromA - 1, versionFromA + 1, ct)
                || !await WriteRowAndIndex(session, handle, b, balanceFromB, balanceFromB + 1, versionFromB + 1, ct))
            {
                await session.LocateAndRollbackTransaction(handle, ct);
                return null;
            }

            (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(handle, ct);
            return commitType == KeyValueResponseType.Committed ? (versionFromA + 1, versionFromB + 1) : null;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static async Task<(long Balance, long Version)?> ReadRowUnderStatement(
        KahunaManager session, TransactionHandle handle, string key, HLCTimestamp statement, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await session.LocateAndTryGetValue(
            handle.TransactionId, key, -1, statement, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom(),
            readMode: KeyValueReadMode.StatementSnapshot);

        return type == KeyValueResponseType.Get && entry?.Value is not null ? DecodeRow(entry.Value) : null;
    }

    private static async Task<bool> WriteRowAndIndex(
        KahunaManager session, TransactionHandle handle, int row, long oldBalance, long newBalance, long version, CancellationToken ct)
    {
        (KeyValueResponseType rowType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, RowKey(row), EncodeRow(newBalance, version), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        if (rowType != KeyValueResponseType.Set)
            return false;

        (KeyValueResponseType deleteType, _, _) = await session.LocateAndTryDeleteKeyValue(
            handle.TransactionId, IndexKey(oldBalance, row), KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        if (deleteType is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            return false;

        (KeyValueResponseType setType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, IndexKey(newBalance, row), Encoding.UTF8.GetBytes(version.ToString()), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        return setType == KeyValueResponseType.Set;
    }

    private static async Task<(long Balance, long Version)?> ReadRow(KahunaManager kahuna, string key, KeyValueReadMode mode, HLCTimestamp at, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, at, KeyValueDurability.Persistent, ct, readMode: mode),
            r => r.Item1, timeoutMs: 30_000);

        return type == KeyValueResponseType.Get && entry?.Value is not null ? DecodeRow(entry.Value) : null;
    }

    /// <summary>
    /// Drives one split with the workers paused until the split reaches its quiesce window, then resumes them
    /// inside the window: the pre-quiesce bulk copy reads the range outside any lock and cannot page past a
    /// live intent, so under this much contention it never completes, while the rig's far larger ranges let
    /// it through. The writes still race the hand-off, the cutover and everything after it.
    /// </summary>
    private static async Task<SplitOutcome> SplitWithRetries(
        (IRaft, KahunaManager)[] nodes, string space, string splitKey, Action<bool> pause, CancellationToken ct)
    {
        SplitOutcome outcome = default;

        for (int attempt = 0; attempt < 40; attempt++)
        {
            pause(true);
            await Task.Delay(300, ct);

            (IRaft _, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);
            outcome = await driver.ForceSplitAtKeyAsync(space, splitKey, () => { pause(false); return Task.CompletedTask; }, ct);
            pause(false);

            if (outcome.IsSuccess)
                return outcome;

            await Task.Delay(150, ct);
        }

        return outcome;
    }

    [Fact]
    public async Task BankTransfers_WithStatementSnapshotReadsAndIndexSpaceSplits_EveryAcknowledgedWriteStaysVisible()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes = [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];
        KahunaManager[] managers = [.. nodes.Select(n => n.Item2)];

        try
        {
            foreach (KahunaManager kahuna in managers)
            {
                kahuna.RegisterKeyRange(Rows);
                kahuna.RegisterKeyRange(Index);
            }

            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);
            Assert.True(await metaLeader.RangeMapStore.MutateAsync(
                _ =>
                [
                    new RangeDescriptor { KeySpace = Rows, StartKey = null, EndKey = null, PartitionId = RangeMapStore.FirstDataPartitionId, Generation = 1 },
                    new RangeDescriptor { KeySpace = Index, StartKey = null, EndKey = null, PartitionId = RangeMapStore.FirstDataPartitionId + 1, Generation = 1 }
                ], ct));

            foreach (KahunaManager kahuna in managers)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Rows, RowKey(0))?.Generation == 1
                                           && kahuna.RangeMapStore.Current.Find(Index, IndexKey(InitialBalance, 0))?.Generation == 1);

            for (int i = 0; i < RowCount; i++)
            {
                KeyValueTransactionResult seeded = await RetryOnMustRetry(
                    k1, Encoding.UTF8.GetBytes($"BEGIN SET `{RowKey(i)}` '{InitialBalance}|0' SET `{IndexKey(InitialBalance, i)}` '0' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, seeded.Type);
            }

            long[] ackedCount = new long[RowCount];
            long[] ackedVersion = new long[RowCount];
            object[] rowLocks = [.. Enumerable.Range(0, RowCount).Select(_ => new object())];

            using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            long attempts = 0, commits = 0;
            bool paused = false;

            Task[] workers = new Task[BankWorkers];
            for (int w = 0; w < BankWorkers; w++)
            {
                int worker = w;
                workers[w] = Task.Run(async () =>
                {
                    Random random = new(worker * 104_729 + 3);
                    (IRaft raft, KahunaManager session) = nodes[worker % nodes.Length];

                    while (!stop.IsCancellationRequested)
                    {
                        while (Volatile.Read(ref paused) && !stop.IsCancellationRequested)
                            await Task.Delay(5, ct);

                        int a = random.Next(RowCount);
                        int b = random.Next(RowCount - 1);
                        if (b >= a)
                            b++;

                        Interlocked.Increment(ref attempts);

                        (long VersionA, long VersionB)? committed = await TryTransfer(raft, session, a, b, worker, ct);
                        if (committed is null)
                        {
                            await Task.Delay(random.Next(1, 8), ct);
                            continue;
                        }

                        Interlocked.Increment(ref commits);
                        lock (rowLocks[a])
                        {
                            ackedCount[a]++;
                            if (committed.Value.VersionA > ackedVersion[a])
                                ackedVersion[a] = committed.Value.VersionA;
                        }
                        lock (rowLocks[b])
                        {
                            ackedCount[b]++;
                            if (committed.Value.VersionB > ackedVersion[b])
                                ackedVersion[b] = committed.Value.VersionB;
                        }
                    }
                }, ct);
            }

            await Task.Delay(1_500, ct);

            // Alternate the cascade between the row space and the index space, as the rig's cascade did.
            (string Space, string Key)[] splitPoints =
            [
                (Rows, RowKey(100)),
                (Index, $"{Index}/{InitialBalance:D6}"),
                (Rows, RowKey(50)),
                (Index, $"{Index}/{InitialBalance + 1:D6}")
            ];

            List<string> splitLog = [];
            foreach ((string space, string splitKey) in splitPoints)
            {
                SplitOutcome outcome = await SplitWithRetries(nodes, space, splitKey, p => Volatile.Write(ref paused, p), ct);
                splitLog.Add($"{splitKey}: {outcome.Status}");
                Assert.True(outcome.IsSuccess, $"split at {splitKey} failed: {outcome.Status}");
                await Task.Delay(1_000, ct);
            }

            await Task.Delay(3_000, ct);
            stop.Cancel();

            try
            {
                await Task.WhenAll(workers);
            }
            catch (OperationCanceledException)
            {
                // the workers observe the stop token through their delays
            }

            output.WriteLine($"attempts {attempts}, commits {commits}, splits: {string.Join("; ", splitLog)}");
            output.WriteLine($"late vetoes {DurableTransactionMetrics.StaleBaseVetoesLateCount}, fence refusals {DurableTransactionMetrics.ReplicaFenceRefusalsCount}, unattested {DurableTransactionMetrics.ReplicaFenceUnattestedCount}");

            Assert.True(commits > 0, "the workload committed nothing");

            List<string> failures = [];
            long totalBalance = 0;

            for (int i = 0; i < RowCount; i++)
            {
                string key = RowKey(i);
                int partition = managers[0].RangeMapStore.Current.Find(Rows, key)?.PartitionId ?? -1;

                if (ackedVersion[i] != ackedCount[i])
                    failures.Add($"{key}@P{partition}: acked version {ackedVersion[i]} but {ackedCount[i]} acked commits (a stale-base commit was acknowledged)");

                for (int n = 0; n < managers.Length; n++)
                {
                    HLCTimestamp now = nodes[n].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[n].Item1.GetLocalNodeId());

                    (long Balance, long Version)? latest = await ReadRow(managers[n], key, KeyValueReadMode.Default, HLCTimestamp.Zero, ct);
                    if (latest?.Version != ackedVersion[i])
                        failures.Add($"{key}@P{partition} on node {n} (latest read): version {latest?.Version.ToString() ?? "null"} but acked {ackedVersion[i]} ({ackedCount[i]} commits)");

                    (long Balance, long Version)? snapshot = await ReadRow(managers[n], key, KeyValueReadMode.StatementSnapshot, now, ct);
                    if (snapshot?.Version != ackedVersion[i])
                        failures.Add($"{key}@P{partition} on node {n} (snapshot read at now): version {snapshot?.Version.ToString() ?? "null"} but acked {ackedVersion[i]}");

                    if (n == 0 && latest is not null)
                        totalBalance += latest.Value.Balance;
                }
            }

            if (totalBalance != RowCount * InitialBalance)
                failures.Add($"balance not conserved: {totalBalance} instead of {RowCount * InitialBalance}");

            // The ordered range scans of both spaces: every row at its acked version, and exactly one index entry
            // per row, at the row's current balance.
            KeyValueGetByRangeResult rowScan = await RetryOnMustRetryAsync(
                () => managers[0].LocateAndGetByRange(HLCTimestamp.Zero, Rows, null, true, null, true, RowCount * 4, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Type, timeoutMs: 30_000);

            Dictionary<string, (long Balance, long Version)> scannedRows = [];
            foreach ((string key, ReadOnlyKeyValueEntry entry) in rowScan.Items)
            {
                if (entry.Value is not null && key.StartsWith(Rows + "/", StringComparison.Ordinal))
                    scannedRows[key] = DecodeRow(entry.Value);
            }

            for (int i = 0; i < RowCount; i++)
            {
                if (!scannedRows.TryGetValue(RowKey(i), out (long Balance, long Version) row))
                    failures.Add($"{RowKey(i)}: missing from the row scan ({rowScan.Type}, {rowScan.Items.Count} items)");
                else if (row.Version != ackedVersion[i])
                    failures.Add($"{RowKey(i)}: row scan shows version {row.Version} but acked {ackedVersion[i]} ({ackedCount[i]} commits)");
            }

            KeyValueGetByRangeResult indexScan = await RetryOnMustRetryAsync(
                () => managers[0].LocateAndGetByRange(HLCTimestamp.Zero, Index, null, true, null, true, RowCount * 8, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Type, timeoutMs: 30_000);

            Dictionary<int, List<string>> indexByRow = [];
            foreach ((string key, ReadOnlyKeyValueEntry entry) in indexScan.Items)
            {
                if (entry.Value is null || !key.StartsWith(Index + "/", StringComparison.Ordinal))
                    continue;

                int row = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
                (indexByRow.TryGetValue(row, out List<string>? entries) ? entries : indexByRow[row] = []).Add(key);
            }

            for (int i = 0; i < RowCount; i++)
            {
                indexByRow.TryGetValue(i, out List<string>? entries);
                string expected = scannedRows.TryGetValue(RowKey(i), out (long Balance, long Version) row) ? IndexKey(row.Balance, i) : "?";

                if (entries is null || entries.Count != 1 || entries[0] != expected)
                    failures.Add($"{RowKey(i)}: index entries [{string.Join(", ", entries ?? [])}] but expected [{expected}]");
            }

            foreach (string failure in failures)
                output.WriteLine(failure);

            Assert.True(failures.Count == 0, $"{failures.Count} visibility failures after the cascade:\n{string.Join("\n", failures)}");
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
