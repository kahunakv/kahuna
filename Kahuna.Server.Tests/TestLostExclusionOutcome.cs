using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The outcome a transaction gets when a partition leader change, with no competing transaction at all, drops a
/// lock or a staging it ran under. The commit is refused either way (see <see cref="TestLeaderChangeLostRangeLock"/>
/// for the anomalies the refusal prevents); these tests pin what the refusal is called.
///
/// <list type="bullet">
/// <item>A <b>script</b> is self-contained: a new run takes new locks under the new leader and reads again, so the
/// refusal answers <see cref="KeyValueResponseType.MustRetry"/>, nothing of the refused run is durable, and the
/// re-run commits. A client that retries only <c>MustRetry</c> (the SDK, the test helpers, CamusDB) then survives
/// leader churn without application involvement.</item>
/// <item>An <b>interactive session</b> cannot repeat the reads it made under the lost lock, so the same refusal
/// stays a terminal <see cref="KeyValueResponseType.Aborted"/>: the caller starts a new transaction, which commits.</item>
/// </list>
/// </summary>
public sealed class TestLostExclusionOutcome : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int LockExpiresMs = 60_000;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLostExclusionOutcome(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

    private static async Task<int> LeaderIndexOf(int partition, IRaft[] rafts, CancellationToken ct)
    {
        int index = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (!await rafts[i].AmILeaderIfHosted(partition, ct))
                    continue;

                index = i;
                return true;
            }

            return false;
        }, timeoutMs: 30_000);

        return index;
    }

    private static Dictionary<int, string> FreshKeyPerPartition(KahunaManager probe, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        Dictionary<int, string> byPartition = [];

        for (int i = 0; i < 4_096 && byPartition.Count < Partitions; i++)
        {
            string candidate = $"{tag}{i}/{random}";
            int partition = probe.KeyValues.LocateDurablePartition(candidate).PartitionId;
            byPartition.TryAdd(partition, candidate);
        }

        return byPartition;
    }

    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
    }

    private static async Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> Read(
        KahunaManager reader, string key, CancellationToken ct) =>
        await RetryOnMustRetryAsync(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);

    private static async Task SettleEverywhere(KahunaManager[] managers, string key, CancellationToken ct)
    {
        await WaitUntilAsync(async () =>
        {
            foreach (KahunaManager manager in managers)
                await manager.KeyValues.RecoverPreparedIntents(ct);

            foreach (KahunaManager manager in managers)
                if (manager.DurablePreparedIntentStore.Get(key) is not null)
                    return false;

            return true;
        }, timeoutMs: 60_000);
    }

    /// <summary>Settles the key everywhere, then asserts every node reads the same committed state of it: absent
    /// when <paramref name="value"/> is null, otherwise the value at the revision.</summary>
    private static async Task AssertReadsEverywhere(KahunaManager[] managers, string key, string? value, long revision, CancellationToken ct)
    {
        await SettleEverywhere(managers, key, ct);

        foreach (KahunaManager reader in managers)
        {
            (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await Read(reader, key, ct);

            if (value is null)
            {
                Assert.Equal(KeyValueResponseType.DoesNotExist, readType);
                continue;
            }

            Assert.Equal(KeyValueResponseType.Get, readType);
            Assert.Equal(value, Encoding.UTF8.GetString(entry!.Value!));
            Assert.Equal(revision, entry.Revision);
        }
    }

    private static async Task<TransactionHandle> StartPessimistic(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static async Task<KeyValueResponseType> TryExclusivePointLock(
        KahunaManager session, TransactionHandle handle, string key, CancellationToken ct)
    {
        List<(KeyValueResponseType Type, string Key, KeyValueDurability Durability, HLCTimestamp Holder)> locks =
            await session.LocateAndTryAcquireManyExclusiveLocks(
                handle.TransactionId, [(key, LockExpiresMs, KeyValueDurability.Persistent)], ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        return Assert.Single(locks).Type;
    }

    private static async Task<(KeyValueResponseType Type, long Revision)> TryWrite(
        KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, long revision, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        return (writeType, revision);
    }

    /// <summary>
    /// The script holds its exclusive point lock when the key's partition changes leader; the lock proof at commit
    /// refuses the commit. The script answers <c>MustRetry</c> with the refusal's reason, leaves nothing durable, and
    /// commits when it is run again.
    /// </summary>
    /// <param name="readStagedKey">The script reads the key it wrote before it commits. The leader change happens
    /// in the background while the script runs, so the read can come back from the new leader without the staging
    /// — the lost-staging refusal — or the lock proof refuses first; either refusal must answer <c>MustRetry</c>.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScriptWhoseLockWasLost_AnswersMustRetry_AndARerunCommits(bool readStagedKey)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lxs");
            KeyValuePair<int, string> data = keys.OrderBy(kv => kv.Key).First();
            (int dataPartition, string dataKey) = (data.Key, data.Value);

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            string script = readStagedKey
                ? $"""
                  BEGIN (locking=pessimistic, timeout=30000)
                   SET '{dataKey}' 'first'
                   SLEEP 1500
                   GET '{dataKey}'
                   COMMIT
                  END
                  """
                : $"""
                  BEGIN (locking=pessimistic, timeout=30000)
                   SET '{dataKey}' 'first'
                   COMMIT
                  END
                  """;

            // One-shot: once the script holds its lock, the key's partition changes leader. The read variant
            // moves it while the script sleeps, after its write was staged on the leader about to be deposed.
            int hookRuns = 0;
            Task leaderMove = Task.CompletedTask;
            session.ScriptExecutor.TestAfterLocksAcquiredHook = async hookCt =>
            {
                session.ScriptExecutor.TestAfterLocksAcquiredHook = null;
                Interlocked.Increment(ref hookRuns);

                if (readStagedKey)
                {
                    leaderMove = Task.Run(async () =>
                    {
                        await Task.Delay(300, ct);
                        await MoveLeadership(rafts, dataPartition, leader, successor, ct);
                    }, ct);
                    return;
                }

                await MoveLeadership(rafts, dataPartition, leader, successor, hookCt);
            };

            long lostLockAborts = DurableTransactionMetrics.LostLockAbortsCount;

            KeyValueTransactionResult refused;
            try
            {
                refused = await session.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            }
            finally
            {
                session.ScriptExecutor.TestAfterLocksAcquiredHook = null;
            }

            await leaderMove;

            Assert.Equal(1, hookRuns);
            Assert.Equal(KeyValueResponseType.MustRetry, refused.Type);
            Assert.StartsWith("Lost ", refused.Reason, StringComparison.Ordinal);

            if (!readStagedKey)
                Assert.True(DurableTransactionMetrics.LostLockAbortsCount > lostLockAborts,
                    "the commit must be refused because the lock was lost, and counted as such");

            // Nothing of the refused run is durable: the answer promised a clean re-run.
            await AssertReadsEverywhere(managers, dataKey, null, 0, ct);

            KeyValueTransactionResult committed = await RetryOnMustRetry(session, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(readStagedKey ? KeyValueResponseType.Get : KeyValueResponseType.Set, committed.Type);

            await AssertReadsEverywhere(managers, dataKey, "first", 0, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The one-phase bundle validates before it proposes; the leader changes in between, and the new leader
    /// refuses the bundle before anything is appended, because it is fenced to the term the validation ran
    /// under. That refusal reaches the script as the same <c>MustRetry</c>, and the re-run commits.
    /// </summary>
    [Fact]
    public async Task ScriptWhoseBundleWasProposedAfterTheLeaderChanged_AnswersMustRetry_AndARerunCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = true);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];
        DurableTransactionFinalizer[] finalizers = [.. managers.Select(static m => m.TransactionCoordinator.DurableFinalizerForTests)];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lxb");
            KeyValuePair<int, string> data = keys.OrderBy(kv => kv.Key).First();
            (int dataPartition, string dataKey) = (data.Key, data.Value);

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            // A transaction block: a bare single statement runs as a plain set, outside any transaction.
            string script = $"""
                BEGIN (locking=pessimistic, timeout=30000)
                 SET '{dataKey}' 'first'
                 COMMIT
                END
                """;

            // One-shot: whichever node finalizes the script moves the leadership inside its window between the
            // validation and the propose, then clears the hook everywhere so nothing replays it.
            int hookRuns = 0;
            foreach (DurableTransactionFinalizer finalizer in finalizers)
            {
                finalizer.TestAfterReadSetValidationHook = async hookCt =>
                {
                    foreach (DurableTransactionFinalizer other in finalizers)
                        other.TestAfterReadSetValidationHook = null;

                    Interlocked.Increment(ref hookRuns);
                    await MoveLeadership(rafts, dataPartition, leader, successor, hookCt);
                };
            }

            long fenceRefusals = DurableTransactionMetrics.OnePhaseBundleTermFenceRefusalsCount;

            KeyValueTransactionResult refused;
            try
            {
                refused = await session.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            }
            finally
            {
                foreach (DurableTransactionFinalizer finalizer in finalizers)
                    finalizer.TestAfterReadSetValidationHook = null;
            }

            Assert.Equal(1, hookRuns);
            Assert.Equal(KeyValueResponseType.MustRetry, refused.Type);
            Assert.True(DurableTransactionMetrics.OnePhaseBundleTermFenceRefusalsCount > fenceRefusals,
                "the bundle must be refused before the append because the partition is led in another term than the one its validation ran under, and counted as such");

            await AssertReadsEverywhere(managers, dataKey, null, 0, ct);

            KeyValueTransactionResult committed = await RetryOnMustRetry(session, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, committed.Type);

            await AssertReadsEverywhere(managers, dataKey, "first", 0, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The interactive session holds its exclusive point lock and a staged write when the key's partition changes
    /// leader. Its commit is refused as a terminal <c>Aborted</c> — not <c>MustRetry</c>, which would tell the
    /// client to repeat a finalize that can only fail again — and a fresh session on the same key commits.
    /// </summary>
    [Fact]
    public async Task InteractiveSessionWhoseLockWasLost_AnswersAborted_AndAFreshSessionCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lxi");
            Assert.True(keys.Count >= 2, "the fixture needs a data partition and a coordinator partition");

            KeyValuePair<int, string>[] ordered = [.. keys.OrderBy(kv => kv.Key)];
            (int dataPartition, string dataKey) = (ordered[0].Key, ordered[0].Value);
            string coordinatorKey = ordered[1].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            (KeyValueResponseType seedType, long seedRevision, _) = await session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, dataKey, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            TransactionHandle stale = await StartPessimistic(session, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryExclusivePointLock(session, stale, dataKey, ct));
            Assert.Equal(KeyValueResponseType.Set, (await TryWrite(session, stale, dataKey, "stale", ct)).Type);

            await MoveLeadership(rafts, dataPartition, leader, successor, ct);

            long lostLockAborts = DurableTransactionMetrics.LostLockAbortsCount;

            (KeyValueResponseType staleCommit, _) = await session.LocateAndCommitTransaction(stale, ct);
            Assert.Equal(KeyValueResponseType.Aborted, staleCommit);
            Assert.True(DurableTransactionMetrics.LostLockAbortsCount > lostLockAborts,
                "the commit must be refused because the lock was lost, and counted as such");

            await AssertReadsEverywhere(managers, dataKey, "seed", seedRevision, ct);

            // The caller's recourse: a new transaction, which takes its lock from the current leader.
            TransactionHandle fresh = await StartPessimistic(managers[successor], coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryExclusivePointLock(managers[successor], fresh, dataKey, ct));
            Assert.Equal(KeyValueResponseType.Set, (await TryWrite(managers[successor], fresh, dataKey, "fresh", ct)).Type);

            (KeyValueResponseType freshCommit, _) = await managers[successor].LocateAndCommitTransaction(fresh, ct);
            Assert.Equal(KeyValueResponseType.Committed, freshCommit);

            await AssertReadsEverywhere(managers, dataKey, "fresh", seedRevision + 1, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
