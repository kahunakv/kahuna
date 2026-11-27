using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A coordinator on a replica that is behind the leader of a written key's partition. The replica applied the
/// prepare of a committed transaction on the key and not its settlement, so its intent store still holds that
/// transaction's intent. The leader settled it and committed a newer revision, which a read-modify-write
/// coordinated on the replica validated. The staged-base check of that commit runs on the replica and finds the
/// older committed intent there. The commit must go through, on the two-phase path and through the one-phase
/// bundle that the replica forwards to the leader.
///
/// <para>The replica's lag is made with the dispatcher's apply skip: after the intent applied there, the replica
/// drops every later entry of the key's partition.</para>
/// </summary>
public sealed class TestStagedBaseCommittedHolderCluster : BaseCluster
{
    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestStagedBaseCommittedHolderCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <param name="applyTimeValidation">With apply-time validation the read-modify-write takes the one-phase
    /// bundle, forwarded to the leader; without it a multi-process group sends it on the two-phase path.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CoordinatorOnALaggingReplica_CommitsOverTheNewerBase(bool applyTimeValidation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, raftLogger, kahunaLogger,
                configure: config => config.OnePhaseApplyTimeValidation = applyTimeValidation);

        IRaft[] rafts = [raft1, raft2, raft3];
        KahunaManager[] managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

        try
        {
            await RunUnderStableLeadership(raft1, Partitions, async () =>
            {
                // The lagging replica must lead some other partition, so that a session can live on it.
                (string key, int partition, int leader, int lagging) = await KeyWithAnotherLeader(rafts, managers[0], ct);

                await CommitBlindWrite(managers[leader], key, "100", ct);
                foreach (KahunaManager manager in managers)
                    await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Get(key) is null);

                // The holder commits on the leader. Its settlement is held, so every replica applies its intent and
                // keeps it.
                ResolutionGate[] gates = [.. managers.Select(m => new ResolutionGate(m))];
                TransactionHandle holder;
                try
                {
                    string holderCoordinator = await KeyLedBy(rafts, managers[0], "sbhc-holder", leader, ct);
                    (holder, _) = await StartAndRead(managers[leader], holderCoordinator, key, ct);
                    await Write(managers[leader], holder, key, "101", ct);
                    Assert.Equal(KeyValueResponseType.Committed, (await managers[leader].LocateAndCommitTransaction(holder, ct)).Type);

                    await WaitUntilAsync(() => managers[lagging].DurablePreparedIntentStore.Get(key)?.TransactionId == holder.TransactionId);

                    // From here on the lagging replica applies nothing more of the key's partition.
                    managers[lagging].KeyValues.ReplicationDispatcher.ApplySkipForTesting = (partitionId, _) => partitionId == partition;
                }
                finally
                {
                    foreach (ResolutionGate gate in gates)
                        gate.Release();
                }

                try
                {
                    await WaitUntilAsync(() => managers[leader].DurablePreparedIntentStore.Get(key) is null);

                    // The leader commits a newer revision over the settled holder.
                    await CommitBlindWrite(managers[leader], key, "102", ct);
                    await WaitUntilAsync(() => managers[leader].DurablePreparedIntentStore.Get(key) is null);

                    PreparedIntent? stale = managers[lagging].DurablePreparedIntentStore.Get(key);
                    Assert.NotNull(stale);
                    Assert.Equal(holder.TransactionId, stale.TransactionId);

                    // A read-modify-write coordinated on the lagging replica reads the newer revision from the leader.
                    string coordinator = await KeyLedBy(rafts, managers[0], "sbhc-coord", lagging, ct);
                    (TransactionHandle writer, long baseRevision) = await StartAndRead(managers[lagging], coordinator, key, ct);
                    Assert.True(baseRevision > stale.Revision, $"the base revision {baseRevision} must be above the stale intent revision {stale.Revision}");

                    await Write(managers[lagging], writer, key, "103", ct);

                    TransactionCommitAnswer answer;
                    using (CheckPasses passes = new(managers[lagging]))
                    {
                        answer = await managers[lagging].LocateAndCommitTransaction(writer, ct);
                        Assert.True(passes.Count > 0, "the staged-base check on the lagging replica must pass over its older committed intent");
                    }

                    TestContext.Current.TestOutputHelper?.WriteLine($"commit: {answer.Type} ({answer.Refusal}: {answer.Reason})");

                    Assert.Equal(KeyValueResponseType.Committed, answer.Type);
                    Assert.Equal(CommitRefusalClass.None, answer.Refusal);

                    await WaitUntilAsync(() => managers[leader].DurablePreparedIntentStore.Get(key) is null);
                    Assert.Equal("103", await ReadValue(managers[leader], key, ct));
                }
                finally
                {
                    managers[lagging].KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;
                }
            });
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    private static async Task<int> LeaderIndexOf(IRaft[] rafts, int partition, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return i;

            await Task.Delay(50, ct);
        }
    }

    /// <summary>A fresh data key whose partition's leader is not the only leader: another node leads at least one
    /// other partition. Returns the key, its partition, its leader and that other node.</summary>
    private static async Task<(string Key, int Partition, int Leader, int Other)> KeyWithAnotherLeader(
        IRaft[] rafts, KahunaManager manager, CancellationToken ct)
    {
        int[] leaders = new int[Partitions + 1];
        for (int p = 1; p <= Partitions; p++)
            leaders[p] = await LeaderIndexOf(rafts, p, ct);

        string random = Guid.NewGuid().ToString("N")[..8];
        for (int i = 0; i < 4_096; i++)
        {
            string candidate = $"sbhc{i}/{random}";
            int partition = manager.KeyValues.LocateDurablePartition(candidate).PartitionId;
            int leader = leaders[partition];

            for (int p = 1; p <= Partitions; p++)
            {
                if (leaders[p] != leader)
                    return (candidate, partition, leader, leaders[p]);
            }
        }

        throw new InvalidOperationException("every partition has the same leader");
    }

    /// <summary>A coordinator key routed to a partition whose leader is <paramref name="nodeIndex"/>, so the
    /// transaction's session, and its finalize, run on that node.</summary>
    private static async Task<string> KeyLedBy(IRaft[] rafts, KahunaManager manager, string prefix, int nodeIndex, CancellationToken ct)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        for (int i = 0; i < 4_096; i++)
        {
            string candidate = $"{prefix}{i}/{random}";
            if (await LeaderIndexOf(rafts, manager.KeyValues.LocateDurablePartition(candidate).PartitionId, ct) == nodeIndex)
                return candidate;
        }

        throw new InvalidOperationException($"no key space under {prefix} routes to a partition led by node {nodeIndex}");
    }

    private static async Task CommitBlindWrite(KahunaManager manager, string key, string value, CancellationToken ct)
    {
        KeyValueTransactionResult result = await RetryOnMustRetry(
            manager, Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '{value}' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);
    }

    private static async Task<(TransactionHandle Handle, long BaseRevision)> StartAndRead(
        KahunaManager coordinator, string coordinatorKey, string readKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await coordinator.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await coordinator.LocateAndTryGetValue(
            handle.TransactionId, readKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, readType);

        return (handle, entry?.Revision ?? -1);
    }

    private static async Task Write(KahunaManager coordinator, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await coordinator.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    private static async Task<string> ReadValue(KahunaManager manager, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await manager.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        return Encoding.UTF8.GetString(entry!.Value!);
    }

    /// <summary>Counts the finalizes whose staged-base check before the first proposal passed on a node, while
    /// installed. The finalizer calls the hook only after that check answers valid.</summary>
    private sealed class CheckPasses : IDisposable
    {
        private readonly DurableTransactionFinalizer finalizer;

        private int count;

        public CheckPasses(KahunaManager manager)
        {
            finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestAfterPreValidationHook = _ =>
            {
                Interlocked.Increment(ref count);
                return Task.CompletedTask;
            };
        }

        public int Count => Volatile.Read(ref count);

        public void Dispose() => finalizer.TestAfterPreValidationHook = null;
    }

    /// <summary>Holds every deferred resolution that a node's finalizer schedules while installed, so a commit's
    /// decision→settlement window stays open until <see cref="Release"/>.</summary>
    private sealed class ResolutionGate
    {
        private readonly DurableTransactionFinalizer finalizer;

        private readonly TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ResolutionGate(KahunaManager manager)
        {
            finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestBeforeDeferredResolutionHook = ct => open.Task.WaitAsync(ct);
        }

        public void Release()
        {
            finalizer.TestBeforeDeferredResolutionHook = null;
            open.TrySetResult();
        }
    }
}
