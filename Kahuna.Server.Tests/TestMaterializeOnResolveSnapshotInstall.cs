using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A replica seeded by a whole-partition snapshot taken after a transaction's prepare and before its settle. With
/// the materializing settle, that settle is the only entry that installs the committed value on each replica, so
/// the seeded replica must install it from the pending intent the seed carried — the one window in which the
/// intent reaches a replica through state transfer instead of through its own prepare apply.
///
/// <para>The install is driven through the node exactly as a leader's transfer drives it (the leader's own
/// export delivered to the follower's <c>ReceiveInstallSnapshot</c>), with the follower's consumer held so the
/// settle is genuinely in the pending tail above the boundary. The seeded replica's installer is wrapped by a
/// recording spy: the leader's own actor also persists the committed row, so the value can reach the seed
/// through the export, and only the spy shows that the settle's apply on the seeded replica installed it.</para>
/// </summary>
public sealed class TestMaterializeOnResolveSnapshotInstall : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int Partition = 1;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestMaterializeOnResolveSnapshotInstall(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

    /// <summary>Records every install on one replica and forwards it to the replica's real installer.</summary>
    private sealed class SpyInstaller(IResolvedIntentInstaller inner) : IResolvedIntentInstaller
    {
        public ConcurrentQueue<(string Key, bool Replay)> Installs { get; } = new();

        public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay)
        {
            Installs.Enqueue((intent.Key, replay));
            inner.Install(partitionId, logIndex, intent, replay);
        }

        public void CompleteEntry(int partitionId, long logIndex, bool replay) => inner.CompleteEntry(partitionId, logIndex, replay);

        public void NoteUnresolvedOnReplay(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp) =>
            inner.NoteUnresolvedOnReplay(partitionId, logIndex, transactionId, epoch, key, commitTimestamp);
    }

    private static async Task<int> LeaderIndexOf(IRaft[] rafts, CancellationToken ct)
    {
        int index = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (!await rafts[i].AmILeaderIfHosted(Partition, ct))
                    continue;

                index = i;
                return true;
            }

            return false;
        }, timeoutMs: 30_000);

        return index;
    }

    private static string[] KeysOnPartition(KahunaManager probe, string tag, int count)
    {
        List<string> keys = new(count);
        string random = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; keys.Count < count && i < 65_536; i++)
        {
            string candidate = $"{tag}{i}{random}/k";
            if (probe.KeyValues.LocateDurablePartition(candidate).PartitionId == Partition)
                keys.Add(candidate);
        }

        Assert.Equal(count, keys.Count);
        return [.. keys];
    }

    private static byte[] Script(IReadOnlyList<string> keys, string value)
    {
        StringBuilder script = new("BEGIN ");
        foreach (string key in keys)
            script.Append($"SET `{key}` '{value}' ");
        script.Append("COMMIT END");
        return Encoding.UTF8.GetBytes(script.ToString());
    }

    /// <summary>The term the partition's leader proposes in, read from a committed proposal's own reply: a
    /// snapshot boundary stamped with the wrong term would turn the retained tail into a truncation.</summary>
    private static async Task<long> ReadCurrentTerm(IRaft raft, KahunaManager session, string key, CancellationToken ct)
    {
        TaskCompletionSource<long> term = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using IDisposable registration = raft.HoldCommittedProposalRepliesForTesting(Partition, reply =>
        {
            term.TrySetResult(reply.Term);
            reply.Release();
        });

        int round = 0;
        while (!term.Task.IsCompleted)
        {
            await session.TryExecuteTransactionScript(Script([key], $"term-{round++}"), null, null);
            await Task.Delay(20, ct);
        }

        return await term.Task;
    }

    private static async Task WaitForStableCommitIndex(IRaft raft, CancellationToken ct)
    {
        long previous = -1;
        await WaitUntilAsync(async () =>
        {
            long current = raft.GetCommitIndex(Partition);
            await Task.Delay(200, ct);
            bool stable = current == previous && current == raft.GetCommitIndex(Partition);
            previous = current;
            return stable;
        }, timeoutMs: 30_000);
    }

    private static async Task<byte[]> ExportPartitionState(KahunaManager leader, long upToIndex, CancellationToken ct)
    {
        await using Stream stream = await leader.KeyValues.PartitionStateTransfer.ExportPartitionState(Partition, upToIndex, ct);
        using MemoryStream buffer = new();
        await stream.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }

    [Fact]
    public async Task SnapshotBetweenThePrepareAndTheSettle_SeededReplicaInstallsFromTheSeededIntent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: config => config.DurableMaterializeOnResolve = true);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        int installee = -1;
        TaskCompletionSource releaseSettle = new(TaskCreationOptions.RunContinuationsAsynchronously);
        DurableTransactionFinalizer? finalizer = null;

        try
        {
            string[] keys = KeysOnPartition(managers[0], "snapmat", count: 2);
            string warmKey = KeysOnPartition(managers[0], "snapwarm", count: 1)[0];

            int leader = await LeaderIndexOf(rafts, ct);
            installee = (leader + 1) % Nodes;
            KahunaManager session = managers[leader];

            long term = await ReadCurrentTerm(rafts[leader], session, warmKey, ct);

            // Hold this coordinator's deferred settlement, so the transaction below is prepared and decided while
            // its intents are still live when the snapshot is exported.
            finalizer = session.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestBeforeDeferredResolutionHook = hookCt => releaseSettle.Task.WaitAsync(hookCt);

            KeyValueTransactionResult committed = await session.TryExecuteTransactionScript(Script(keys, "committed"), null, null);
            Assert.Equal(KeyValueResponseType.Set, committed.Type);

            foreach (string key in keys)
                Assert.NotNull(session.DurablePreparedIntentStore.Get(key));

            // From here on nothing reaches the seeded replica's consumer until the install, so the settle is
            // in the pending tail above the boundary.
            Assert.Equal(RaftOperationStatus.Success, await rafts[installee].HoldConsumerAppliesForTesting(Partition, ct));

            await WaitForStableCommitIndex(rafts[leader], ct);
            long snapshotIndex = rafts[leader].GetCommitIndex(Partition);
            byte[] payload = await ExportPartitionState(session, snapshotIndex, ct);

            // The export ran before the settle: the leader still holds both intents, so the seed carries them.
            foreach (string key in keys)
                Assert.NotNull(session.DurablePreparedIntentStore.Get(key));

            finalizer.TestBeforeDeferredResolutionHook = null;
            releaseSettle.TrySetResult();

            await WaitUntilAsync(() => keys.All(key => session.DurablePreparedIntentStore.Get(key) is null), timeoutMs: 30_000);
            await WaitUntilAsync(() => rafts[installee].GetCommitIndex(Partition) > snapshotIndex, timeoutMs: 30_000);

            SpyInstaller spy = new(managers[installee].DurablePreparedIntentStore.ResolvedIntentInstaller!);
            managers[installee].DurablePreparedIntentStore.AttachResolvedIntentInstaller(spy);

            SnapshotResponse response = await ((RaftManager)rafts[installee]).ReceiveInstallSnapshot(new SnapshotRequest
            {
                SessionId = Guid.NewGuid().ToString("N"),
                PartitionId = Partition,
                SnapshotIndex = snapshotIndex,
                LeaderTerm = term,
                LastIncludedTerm = term,
                LeaderEndpoint = EndpointOf(leader),
                FollowerEndpoint = EndpointOf(installee),
                ChunkIndex = 0,
                IsLast = true,
                Data = payload,
                Kind = SnapshotKind.PartitionState,
                SnapshotChecksum = Convert.ToHexString(SHA256.HashData(payload))
            }, ct);

            Assert.True(response.Success, "the follower refused the snapshot install");

            // The seed carried the pending intents.
            foreach (string key in keys)
                Assert.NotNull(managers[installee].DurablePreparedIntentStore.Get(key));

            Assert.Equal(RaftOperationStatus.Success, await rafts[installee].ResumeConsumerAppliesForTesting(Partition, ct));

            // The settle in the tail applies on the seeded replica: it installs each value from the seeded intent
            // and then removes the intent.
            await WaitUntilAsync(() => keys.All(key => managers[installee].DurablePreparedIntentStore.Get(key) is null), timeoutMs: 30_000);

            foreach (string key in keys)
            {
                Assert.Contains((key, false), spy.Installs);

                byte[] expected = Encoding.UTF8.GetBytes("committed");
                await WaitUntilAsync(
                    () => managers[installee].PersistenceBackend.GetKeyValue(key) is { Value: { } value } && value.AsSpan().SequenceEqual(expected),
                    timeoutMs: 30_000);
            }
        }
        finally
        {
            if (finalizer is not null)
                finalizer.TestBeforeDeferredResolutionHook = null;
            releaseSettle.TrySetResult();

            if (installee >= 0)
            {
                try { await rafts[installee].ResumeConsumerAppliesForTesting(Partition, ct); }
                catch (Exception) { /* teardown: the partition may already be stopping */ }
            }

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
