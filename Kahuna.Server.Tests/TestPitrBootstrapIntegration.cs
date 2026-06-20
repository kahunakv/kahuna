
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kahuna.Server.Communication.Internode;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Live multi-node integration tests for the PITR bootstrap-from-restore path.
/// Verifies that a node seeded by <see cref="BootstrapHelper"/> joins an existing
/// cluster via AppendEntries (delta) rather than InstallSnapshot, by asserting that the
/// snapshot-export hook is never invoked during the join.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestPitrBootstrapIntegration : BaseCluster, IDisposable
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;
    private readonly string tempRoot;

    public TestPitrBootstrapIntegration(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
        tempRoot = Path.Combine(Path.GetTempPath(), "kahuna_pitr_int_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);
    }

    public void Dispose()
    {
        if (Directory.Exists(tempRoot))
        {
            try { Directory.Delete(tempRoot, recursive: true); }
            catch { /* ignore cleanup failures */ }
        }
    }

    /// <summary>
    /// Records calls to <see cref="ExportRange"/> without throwing, allowing an explicit
    /// assertion after join that the count is zero (snapshot path was never triggered).
    /// </summary>
    private sealed class TrackingTransfer : IRaftStateMachineTransfer
    {
        private int exportCalls;

        public int ExportCalls => exportCalls;

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct)
        {
            Interlocked.Increment(ref exportCalls);
            return Task.FromResult<Stream>(new MemoryStream());
        }

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    // I1

    [Theory, CombinatorialData]
    public async Task SeededNode_JoinsViaDelta_NotSnapshot(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(3)] int partitions)
    {
        string artifactsDir = Path.Combine(tempRoot, "artifacts");
        Directory.CreateDirectory(artifactsDir);
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(Path.Combine(tempRoot, "catalog")));

        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3,
         InMemoryCommunication raftComm,
         MemoryInterNodeCommmunication interNodeComm) =
            await AssembleThreeNodeClusterFull(walStorage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Write a persistent KV entry that must survive on the joined node.
            string key = "pitr-int-" + Guid.NewGuid().ToString("N");
            byte[] value = "pitr-value"u8.ToArray();

            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Flush so the persistence backend contains the written data before checkpoint.
            KahunaManager km1 = (KahunaManager)kahuna1;
            await km1.FlushPersistenceAsync();

            // Take a Full backup from node 1's WAL and backend. No snapshotT cap needed here;
            // we'll derive targetTime from the manifest's partition ranges after the fact.
            BackupManifest fullManifest = await BackupDriver.RunFullAsync(
                raft1.WalAdapter, raft1.GetPartitionMap(), km1.PersistenceBackend,
                artifactsDir, catalog);

            // targetTime = max ToHlc across all partition ranges captured by the Full backup.
            HLCTimestamp targetTime = fullManifest.PartitionRanges
                .Select(r => r.ToHlc)
                .Aggregate(HLCTimestamp.Zero, (acc, hlc) => hlc.CompareTo(acc) > 0 ? hlc : acc);

            // Bootstrap node 4's WAL and backend from the Full backup so it presents itself
            // to the leader at the correct Raft index+term when it joins.
            InMemoryWAL seededWal = new(raftLogger);
            string cpPath = Path.Combine(artifactsDir, fullManifest.BackupId.ToString("N"), "checkpoint");
            MemoryPersistenceBackend seededBackend = MemoryPersistenceBackend.OpenCheckpoint(cpPath);

            IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(fullManifest.BackupId);

            // 24-hour window ensures the just-taken backup is always within the guard-rail.
            BootstrapHelper.BootstrapNode(
                chain, artifactsDir, targetTime, seededBackend, seededWal,
                TimeSpan.FromHours(24), DateTime.UtcNow);

            // Build node 4 with the seeded WAL + backend.
            (IRaft raft4, IKahuna kahuna4) = BuildNodeWithExternalWal(
                interNodeComm, raftComm, seededWal, seededBackend,
                nodeId: 4, port: 8004,
                peers: ["localhost:8001", "localhost:8002", "localhost:8003"],
                raftLogger, kahunaLogger, initialPartitions: partitions);

            // Replace the KvStateMachineTransfer registered by KahunaManager with a tracking
            // wrapper so we can assert the snapshot-export path is never triggered.
            TrackingTransfer tracker = new();
            raft4.RegisterStateMachineTransfer(tracker);

            raftComm.SetNodes(new Dictionary<string, IRaft>
            {
                { "localhost:8001", raft1 }, { "localhost:8002", raft2 },
                { "localhost:8003", raft3 }, { "localhost:8004", raft4 }
            });
            interNodeComm.SetNodes(new Dictionary<string, IKahuna>
            {
                { "localhost:8001", kahuna1 }, { "localhost:8002", kahuna2 },
                { "localhost:8003", kahuna3 }, { "localhost:8004", kahuna4 }
            });

            // Settle P0 leader before joining to avoid promotion stalls from election churn.
            await raft1.WaitForLeaderStableAsync(0, TimeSpan.FromMilliseconds(500),
                TestContext.Current.CancellationToken);

            // JoinCluster blocks until node 4 is promoted to Voter.
            await raft4.JoinCluster(["localhost:8001"], TestContext.Current.CancellationToken);
            Assert.Equal(ClusterMemberRole.Voter, raft4.LocalRole);

            // The seeded WAL checkpoint carries the correct index + term, so the leader found
            // a non-empty AppendEntries batch and never fell back to InstallSnapshot.
            Assert.Equal(0, tracker.ExportCalls);

            // The KV entry written before the backup must be readable on node 4: its state
            // comes from the checkpoint (restored by OpenCheckpoint) together with any delta
            // entries replayed via AppendEntries from the leader after join.
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) =
                await kahuna4.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry);
            Assert.Equal(value, entry.Value);

            await LeaveClusterSingle(raft4);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }
}
