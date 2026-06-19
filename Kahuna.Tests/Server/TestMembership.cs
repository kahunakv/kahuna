
using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

[Collection("ClusterTests")]
public sealed class TestMembership : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestMembership(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // E1

    [Theory, CombinatorialData]
    public async Task NewVoterJoins_ServesAllPartitions(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(3)] int partitions)
    {
        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3,
         InMemoryCommunication raftComm,
         MemoryInterNodeCommmunication interNodeComm) =
            await AssembleThreeNodeClusterFull(walStorage, partitions, raftLogger, kahunaLogger);

        try
        {
            (IRaft raft4, IKahuna kahuna4) = BuildNode(interNodeComm, raftComm, walStorage,
                nodeId: 4, port: 8004,
                peers: new[] { "localhost:8001", "localhost:8002", "localhost:8003" },
                raftLogger, kahunaLogger);

            raftComm.SetNodes(new Dictionary<string, IRaft>
            {
                { "localhost:8001", raft1 },
                { "localhost:8002", raft2 },
                { "localhost:8003", raft3 },
                { "localhost:8004", raft4 }
            });
            interNodeComm.SetNodes(new Dictionary<string, IKahuna>
            {
                { "localhost:8001", kahuna1 },
                { "localhost:8002", kahuna2 },
                { "localhost:8003", kahuna3 },
                { "localhost:8004", kahuna4 }
            });

            // JoinCluster(seeds) blocks until node4 is promoted to Voter.
            await raft4.JoinCluster(new[] { "localhost:8001" }, TestContext.Current.CancellationToken);

            Assert.Equal(ClusterMemberRole.Voter, raft4.LocalRole);

            for (int p = 1; p <= partitions; p++)
            {
                string key = $"e1-p{p}-{Guid.NewGuid():N}";
                byte[] value = Encoding.UTF8.GetBytes($"v{p}");

                (KeyValueResponseType setType, _, _) = await kahuna4.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Ephemeral, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, setType);

                (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna4.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Ephemeral, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Get, getType);
                Assert.NotNull(entry);
                Assert.Equal(value, entry.Value);
            }

            await LeaveClusterSingle(raft4);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    // E2

    [Theory, CombinatorialData]
    public async Task GracefulLeave_ShrinksRosterAndClusterServesRequests(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(3)] int partitions)
    {
        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(walStorage, partitions, raftLogger, kahunaLogger);

        try
        {
            long versionBefore = Math.Max(
                raft1.GetMembership().MembershipVersion,
                raft2.GetMembership().MembershipVersion);

            await raft3.LeaveCluster(dispose: false);

            await WaitUntilAsync(() =>
            {
                ClusterMembership m = raft1.GetMembership();
                return m.MembershipVersion > versionBefore
                    && m.Members.Count(x =>
                        x.Role != ClusterMemberRole.Leaving &&
                        x.Role != ClusterMemberRole.NotMember) == 2;
            }, timeoutMs: 15_000);

            ClusterMembership after = raft1.GetMembership();
            Assert.DoesNotContain(after.Members, m =>
                m.Endpoint == "localhost:8003" && m.Role == ClusterMemberRole.Voter);

            string key = $"e2-{Guid.NewGuid():N}";
            byte[] value = Encoding.UTF8.GetBytes("after-leave");

            (KeyValueResponseType type, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Ephemeral, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, type);

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                KeyValueDurability.Ephemeral, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry);
        }
        finally
        {
            await LeaveClusterSingle(raft1);
            await LeaveClusterSingle(raft2);
            try { await raft3.LeaveCluster(dispose: true); } catch { }
        }
    }

    // E3

    [Theory, CombinatorialData]
    public async Task SwimEviction_KilledNode_RemovedFromRoster(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(3)] int partitions)
    {
        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna _, IKahuna _,
         InMemoryCommunication raftComm,
         MemoryInterNodeCommmunication _2) =
            await AssembleSwimCluster(walStorage, partitions, raftLogger, kahunaLogger,
                pingIntervalMs: 200, pingTimeoutMs: 100,
                suspicionTimeoutMs: 400, deadMemberEvictionGraceMs: 300,
                updateNodesIntervalMs: 300);

        try
        {
            raftComm.PartitionNode("localhost:8003");

            await WaitUntilAsync(() =>
                raft1.GetMembership().Members.All(x => x.Endpoint != "localhost:8003"),
                timeoutMs: 15_000);

            string key = $"e3-{Guid.NewGuid():N}";
            (KeyValueResponseType type, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("alive"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Ephemeral,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, type);

            ClusterMembership roster = raft1.GetMembership();
            Assert.Contains(roster.Members, m =>
                m.Endpoint == "localhost:8001" && m.Role == ClusterMemberRole.Voter);
            Assert.Contains(roster.Members, m =>
                m.Endpoint == "localhost:8002" && m.Role == ClusterMemberRole.Voter);
        }
        finally
        {
            raftComm.HealPartition("localhost:8003");
            await LeaveClusterSingle(raft1);
            await LeaveClusterSingle(raft2);
            try { await raft3.LeaveCluster(dispose: true); } catch { }
        }
    }

    // E4

    [Theory, CombinatorialData]
    public async Task RangeRouting_StableAcrossMembershipChange(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(3)] int partitions)
    {
        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3,
         InMemoryCommunication raftComm,
         MemoryInterNodeCommmunication interNodeComm) =
            await AssembleThreeNodeClusterFull(walStorage, partitions, raftLogger, kahunaLogger);

        try
        {
            string[] keys = new[] { "e4:alpha/row1", "e4:beta/row2", "e4:gamma/row3", "e4:delta/row4" };

            Dictionary<string, int> routingBefore = new();
            foreach (string key in keys)
                routingBefore[key] = raft1.GetPartitionKey(key);

            (IRaft raft4, IKahuna kahuna4) = BuildNode(interNodeComm, raftComm, walStorage,
                nodeId: 4, port: 8004,
                peers: new[] { "localhost:8001", "localhost:8002", "localhost:8003" },
                raftLogger, kahunaLogger);

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

            // JoinCluster(seeds) blocks until node4 is promoted to Voter.
            await raft4.JoinCluster(new[] { "localhost:8001" }, TestContext.Current.CancellationToken);

            foreach (string key in keys)
                Assert.Equal(routingBefore[key], raft1.GetPartitionKey(key));

            await LeaveClusterSingle(raft4);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    // E5

    [Fact]
    public async Task CompactionFloorJoin_SurfacesClearError()
    {
        var throwMsg = "RaftManager.JoinCluster: promotion permanently blocked — partition 1 learner start index 1 is below the WAL compaction floor 500";
        var stubRaft = new StubRaftForE5(new InvalidOperationException(throwMsg));

        var opts = new KahunaCommandLineOptions
        {
            RaftJoinExisting = true,
            InitialCluster = new[] { "localhost:8001" }
        };

        var svc = new Kahuna.Services.ReplicationService(
            new StubKahunaForE5(), stubRaft, opts, raftLogger);

        // BackgroundService.StartAsync may return Task.CompletedTask before the async
        // state machine finishes (xunit SynchronizationContext defers continuations).
        // Call ExecuteAsync directly via reflection to observe the rethrow without the
        // BackgroundService hosting layer in the way.
        var executeMethod = svc.GetType().GetMethod(
            "ExecuteAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var executeTask = (Task)executeMethod.Invoke(svc, [CancellationToken.None])!;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => executeTask);

        Assert.Contains("permanently blocked", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // stubs

    private sealed class StubRaftForE5 : IRaft
    {
        private readonly Exception throwOnJoin;
        public StubRaftForE5(Exception throwOnJoin) { this.throwOnJoin = throwOnJoin; }

        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default)
            => Task.FromException(throwOnJoin);
        public Task JoinCluster(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public RaftConfiguration Configuration { get; } = new() { Host = "localhost", Port = 9999 };
        public string GetLocalEndpoint() => "localhost:9999";
        public ClusterMemberRole LocalRole => ClusterMemberRole.NotMember;
        public ClusterMembership GetMembership() => new();
        public Task LeaveCluster(bool dispose = false) => Task.CompletedTask;

        public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add { } remove { } }
        public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add { } remove { } }
        public event Action<int, RaftLog>? OnReplicationError { add { } remove { } }
        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }
        public event Action<int>? OnRestoreStarted { add { } remove { } }
        public event Action<int>? OnRestoreFinished { add { } remove { } }
        public event Func<int, string, Task<bool>>? OnLeaderChanged { add { } remove { } }
        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add { } remove { } }

        public bool Joined => false;
        public bool IsInitialized => false;
        public IWAL WalAdapter => null!;
        public ICommunication Communication => null!;
        public IDiscovery Discovery => null!;
        public HybridLogicalClock HybridLogicalClock => null!;
        public IRaftReadScheduler ReadScheduler => null!;
        public IRaftWalScheduler WalScheduler => null!;

        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => Array.Empty<RaftPartitionRange>();
        public int GetPartitionKey(string partitionKey) => 0;
        public int GetPrefixPartitionKey(string prefixPartitionKey) => 0;
        public long GetPartitionGeneration(int partitionId) => 0;
        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) => ValueTask.FromResult<long?>(null);
        public ValueTask<bool> AmILeaderQuick(int partitionId) => ValueTask.FromResult(false);
        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => ValueTask.FromResult(false);
        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => ValueTask.FromResult(string.Empty);
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => ValueTask.FromResult(string.Empty);
        public Task UpdateNodes() => Task.CompletedTask;
        public IList<RaftNode> GetNodes() => Array.Empty<RaftNode>();
        public HLCTimestamp GetLastNodeActivity(string endpoint) => HLCTimestamp.Zero;
        public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => Array.Empty<string>();
        public Task Handshake(HandshakeRequest request) => Task.CompletedTask;
        public void RequestVote(RequestVotesRequest request) { }
        public void Vote(VoteRequest request) { }
        public void AppendLogs(AppendLogsRequest request) { }
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) { }
        public void SetMinRetainIndex(int partitionId, long index) { }
        public int GetLocalNodeId() => 99;
        public string GetLocalNodeName() => "stub";
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) { }
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
    }

    private sealed class StubKahunaForE5 : IKahuna
    {
        public Task<bool> OnLogRestored(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> OnReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void OnReplicationError(int partitionId, RaftLog log) { }
        public Task FlushPersistenceAsync() => Task.CompletedTask;
        public void RegisterKeyRange(string keySpace) { }
        public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) => Task.FromResult(false);
        public Task<int> TriggerAutoSplitAsync(CancellationToken ct = default) => Task.FromResult(0);
        public Task<int> TriggerAutoMergeAsync(CancellationToken ct = default) => Task.FromResult(0);

        public Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(LockResponseType, ReadOnlyLockEntry?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability) => throw new NotImplementedException();
        public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability) => throw new NotImplementedException();
        public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability) => throw new NotImplementedException();
        public Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string resource, LockDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0) => throw new NotImplementedException();
        public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(HLCTimestamp transactionId, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(HLCTimestamp transactionId, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, long routedGeneration = 0) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(HLCTimestamp transactionId, List<(string key, long revision, KeyValueDurability durability)> keys) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(HLCTimestamp transactionId, List<(string key, long revision, KeyValueDurability durability)> keys) => throw new NotImplementedException();
        public Task<KeyValueResponseType> TryCheckWriteIntentValue(HLCTimestamp transactionId, string key, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndCommitTransaction(string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, List<KeyValueTransactionReadKey> readKeys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<KeyValueResponseType> LocateAndRollbackTransaction(string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<List<KeyValueRangeLock>> GetRangeLocks(string keySpace) => throw new NotImplementedException();
        public Task ImportRangeLocks(string keySpace, List<KeyValueRangeLock> locks) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, long routedGeneration = 0) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long)> TryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, long)> TryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueTransactionResult> TryExecuteTransactionScript(byte[] script, string? hash, List<KeyValueParameter>? parameters) => throw new NotImplementedException();
        public Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp transactionId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotImplementedException();
        public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(KeyValueTransactionOptions options) => throw new NotImplementedException();
        public Task<KeyValueResponseType> CommitTransaction(HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, List<KeyValueTransactionReadKey> readKeys) => throw new NotImplementedException();
        public Task<KeyValueResponseType> RollbackTransaction(HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys) => throw new NotImplementedException();
        public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(string name, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(SequenceResponseType, long)> LocateAndCreateSequence(string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<SequenceResponseType> LocateAndDeleteSequence(string name, SequenceDurability durability, CancellationToken cancellationToken) => throw new NotImplementedException();
    }
}
