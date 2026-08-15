
using Kahuna.Server.KeyValues.Ranges;

using Kommander;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the per-node projection of the committed partition map. The projection must be
/// bit-for-bit legacy when replica sets are empty (RF = 0), report exactly the committed replica
/// sets when placement is on, deliver hosted-set transitions exactly once, and swap snapshots
/// atomically so a concurrent reader never observes a half-applied map.
/// </summary>
public sealed class TestPartitionPlacementView
{
    private const string LocalEndpoint = "node1:2070";

    private static RaftPartitionRange Range(int partitionId, params RaftReplica[] replicas) => new()
    {
        PartitionId = partitionId,
        State = RaftPartitionState.Active,
        Replicas = [.. replicas]
    };

    private static RaftReplica Voter(string endpoint) => new() { Endpoint = endpoint, Role = RaftReplicaRole.Voter };

    private static RaftReplica Learner(string endpoint) => new() { Endpoint = endpoint, Role = RaftReplicaRole.Learner };

    private static RaftReplica Removing(string endpoint) => new() { Endpoint = endpoint, Role = RaftReplicaRole.Removing };

    [Fact]
    public void LegacyMap_ReportsHostedEverywhere()
    {
        MapStubRaft raft = new(LocalEndpoint, [Range(1), Range(2), Range(3)]);
        using PartitionPlacementView view = new(raft);

        Assert.True(view.IsLocallyHosted(0));
        Assert.True(view.IsLocallyHosted(1));
        Assert.True(view.IsLocallyHosted(2));
        Assert.True(view.IsLocallyHosted(3));

        // A partition the snapshot has never seen must fall back to the legacy path too:
        // the projection can be behind the cluster, and the fallback is retry-safe.
        Assert.True(view.IsLocallyHosted(99));

        Assert.Empty(view.ReplicaEndpoints(1));
        Assert.Empty(view.ReplicaEndpoints(99));
        Assert.Equal([1, 2, 3], view.HostedPartitions.Order());
    }

    [Fact]
    public void PlacedMap_ReportsExactlyTheCommittedSet()
    {
        // Six nodes, four ranges at RF=3; the local node hosts ranges 1 and 3 only.
        MapStubRaft raft = new(LocalEndpoint,
        [
            Range(1, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070")),
            Range(2, Voter("node4:2070"), Voter("node5:2070"), Voter("node6:2070")),
            Range(3, Voter("node5:2070"), Voter(LocalEndpoint), Voter("node2:2070")),
            Range(4, Voter("node3:2070"), Voter("node4:2070"), Voter("node6:2070"))
        ]);
        using PartitionPlacementView view = new(raft);

        Assert.True(view.IsLocallyHosted(1));
        Assert.False(view.IsLocallyHosted(2));
        Assert.True(view.IsLocallyHosted(3));
        Assert.False(view.IsLocallyHosted(4));
        Assert.Equal([1, 3], view.HostedPartitions.Order());

        // Remote endpoints exclude the local node and preserve the committed set.
        Assert.Equal(["node2:2070", "node3:2070"], view.ReplicaEndpoints(1));
        Assert.Equal(["node4:2070", "node5:2070", "node6:2070"], view.ReplicaEndpoints(2));
    }

    [Fact]
    public void ReplicaEndpoints_ListsVotersBeforeTransitionalReplicas()
    {
        MapStubRaft raft = new(LocalEndpoint,
        [
            Range(1, Learner("node2:2070"), Voter("node3:2070"), Removing("node4:2070"), Voter("node5:2070"), Voter(LocalEndpoint))
        ]);
        using PartitionPlacementView view = new(raft);

        Assert.True(view.IsLocallyHosted(1));
        Assert.Equal(["node3:2070", "node5:2070", "node2:2070", "node4:2070"], view.ReplicaEndpoints(1));
    }

    [Fact]
    public void HostedTransitions_FireExactlyOncePerChange()
    {
        MapStubRaft raft = new(LocalEndpoint,
        [
            Range(1, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070")),
            Range(2, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070"))
        ]);
        using PartitionPlacementView view = new(raft);

        List<(int[] Gained, int[] Lost)> transitions = [];
        view.HostedPartitionsChanged += (gained, lost) => transitions.Add(([.. gained.Order()], [.. lost.Order()]));

        // The local replica of range 1 moves to node4; range 3 arrives hosted here.
        raft.ApplyMap(
        [
            Range(1, Voter("node4:2070"), Voter("node2:2070"), Voter("node3:2070")),
            Range(2, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070")),
            Range(3, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node4:2070"))
        ]);

        (int[] gained, int[] lost) = Assert.Single(transitions);
        Assert.Equal([3], gained);
        Assert.Equal([1], lost);

        // Re-applying the same map is not a transition.
        raft.ApplyMap(raft.GetPartitionMap());
        Assert.Single(transitions);
    }

    [Fact]
    public void LegacyRangeGainingRemotePlacement_IsReportedAsLost()
    {
        MapStubRaft raft = new(LocalEndpoint, [Range(1)]);
        using PartitionPlacementView view = new(raft);

        List<int> lostPartitions = [];
        view.HostedPartitionsChanged += (_, lost) => lostPartitions.AddRange(lost);

        raft.ApplyMap([Range(1, Voter("node2:2070"), Voter("node3:2070"), Voter("node4:2070"))]);

        Assert.Equal([1], lostPartitions);
        Assert.False(view.IsLocallyHosted(1));
    }

    [Fact]
    public async Task SnapshotSwap_IsAtomicUnderAConcurrentReader()
    {
        RaftPartitionRange[] mapA =
        [
            Range(1, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070")),
            Range(2, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070")),
            Range(3, Voter("node2:2070"), Voter("node3:2070"), Voter("node4:2070")),
            Range(4, Voter("node2:2070"), Voter("node3:2070"), Voter("node4:2070"))
        ];
        RaftPartitionRange[] mapB =
        [
            Range(1, Voter("node2:2070"), Voter("node3:2070"), Voter("node4:2070")),
            Range(2, Voter("node2:2070"), Voter("node3:2070"), Voter("node4:2070")),
            Range(3, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070")),
            Range(4, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070"))
        ];

        MapStubRaft raft = new(LocalEndpoint, mapA);
        using PartitionPlacementView view = new(raft);
        using CancellationTokenSource cts = new();

        int[] hostedUnderMapA = [1, 2];
        int[] hostedUnderMapB = [3, 4];

        Task reader = Task.Run(() =>
        {
            while (!cts.IsCancellationRequested)
            {
                // Each snapshot hosts either exactly {1,2} or exactly {3,4}; observing a mix
                // means a reader saw a half-applied swap.
                int[] hosted = [.. view.HostedPartitions.Order()];
                Assert.True(hosted.SequenceEqual(hostedUnderMapA) || hosted.SequenceEqual(hostedUnderMapB),
                    $"Observed a torn hosted set: [{string.Join(',', hosted)}]");
            }
        }, CancellationToken.None);

        for (int i = 0; i < 2_000; i++)
            raft.ApplyMap(i % 2 == 0 ? mapB : mapA);

        cts.Cancel();
        await reader;
    }

    [Fact]
    public void Dispose_StopsTrackingMapChanges()
    {
        MapStubRaft raft = new(LocalEndpoint, [Range(1, Voter(LocalEndpoint), Voter("node2:2070"), Voter("node3:2070"))]);
        PartitionPlacementView view = new(raft);
        view.Dispose();

        raft.ApplyMap([Range(1, Voter("node2:2070"), Voter("node3:2070"), Voter("node4:2070"))]);

        // The stale answer is fine (the view is dead); what must not happen is the disposed
        // view still reacting to the live event feed.
        Assert.True(view.IsLocallyHosted(1));
    }

    /// <summary>
    /// An <see cref="IRaft"/> stub exposing only what the placement view consumes: the local
    /// endpoint, the committed partition map, and the map-changed event. Everything else throws
    /// so an unexpected dependency is visible.
    /// </summary>
    private sealed class MapStubRaft : IRaft
    {
        private readonly string localEndpoint;

        private IReadOnlyList<RaftPartitionRange> map;

        public MapStubRaft(string localEndpoint, IReadOnlyList<RaftPartitionRange> initialMap)
        {
            this.localEndpoint = localEndpoint;
            map = initialMap;
        }

        /// <summary>Swaps the committed map and fires the event, mirroring map application.</summary>
        public void ApplyMap(IReadOnlyList<RaftPartitionRange> newMap)
        {
            map = newMap;
            OnPartitionMapChanged?.Invoke(newMap);
        }

        public string GetLocalEndpoint() => localEndpoint;

        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => map;

        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged;

        public bool IsInitialized => true;
        public bool Joined => true;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public RaftConfiguration Configuration { get; } = new();
        public ClusterMembership GetMembership() => throw new NotImplementedException();

        public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task JoinCluster(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public ValueTask<bool> AmILeaderQuick(int partitionId) => throw new NotImplementedException();
        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();
        public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public IReadOnlyList<RaftReplica> GetPartitionReplicas(int partitionId) => throw new NotImplementedException();
        public int GetEffectiveReplicationFactor(int partitionId) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SetReplicationFactorAsync(int partitionId, int replicationFactor, CancellationToken ct = default) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task UpdateNodes() => throw new NotImplementedException();
        public IList<RaftNode> GetNodes() => throw new NotImplementedException();
        public HLCTimestamp GetLastNodeActivity(string endpoint) => throw new NotImplementedException();
        public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => throw new NotImplementedException();
        public Task Handshake(HandshakeRequest request) => throw new NotImplementedException();
        public void RequestVote(RequestVotesRequest request) => throw new NotImplementedException();
        public void Vote(VoteRequest request) => throw new NotImplementedException();
        public void AppendLogs(AppendLogsRequest request) => throw new NotImplementedException();
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public void SetMinRetainIndex(int partitionId, long index) => throw new NotImplementedException();
        public IDisposable AcquireRetentionHold(int partitionId, long index) => throw new NotImplementedException();
        public int GetLocalNodeId() => throw new NotImplementedException();
        public string GetLocalNodeName() => throw new NotImplementedException();
        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, TimeSpan timeout, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public long GetPartitionGeneration(int partitionId) => throw new NotImplementedException();
        public double GetPartitionLogOpsPerSecond(int partitionId) => throw new NotImplementedException();
        public int GetPartitionWalQueueDepth(int partitionId) => throw new NotImplementedException();
        public double GetPartitionCommitWaitMs(int partitionId) => throw new NotImplementedException();
        public long GetStaleProposedSkippedCount(int partitionId) => throw new NotImplementedException();
        public IReadOnlyList<RaftSnapshotStatus> GetSnapshotStatuses(int partitionId) => throw new NotImplementedException();
        public bool HostsPartition(int partitionId) => throw new NotImplementedException();
        public string? GetPartitionLeaderHint(int partitionId) => throw new NotImplementedException();
        public long GetCommitIndex(int partitionId) => throw new NotImplementedException();
        public int GetPartitionKey(string partitionKey) => throw new NotImplementedException();
        public int GetPrefixPartitionKey(string prefixPartitionKey) => throw new NotImplementedException();
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => throw new NotImplementedException();
        public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) => throw new NotImplementedException();
        public void RegisterPartitionStateTransfer(IRaftPartitionStateTransfer? transfer) => throw new NotImplementedException();

        public IWAL WalAdapter => throw new NotImplementedException();
        public ICommunication Communication => throw new NotImplementedException();
        public IDiscovery Discovery => throw new NotImplementedException();
        public HybridLogicalClock HybridLogicalClock => throw new NotImplementedException();
        public IRaftReadScheduler ReadScheduler => throw new NotImplementedException();
        public IRaftWalScheduler WalScheduler => throw new NotImplementedException();

        public event Action<int>? OnRestoreStarted { add { } remove { } }
        public event Action<int>? OnRestoreFinished { add { } remove { } }
        public event Action<int, RaftLog>? OnReplicationError { add { } remove { } }
        public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add { } remove { } }
        public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add { } remove { } }
        public event Func<int, string, Task<bool>>? OnLeaderChanged { add { } remove { } }
        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }
    }
}
