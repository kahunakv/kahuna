
using Kahuna.Communication.External.Rest;
using Kahuna.Shared.Communication.Rest;

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

using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the cluster readiness surface. A node opens its port and answers membership
/// queries roughly a second after launch while still unable to resolve any partition leader, so
/// every key/value request is refused until initialization completes. The health endpoint and the
/// membership <c>initialized</c> flag must make that window observable: not-ready while
/// initializing (even though the node already reports a Voter role — the role alone is
/// misleading), not-ready when evicted to NotMember, and ready only once initialization is done.
/// </summary>
public sealed class TestClusterReadiness : IDisposable
{
    private readonly ILoggerFactory loggerFactory;

    public TestClusterReadiness(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    public void Dispose()
    {
        loggerFactory.Dispose();
    }

    [Fact]
    public void Health_WhileInitializing_ReportsNotReadyDespiteVoterRole()
    {
        KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(
            new FakeRaft { Initialized = false, Role = ClusterMemberRole.Voter });

        Assert.False(health.Ready);
        Assert.False(health.Initialized);
        Assert.Equal("Voter", health.LocalRole);
    }

    [Fact]
    public void Health_InitializedVoter_ReportsReady()
    {
        KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(
            new FakeRaft { Initialized = true, Role = ClusterMemberRole.Voter });

        Assert.True(health.Ready);
        Assert.True(health.Initialized);
        Assert.Equal("Voter", health.LocalRole);
    }

    [Fact]
    public void Health_InitializedLearner_ReportsReady()
    {
        KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(
            new FakeRaft { Initialized = true, Role = ClusterMemberRole.Learner });

        Assert.True(health.Ready);
    }

    /// <summary>A node evicted from the roster while it was down reports NotMember and cannot
    /// resolve partition leaders even though it finished its local initialization.</summary>
    [Fact]
    public void Health_InitializedButNotMember_ReportsNotReady()
    {
        KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(
            new FakeRaft { Initialized = true, Role = ClusterMemberRole.NotMember });

        Assert.False(health.Ready);
        Assert.True(health.Initialized);
        Assert.Equal("NotMember", health.LocalRole);
    }

    /// <summary>A probe hitting a node so early in boot that membership state is not constructed
    /// yet must get a fail-closed not-ready, never an unhandled exception (HTTP 500).</summary>
    [Fact]
    public void Health_MembershipStateUnavailable_FailsClosed()
    {
        KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(
            new FakeRaft { Initialized = false, RoleError = new NullReferenceException() });

        Assert.False(health.Ready);
        Assert.False(health.Initialized);
    }

    [Fact]
    public void Membership_CarriesInitializedFlag()
    {
        FakeRaft raft = new() { Initialized = false, Role = ClusterMemberRole.Voter };

        Assert.False(ClusterHandlers.BuildMembershipResponse(raft).Initialized);

        raft.Initialized = true;

        KahunaClusterMembershipResponse membership = ClusterHandlers.BuildMembershipResponse(raft);
        Assert.True(membership.Initialized);
        Assert.Equal("Voter", membership.LocalRole);
        Assert.Single(membership.Members);
    }

    /// <summary>Drives the real Raft implementation end to end: a fully started node must report
    /// ready/initialized through the same builders the REST routes serve.</summary>
    [Fact]
    public async Task StartedEmbeddedNode_ReportsReadyAndInitialized()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        // Before StartAsync the node is exactly in the "answering but not serving" window the
        // readiness surface exists to expose.
        Assert.False(ClusterHandlers.BuildHealthResponse(node.Raft).Ready);

        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(node.Raft);
        Assert.True(health.Ready);
        Assert.True(health.Initialized);
        Assert.NotEqual("NotMember", health.LocalRole);

        Assert.True(ClusterHandlers.BuildMembershipResponse(node.Raft).Initialized);
    }

    /// <summary>
    /// An <see cref="IRaft"/> exposing only the members the readiness/membership builders touch;
    /// everything else throws so an unexpected dependency is visible immediately.
    /// </summary>
    private sealed class FakeRaft : IRaft
    {
        public bool Initialized { get; set; }

        public ClusterMemberRole Role { get; set; } = ClusterMemberRole.Voter;

        public Exception? RoleError { get; init; }

        public bool IsInitialized => Initialized;

        public ClusterMemberRole LocalRole => RoleError is null ? Role : throw RoleError;

        public bool Joined => true;

        public RaftConfiguration Configuration { get; } = new() { InitialPartitions = 3 };

        public string GetLocalEndpoint() => "localhost:8080";

        public ClusterMembership GetMembership() => new()
        {
            MembershipVersion = 7,
            Members =
            [
                new ClusterMember { Endpoint = "localhost:8080", NodeId = 1, Role = Role, JoinedVersion = 1 }
            ]
        };

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

        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add { } remove { } }

        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }

        public ValueTask<bool> AmILeaderQuick(int partitionId) => throw new NotImplementedException();

        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();

        public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();

        public Task JoinCluster(CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();

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

        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => throw new NotImplementedException();

        public int GetPartitionKey(string partitionKey) => throw new NotImplementedException();

        public int GetPrefixPartitionKey(string prefixPartitionKey) => throw new NotImplementedException();

        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => throw new NotImplementedException();

        public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) => throw new NotImplementedException();
    }
}
