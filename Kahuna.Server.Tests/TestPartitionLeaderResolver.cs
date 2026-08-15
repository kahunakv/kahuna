
using Kahuna.Server;

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
/// Coverage for the placement-safe leadership wrappers. A partition this node does not host must
/// answer false/null (a retryable routing condition) instead of throwing; the advisory hosted
/// check must not race incorrectly (a typed throw after the guard says yes is still answered
/// retryably); and an unknown partition id must keep throwing — that is a caller error, and
/// swallowing it would turn a routing bug into an infinite retry loop.
/// </summary>
public sealed class TestPartitionLeaderResolver
{
    [Fact]
    public async Task HostedPartition_DelegatesToTheRawApis()
    {
        LeadershipStubRaft raft = new() { Hosts = true, Answer = true, LeaderEndpoint = "node2:2070" };

        Assert.True(await raft.AmILeaderIfHosted(1, CancellationToken.None));
        Assert.True(await raft.AmILeaderQuickIfHosted(1));
        Assert.True(await raft.ConfirmLeadershipIfHosted(1, CancellationToken.None));
        Assert.Equal("node2:2070", await raft.TryResolveLeader(1, CancellationToken.None));

        Assert.Equal(4, raft.RawCalls);

        raft.Answer = false;

        Assert.False(await raft.AmILeaderIfHosted(1, CancellationToken.None));
        Assert.False(await raft.AmILeaderQuickIfHosted(1));
        Assert.False(await raft.ConfirmLeadershipIfHosted(1, CancellationToken.None));
    }

    [Fact]
    public async Task NotHostedPartition_AnswersRetryablyWithoutCallingKommander()
    {
        LeadershipStubRaft raft = new() { Hosts = false, Failure = new InvalidOperationException("the raw API must not be reached") };

        Assert.False(await raft.AmILeaderIfHosted(1, CancellationToken.None));
        Assert.False(await raft.AmILeaderQuickIfHosted(1));
        Assert.False(await raft.ConfirmLeadershipIfHosted(1, CancellationToken.None));

        // No hint and no replicas known: nothing to forward to, so the resolver answers null
        // (MustRetry) without touching the leadership APIs.
        Assert.Null(await raft.TryResolveLeader(1, CancellationToken.None));

        Assert.Equal(0, raft.RawCalls);
    }

    [Fact]
    public async Task NotHostedPartition_PrefersTheGossipedLeaderHint()
    {
        LeadershipStubRaft raft = new()
        {
            Hosts = false,
            Failure = new InvalidOperationException("the raw API must not be reached"),
            LeaderHint = "node4:2070",
            Replicas = [Voter("node2:2070"), Voter("node3:2070")]
        };

        Assert.Equal("node4:2070", await raft.TryResolveLeader(1, CancellationToken.None));
        Assert.Equal(0, raft.RawCalls);
    }

    [Fact]
    public async Task NotHostedPartition_FallsBackToTheFirstRemoteVoter()
    {
        // A hint naming the local node is useless for forwarding (the partition is not hosted
        // here) and must be skipped; transitional replicas come after voters.
        LeadershipStubRaft raft = new()
        {
            Hosts = false,
            LeaderHint = "node1:2070",
            Replicas =
            [
                new RaftReplica { Endpoint = "node5:2070", Role = RaftReplicaRole.Learner },
                new RaftReplica { Endpoint = "node1:2070", Role = RaftReplicaRole.Voter },
                Voter("node3:2070"),
                Voter("node4:2070")
            ]
        };

        Assert.Equal("node3:2070", await raft.TryResolveLeader(1, CancellationToken.None));
    }

    [Fact]
    public async Task NotHostedPartition_TransitionalReplicaIsTheLastResort()
    {
        LeadershipStubRaft raft = new()
        {
            Hosts = false,
            Replicas = [new RaftReplica { Endpoint = "node5:2070", Role = RaftReplicaRole.Learner }]
        };

        Assert.Equal("node5:2070", await raft.TryResolveLeader(1, CancellationToken.None));
    }

    [Fact]
    public async Task ForwardedRequest_NeverForwardsOnwardFromANonHostingReceiver()
    {
        LeadershipStubRaft raft = new()
        {
            Hosts = false,
            LeaderHint = "node4:2070",
            Replicas = [Voter("node2:2070"), Voter("node3:2070")]
        };

        using (ForwardedRequestScope.Enter())
            Assert.Null(await raft.TryResolveLeader(1, CancellationToken.None));

        // The suppression is scoped to the forwarded flow; an originator resolves normally again.
        Assert.Equal("node4:2070", await raft.TryResolveLeader(1, CancellationToken.None));
    }

    [Fact]
    public async Task MaterializationRace_TypedThrowAfterTheGuard_IsAnsweredRetryably()
    {
        // The committed map can list this node as a replica before the partition materializes
        // locally, so the hosted guard can say yes while the raw API still throws the typed
        // exception. The wrappers must treat the throw as the authoritative retryable answer.
        LeadershipStubRaft raft = new() { Hosts = true, Failure = new PartitionNotHostedException(7) };

        Assert.False(await raft.AmILeaderIfHosted(7, CancellationToken.None));
        Assert.False(await raft.AmILeaderQuickIfHosted(7));
        Assert.False(await raft.ConfirmLeadershipIfHosted(7, CancellationToken.None));
        Assert.Null(await raft.TryResolveLeader(7, CancellationToken.None));

        Assert.Equal(4, raft.RawCalls);
    }

    [Fact]
    public async Task UnknownPartition_PlainRaftExceptionPropagates()
    {
        LeadershipStubRaft raft = new() { Hosts = true, Failure = new RaftException("Invalid partition: 99") };

        await Assert.ThrowsAsync<RaftException>(async () => await raft.AmILeaderIfHosted(99, CancellationToken.None));
        await Assert.ThrowsAsync<RaftException>(async () => await raft.AmILeaderQuickIfHosted(99));
        await Assert.ThrowsAsync<RaftException>(async () => await raft.ConfirmLeadershipIfHosted(99, CancellationToken.None));
        await Assert.ThrowsAsync<RaftException>(async () => await raft.TryResolveLeader(99, CancellationToken.None));
    }

    private static RaftReplica Voter(string endpoint) => new() { Endpoint = endpoint, Role = RaftReplicaRole.Voter };

    /// <summary>
    /// An <see cref="IRaft"/> stub exposing only the leadership surface the resolver wraps, with
    /// a configurable hosted answer and failure; everything else throws so an unexpected
    /// dependency is visible.
    /// </summary>
    private sealed class LeadershipStubRaft : IRaft
    {
        public bool Hosts { get; set; } = true;

        public bool Answer { get; set; } = true;

        public string LeaderEndpoint { get; set; } = "node2:2070";

        public string? LeaderHint { get; set; }

        public List<RaftReplica> Replicas { get; set; } = [];

        public Exception? Failure { get; set; }

        public int RawCalls { get; private set; }

        private T Raw<T>(T value)
        {
            RawCalls++;

            if (Failure is not null)
                throw Failure;

            return value;
        }

        public bool HostsPartition(int partitionId) => Hosts;

        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => new(Raw(Answer));

        public ValueTask<bool> AmILeaderQuick(int partitionId) => new(Raw(Answer));

        public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) => new(Raw(Answer));

        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => new(Raw(LeaderEndpoint));

        public bool IsInitialized => true;
        public bool Joined => true;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public RaftConfiguration Configuration { get; } = new();
        public ClusterMembership GetMembership() => throw new NotImplementedException();
        public string GetLocalEndpoint() => "node1:2070";

        public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task JoinCluster(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public IReadOnlyList<RaftReplica> GetPartitionReplicas(int partitionId) => Replicas;
        public int GetEffectiveReplicationFactor(int partitionId) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SetReplicationFactorAsync(int partitionId, int replicationFactor, CancellationToken ct = default) => throw new NotImplementedException();
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
        public string? GetPartitionLeaderHint(int partitionId) => LeaderHint;
        public long GetCommitIndex(int partitionId) => throw new NotImplementedException();
        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => throw new NotImplementedException();
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
        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add { } remove { } }
        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }
    }
}
