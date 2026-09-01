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
/// An <see cref="IRaft"/> stub exposing only the leadership surface the resolver wraps, with
/// a configurable hosted answer and failure; everything else throws so an unexpected
/// dependency is visible.
/// </summary>
internal sealed class LeadershipStubRaft : IRaft
{
    public bool Hosts { get; set; } = true;

    public bool Answer { get; set; } = true;

    public string LeaderEndpoint { get; set; } = "node2:2070";

    public string LocalEndpoint { get; set; } = "node1:2070";

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
    public string GetLocalEndpoint() => LocalEndpoint;

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

    public IReadOnlyList<RaftBackfillStatus> GetBackfillStatuses(int partitionId) => throw new NotImplementedException();

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
