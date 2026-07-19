
using Kommander;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;

namespace Kahuna.Server.Tests;

/// <summary>
/// An <see cref="IRaftReadScheduler"/> for tests that records the partition id every enqueue is
/// tagged with, and can either delegate to a real scheduler (so the read actually runs) or reject
/// every enqueue with <see cref="ReadBackpressureExceededException"/> (to drive the back-pressure
/// path). Used to verify that the resumable-read handlers route scan work to the owning data
/// partition and that a rejected enqueue surfaces as a retryable response instead of faulting.
/// </summary>
internal sealed class RecordingReadScheduler : IRaftReadScheduler
{
    private readonly IRaftReadScheduler? inner;
    private readonly bool rejectWithBackpressure;
    private readonly object gate = new();

    internal List<int> EnqueuedPartitions { get; } = new();

    internal RecordingReadScheduler(IRaftReadScheduler? inner = null, bool rejectWithBackpressure = false)
    {
        this.inner = inner;
        this.rejectWithBackpressure = rejectWithBackpressure;
    }

    public Task<T> EnqueueTask<T>(int partitionId, Func<T> operation)
    {
        lock (gate)
            EnqueuedPartitions.Add(partitionId);

        if (rejectWithBackpressure)
            throw new ReadBackpressureExceededException(partitionId, FairReadScheduler.DefaultMaxQueueDepth);

        if (inner is not null)
            return inner.EnqueueTask(partitionId, operation);

        // No real scheduler supplied: run off the caller's thread so the actor mailbox is still
        // freed (the handler dispatches, returns, and resumes on the ResumeRead message).
        return Task.Run(operation);
    }
}

/// <summary>
/// An <see cref="IRaft"/> that delegates every member to a real <see cref="RaftManager"/> except
/// <see cref="ReadScheduler"/>, which it replaces with a caller-supplied fake. This keeps real HLC
/// and partition routing (which the scan handlers depend on) while letting a test observe or fault
/// the partition-tagged read scheduling.
/// </summary>
internal sealed class SchedulerOverridingRaft(IRaft inner, IRaftReadScheduler scheduler) : IRaft
{
    public IRaftReadScheduler ReadScheduler => scheduler;

    public bool Joined => inner.Joined;
    public IWAL WalAdapter => inner.WalAdapter;
    public ICommunication Communication => inner.Communication;
    public IDiscovery Discovery => inner.Discovery;
    public RaftConfiguration Configuration => inner.Configuration;
    public HybridLogicalClock HybridLogicalClock => inner.HybridLogicalClock;
    public IRaftWalScheduler WalScheduler => inner.WalScheduler;
    public bool IsInitialized => inner.IsInitialized;
    public ClusterMemberRole LocalRole => inner.LocalRole;

    public event Action<int>? OnRestoreStarted { add => inner.OnRestoreStarted += value; remove => inner.OnRestoreStarted -= value; }
    public event Action<int>? OnRestoreFinished { add => inner.OnRestoreFinished += value; remove => inner.OnRestoreFinished -= value; }
    public event Action<int, RaftLog>? OnReplicationError { add => inner.OnReplicationError += value; remove => inner.OnReplicationError -= value; }
    public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add => inner.OnLogRestored += value; remove => inner.OnLogRestored -= value; }
    public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add => inner.OnReplicationReceived += value; remove => inner.OnReplicationReceived -= value; }
    public event Func<int, string, Task<bool>>? OnLeaderChanged { add => inner.OnLeaderChanged += value; remove => inner.OnLeaderChanged -= value; }
    public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add => inner.OnPartitionMapChanged += value; remove => inner.OnPartitionMapChanged -= value; }
    public event Action<ClusterMembership>? OnMembershipChanged { add => inner.OnMembershipChanged += value; remove => inner.OnMembershipChanged -= value; }

    public ClusterMembership GetMembership() => inner.GetMembership();
    public Task JoinCluster(CancellationToken cancellationToken = default) => inner.JoinCluster(cancellationToken);
    public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => inner.JoinCluster(seeds, cancellationToken);
    public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => inner.LeaveCluster(dispose, cancellationToken);
    public Task UpdateNodes() => inner.UpdateNodes();
    public IList<RaftNode> GetNodes() => inner.GetNodes();
    public HLCTimestamp GetLastNodeActivity(string endpoint) => inner.GetLastNodeActivity(endpoint);
    public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => inner.GetActiveNodes(within);
    public Task Handshake(HandshakeRequest request) => inner.Handshake(request);
    public void RequestVote(RequestVotesRequest request) => inner.RequestVote(request);
    public void Vote(VoteRequest request) => inner.Vote(request);
    public void AppendLogs(AppendLogsRequest request) => inner.AppendLogs(request);
    public void CompleteAppendLogs(CompleteAppendLogsRequest request) => inner.CompleteAppendLogs(request);
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => inner.ReplicateLogs(partitionId, type, data, autoCommit, expectedGeneration, cancellationToken);
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => inner.ReplicateLogs(partitionId, type, logs, autoCommit, expectedGeneration, cancellationToken);
    public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default) => inner.ReplicateEntries(partitionId, entries, cancellationToken);
    public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => inner.ReplicateCheckpoint(partitionId, cancellationToken);
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => inner.CommitLogs(partitionId, ticketId, cancellationToken);
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => inner.RollbackLogs(partitionId, ticketId, cancellationToken);
    public void SetMinRetainIndex(int partitionId, long index) => inner.SetMinRetainIndex(partitionId, index);
    public string GetLocalEndpoint() => inner.GetLocalEndpoint();
    public int GetLocalNodeId() => inner.GetLocalNodeId();
    public string GetLocalNodeName() => inner.GetLocalNodeName();
    public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) => inner.GetFollowerLagAsync(partitionId, followerEndpoint);
    public ValueTask<bool> AmILeaderQuick(int partitionId) => inner.AmILeaderQuick(partitionId);
    public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => inner.AmILeader(partitionId, cancellationToken);
    public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => inner.WaitForLeader(partitionId, cancellationToken);
    public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => inner.WaitForLeaderStableAsync(partitionId, minStableFor, cancellationToken);
    public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => inner.ForceLeaderForTestingAsync(partitionId, cancellationToken);
    public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => inner.StepDownAsync(partitionId, cancellationToken);
    public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => inner.TransferLeadershipAsync(partitionId, targetEndpoint, cancellationToken);
    public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => inner.SuspendHeartbeatsAsync(partitionId, cancellationToken);
    public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => inner.ResumeHeartbeatsAsync(partitionId, cancellationToken);
    public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => inner.CreatePartitionAsync(partitionId, mode, hashRange, ct);
    public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => inner.RemovePartitionAsync(partitionId, ct);
    public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => inner.SplitPartitionAsync(sourcePartitionId, targetPartitionId, plan, ct);
    public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => inner.MergePartitionsAsync(survivorPartitionId, sourcePartitionId, plan, ct);
    public long GetPartitionGeneration(int partitionId) => inner.GetPartitionGeneration(partitionId);
    public double GetPartitionLogOpsPerSecond(int partitionId) => inner.GetPartitionLogOpsPerSecond(partitionId);
    public int GetPartitionWalQueueDepth(int partitionId) => inner.GetPartitionWalQueueDepth(partitionId);
    public double GetPartitionCommitWaitMs(int partitionId) => inner.GetPartitionCommitWaitMs(partitionId);
    public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => inner.GetPartitionMap();
    public int GetPartitionKey(string partitionKey) => inner.GetPartitionKey(partitionKey);
    public int GetPrefixPartitionKey(string prefixPartitionKey) => inner.GetPrefixPartitionKey(prefixPartitionKey);
    public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => inner.RegisterStateMachineTransfer(transfer);

    public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) => inner.RegisterSystemStateTransfer(transfer);
}
