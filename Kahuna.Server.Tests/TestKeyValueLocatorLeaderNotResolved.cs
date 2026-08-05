
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

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
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit coverage for the locator's handling of a node that reports itself joined but cannot resolve
/// a partition leader — the state a restarted node is in while it rejoins the cluster and its Raft
/// partitions are not yet constructed. In that window <c>IRaft.WaitForLeader</c> throws a
/// <see cref="RaftNodeNotReadyException"/> (and can also throw plain <see cref="RaftException"/>
/// when an election doesn't settle in time). The locator must map both to a typed
/// <see cref="KeyValueResponseType.MustRetry"/> response — never let the exception escape to the
/// transport layer, where it previously surfaced to clients as an unhandled HTTP 500 with no
/// response type to classify.
/// </summary>
public sealed class TestKeyValueLocatorLeaderNotResolved
{
    [Fact]
    public async Task StartTransaction_LeaderUnresolvable_ReturnsMustRetry()
    {
        KeyValueLocator locator = BuildLocator(new NotReadyRaft());

        (KeyValueResponseType type, TransactionHandle handle) = await locator.LocateAndStartTransaction(
            new() { CoordinatorKey = "coordinator-key" }, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.True(handle.IsEmpty);
    }

    [Fact]
    public async Task CommitTransaction_LeaderUnresolvable_ReturnsMustRetry()
    {
        KeyValueLocator locator = BuildLocator(new NotReadyRaft());

        TransactionHandle handle = new(new HLCTimestamp(1, 100, 0), "coordinator-key", null);

        (KeyValueResponseType type, string? anchor) = await locator.LocateAndCommitTransaction(
            handle, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Null(anchor);
    }

    [Fact]
    public async Task RollbackTransaction_LeaderUnresolvable_ReturnsMustRetry()
    {
        KeyValueLocator locator = BuildLocator(new NotReadyRaft());

        TransactionHandle handle = new(new HLCTimestamp(1, 100, 0), "coordinator-key", null);

        KeyValueResponseType type = await locator.LocateAndRollbackTransaction(
            handle, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    /// <summary>
    /// The undecided-election shape (plain <see cref="RaftException"/> from the 10 s budget) must map
    /// the same way — MustRetry, not an escaping exception.
    /// </summary>
    [Fact]
    public async Task StartTransaction_ElectionUndecided_ReturnsMustRetry()
    {
        KeyValueLocator locator = BuildLocator(
            new NotReadyRaft { WaitForLeaderError = new RaftException("Leader couldn't be found or is not decided") });

        (KeyValueResponseType type, TransactionHandle handle) = await locator.LocateAndStartTransaction(
            new() { CoordinatorKey = "coordinator-key" }, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.True(handle.IsEmpty);
    }

    /// <summary>Cancellation must still propagate — only Raft resolution failures map to MustRetry.</summary>
    [Fact]
    public async Task StartTransaction_Cancelled_Throws()
    {
        KeyValueLocator locator = BuildLocator(
            new NotReadyRaft { WaitForLeaderError = new OperationCanceledException() });

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await locator.LocateAndStartTransaction(
                new() { CoordinatorKey = "coordinator-key" }, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// The transaction-session paths under test consult only the hash router and the Raft façade
    /// before redirecting, so the manager and inter-node transport are never touched — passing null
    /// keeps the fixture honest: if a refactor makes these paths reach the manager while the leader
    /// is unresolved, the test fails loudly instead of silently serving from a half-built node.
    /// </summary>
    private static KeyValueLocator BuildLocator(IRaft raft) =>
        new(
            manager: null!,
            new KahunaConfiguration(),
            raft,
            interNodeCommunication: null!,
            keySpaceRegistry: null!,
            quiesceStore: null!,
            NullLogger<IKahuna>.Instance);

    /// <summary>
    /// An <see cref="IRaft"/> frozen in the restart window: joined, not the leader of anything, and
    /// unable to resolve any partition leader. Only the members the locator's routing path touches
    /// are implemented; everything else throws so an unexpected dependency is visible immediately.
    /// </summary>
    private sealed class NotReadyRaft : IRaft
    {
        public Exception WaitForLeaderError { get; init; } =
            new RaftNodeNotReadyException("Cannot resolve leader for partition 3: node has not completed cluster initialization");

        public bool Joined => true;

        public bool IsInitialized => false;

        public RaftConfiguration Configuration { get; } = new() { InitialPartitions = 3 };

        public string GetLocalEndpoint() => "localhost:8080";

        public ValueTask<bool> AmILeaderQuick(int partitionId) => ValueTask.FromResult(false);

        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) => ValueTask.FromResult(false);

        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => throw WaitForLeaderError;

        public IWAL WalAdapter => throw new NotImplementedException();

        public ICommunication Communication => throw new NotImplementedException();

        public IDiscovery Discovery => throw new NotImplementedException();

        public HybridLogicalClock HybridLogicalClock => throw new NotImplementedException();

        public IRaftReadScheduler ReadScheduler => throw new NotImplementedException();

        public IRaftWalScheduler WalScheduler => throw new NotImplementedException();

        public ClusterMemberRole LocalRole => throw new NotImplementedException();

        public event Action<int>? OnRestoreStarted { add { } remove { } }

        public event Action<int>? OnRestoreFinished { add { } remove { } }

        public event Action<int, RaftLog>? OnReplicationError { add { } remove { } }

        public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add { } remove { } }

        public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add { } remove { } }

        public event Func<int, string, Task<bool>>? OnLeaderChanged { add { } remove { } }

        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add { } remove { } }

        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }

        public ClusterMembership GetMembership() => throw new NotImplementedException();

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
