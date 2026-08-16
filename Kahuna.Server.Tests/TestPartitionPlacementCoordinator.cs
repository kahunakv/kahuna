using System.Collections.Concurrent;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

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
/// Coverage for the subscription lifecycle of the placement coordinator. The coordinator both
/// subscribes to and unsubscribes from the committed-map event, so a mismatch between the two
/// would leave a disposed node still reacting to map applications — a leak that no functional
/// assertion elsewhere would catch, because the handler's work is idempotent.
/// </summary>
public sealed class TestPartitionPlacementCoordinator
{
    private const string LocalEndpoint = "node1:2070";

    private const string BannerPrefix = "Partition placement:";

    /// <summary>A legacy (empty replica set) range: hosted everywhere, so no purge is ever derived.</summary>
    private static RaftPartitionRange Range(int partitionId) => new()
    {
        PartitionId = partitionId,
        State = RaftPartitionState.Active,
        Replicas = []
    };

    /// <summary>
    /// Builds a coordinator over the stub. The key-value, lock and background-writer collaborators
    /// are only reached from the purge path, which a legacy map never triggers, so this fixture
    /// leaves them unset rather than standing up a whole node.
    /// </summary>
    private static PartitionPlacementCoordinator Build(MapStubRaft raft, CapturingKahunaLogger logger) =>
        new(raft, null!, null!,
            new KahunaDurabilityProvider(new PartitionDurabilityTracker(), new MemoryPersistenceBackend()),
            null!, logger);

    [Fact]
    public void Dispose_ReleasesBothMapSubscriptions()
    {
        MapStubRaft raft = new(LocalEndpoint, [Range(1), Range(2)]);
        CapturingKahunaLogger logger = new();

        Assert.Equal(0, raft.MapSubscriberCount);

        PartitionPlacementCoordinator coordinator = Build(raft, logger);

        // Two subscribers: the placement view's own projection rebuild, and the coordinator's
        // one-shot startup handler.
        Assert.Equal(2, raft.MapSubscriberCount);

        coordinator.Dispose();

        // Both must come off. A '-=' that does not match its '+=' unsubscribes nothing while
        // reporting success, which is exactly the failure this asserts against.
        Assert.Equal(0, raft.MapSubscriberCount);

        // And the released handlers must really be inert: applying a map after disposal does no
        // work at all (the collaborators the purge path would need are not even wired here).
        raft.ApplyMap([Range(1), Range(2), Range(3)]);

        Assert.DoesNotContain(logger.Lines, l => l.StartsWith(BannerPrefix, StringComparison.Ordinal));
    }

    [Fact]
    public void FirstMapApplication_LogsBannerOnceThenUnsubscribesItself()
    {
        MapStubRaft raft = new(LocalEndpoint, [Range(1), Range(2)]);
        CapturingKahunaLogger logger = new();

        using PartitionPlacementCoordinator coordinator = Build(raft, logger);

        raft.ApplyMap([Range(1), Range(2)]);

        Assert.Single(logger.Lines, l => l.StartsWith(BannerPrefix, StringComparison.Ordinal));

        // The startup handler releases itself on its first run, leaving only the view subscribed.
        Assert.Equal(1, raft.MapSubscriberCount);

        // Later map applications must not re-log the banner or re-run the startup sweep.
        raft.ApplyMap([Range(1), Range(2), Range(3)]);

        Assert.Single(logger.Lines, l => l.StartsWith(BannerPrefix, StringComparison.Ordinal));
    }

    /// <summary>Records Information+ lines so the banner can be asserted without a live sink.</summary>
    private sealed class CapturingKahunaLogger : ILogger<IKahuna>
    {
        public readonly ConcurrentQueue<string> Lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                Lines.Enqueue(formatter(state, exception));
        }
    }

    /// <summary>
    /// Minimal <see cref="IRaft"/> that applies committed maps and exposes how many handlers are
    /// attached to the map event, so subscription leaks are directly observable.
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

        public int MapSubscriberCount => OnPartitionMapChanged?.GetInvocationList().Length ?? 0;

        public string GetLocalEndpoint() => localEndpoint;

        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => map;

        public bool HostsPartition(int partitionId) => true;

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
