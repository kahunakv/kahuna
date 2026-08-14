
using System.Text;
using Kahuna.Communication.External;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Rest;
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

using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for decommissioning a running node over the API. Removing a node by stopping it and
/// waiting for failure detection costs the suspicion timeout plus the eviction grace per node and
/// logs a planned removal as a failure; committing the removal while the node is still up replaces
/// that with one consensus round trip and a verdict the caller can act on. The verdict is what
/// these tests pin down: the removal must be reported as committed only when it committed, the
/// last-voter refusal must be permanent and must leave the node serving, and a node that left must
/// not quietly re-admit itself while it keeps running.
/// </summary>
public sealed class TestClusterLeave : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestClusterLeave(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Theory, CombinatorialData]
    public async Task Leave_ThreeNodeCluster_CommitsAndSurvivorsKeepServing(
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

            LeaveClusterResult result = await ClusterLeave.ExecuteAsync(
                raft3, TestContext.Current.CancellationToken);

            Assert.Equal(LeaveClusterOutcome.Committed, result.Outcome);
            Assert.True(result.MembershipVersion > versionBefore,
                $"Expected a roster version above {versionBefore}, got {result.MembershipVersion}.");
            Assert.Equal(StatusCodes.Status200OK, ClusterLeave.ToStatusCode(result.Outcome));

            KahunaClusterLeaveResponse response = ClusterLeave.ToResponse(result);
            Assert.True(response.Left);
            Assert.False(response.Retryable);

            // The survivors drop the leaver from their committed roster.
            await WaitUntilAsync(() =>
            {
                ClusterMembership m = raft1.GetMembership();
                return m.MembershipVersion > versionBefore
                    && m.Members.Count(x =>
                        x.Role != ClusterMemberRole.Leaving &&
                        x.Role != ClusterMemberRole.NotMember) == 2;
            }, timeoutMs: 30_000);

            Assert.DoesNotContain(raft1.GetMembership().Members, m =>
                m.Endpoint == "localhost:8003" && m.Role == ClusterMemberRole.Voter);

            // …and keep serving reads and writes throughout.
            string key = $"leave-{Guid.NewGuid():N}";
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("after-leave"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Ephemeral,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                KeyValueDurability.Ephemeral, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry);

            // The departed node is still up — that is the whole point of separating leaving from
            // stopping — so a repeat request is answered, and answered idempotently.
            LeaveClusterResult again = await ClusterLeave.ExecuteAsync(
                raft3, TestContext.Current.CancellationToken);

            Assert.Equal(LeaveClusterOutcome.NotAMember, again.Outcome);
            Assert.True(again.Left);
            Assert.Equal(StatusCodes.Status200OK, ClusterLeave.ToStatusCode(again.Outcome));

            // Auto-rejoin exists to rescue a node evicted while it was down; it must not undo a
            // decommission the operator asked for.
            await Task.Delay(3_000, TestContext.Current.CancellationToken);
            Assert.DoesNotContain(raft1.GetMembership().Members, m => m.Endpoint == "localhost:8003");
        }
        finally
        {
            await LeaveClusterSingle(raft1);
            await LeaveClusterSingle(raft2);
            try { await LeaveClusterSingle(raft3); } catch { }
        }
    }

    /// <summary>
    /// The last voter cannot be removed — the cluster would be left unable to commit anything. The
    /// refusal must be permanent (409, not retryable) and must cost the node nothing: it stays a
    /// Voter and goes on serving, rather than parking in Leaving with elections suppressed.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Leave_LastVoter_IsRefusedAndNodeKeepsServing(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(3)] int partitions)
    {
        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(walStorage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Shrink to a single voter first — the refusal only exists at that boundary.
            await raft3.LeaveCluster(dispose: false, TestContext.Current.CancellationToken);
            await raft2.LeaveCluster(dispose: false, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() =>
                raft1.GetMembership().Members.Count(x => x.Role == ClusterMemberRole.Voter) == 1,
                timeoutMs: 30_000);

            // The refusal is the leader's verdict, so the sole survivor has to have taken over the
            // system partition after the other two left before its own request can be answered.
            await WaitUntilAsync(async () => await raft1.AmILeaderQuick(0).AsTask(), timeoutMs: 30_000);

            long versionBefore = raft1.GetMembership().MembershipVersion;

            LeaveClusterResult result = await ClusterLeave.ExecuteAsync(
                raft1, TestContext.Current.CancellationToken);

            Assert.Equal(LeaveClusterOutcome.RefusedInsufficientVoters, result.Outcome);
            Assert.False(result.Left);
            Assert.True(result.Terminal);

            KahunaClusterLeaveResponse response = ClusterLeave.ToResponse(result);
            Assert.Equal(StatusCodes.Status409Conflict, ClusterLeave.ToStatusCode(result.Outcome));
            Assert.False(response.Retryable);
            Assert.False(response.Left);

            // Nothing was committed and the node did not give up its role: a refused leave must
            // leave the node campaigning and serving, not parked in Leaving.
            Assert.Equal(versionBefore, raft1.GetMembership().MembershipVersion);
            Assert.Equal(ClusterMemberRole.Voter, raft1.LocalRole);
            Assert.Contains(raft1.GetMembership().Members, m =>
                m.Endpoint == "localhost:8001" && m.Role == ClusterMemberRole.Voter);

            string key = $"refused-{Guid.NewGuid():N}";
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("still-serving"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Ephemeral,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);
        }
        finally
        {
            try { await LeaveClusterSingle(raft1); } catch { }
            try { await LeaveClusterSingle(raft2); } catch { }
            try { await LeaveClusterSingle(raft3); } catch { }
        }
    }

    [Fact]
    public async Task Leave_UninitializedNode_ReportsNotInitialized()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        // Never started: the node has no committed roster, so it has no membership entry to remove.
        LeaveClusterResult result = await ClusterLeave.ExecuteAsync(
            node.Raft, TestContext.Current.CancellationToken);

        Assert.Equal(LeaveClusterOutcome.NotInitialized, result.Outcome);
        Assert.False(result.Left);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ClusterLeave.ToStatusCode(result.Outcome));
        Assert.True(ClusterLeave.ToResponse(result).Retryable);
    }

    /// <summary>
    /// A caller sequencing removals branches on this answer, so a consensus-layer failure must
    /// arrive as an outcome rather than as an exception it cannot classify.
    /// </summary>
    [Fact]
    public async Task Leave_WhenConsensusThrows_ReportsOutcomeInsteadOfFailing()
    {
        LeaveClusterResult result = await ClusterLeave.ExecuteAsync(
            new ThrowingRaft(new InvalidOperationException("consensus is tearing down")),
            TestContext.Current.CancellationToken);

        Assert.Equal(LeaveClusterOutcome.NotInitialized, result.Outcome);
        Assert.False(result.Left);
    }

    [Fact]
    public async Task Leave_WhenCallerCancels_ReportsTimeoutRatherThanSuccess()
    {
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        LeaveClusterResult result = await ClusterLeave.ExecuteAsync(
            new ThrowingRaft(new OperationCanceledException()), cts.Token);

        // Unresolved, never mistaken for a completed removal: the caller must re-read the roster.
        Assert.Equal(LeaveClusterOutcome.Timeout, result.Outcome);
        Assert.False(result.Left);
        Assert.Equal(StatusCodes.Status504GatewayTimeout, ClusterLeave.ToStatusCode(result.Outcome));
        Assert.True(ClusterLeave.ToResponse(result).Retryable);
    }

    [Theory]
    [InlineData(LeaveClusterOutcome.Committed, StatusCodes.Status200OK, true, false)]
    [InlineData(LeaveClusterOutcome.NotAMember, StatusCodes.Status200OK, true, false)]
    [InlineData(LeaveClusterOutcome.RefusedInsufficientVoters, StatusCodes.Status409Conflict, false, false)]
    [InlineData(LeaveClusterOutcome.NotInitialized, StatusCodes.Status503ServiceUnavailable, false, true)]
    [InlineData(LeaveClusterOutcome.NoLeader, StatusCodes.Status503ServiceUnavailable, false, true)]
    [InlineData(LeaveClusterOutcome.Timeout, StatusCodes.Status504GatewayTimeout, false, true)]
    public void OutcomeMapping_IsStableAcrossTransports(
        LeaveClusterOutcome outcome, int expectedStatus, bool expectedLeft, bool expectedRetryable)
    {
        LeaveClusterResult result = new(outcome, 42);
        KahunaClusterLeaveResponse response = ClusterLeave.ToResponse(result);

        Assert.Equal(expectedStatus, ClusterLeave.ToStatusCode(outcome));
        Assert.Equal(expectedLeft, response.Left);
        Assert.Equal(expectedRetryable, response.Retryable);
        Assert.Equal(outcome.ToString(), response.Outcome);
        Assert.Equal(42, response.MembershipVersion);
        Assert.NotEmpty(response.Reason);
    }

    /// <summary>
    /// An <see cref="IRaft"/> whose leave request fails the way a node whose consensus layer is
    /// already tearing down would; everything else throws so an unexpected dependency is visible.
    /// </summary>
    private sealed class ThrowingRaft : IRaft
    {
        private readonly Exception failure;

        public ThrowingRaft(Exception failure) { this.failure = failure; }

        public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default) =>
            Task.FromException<LeaveClusterResult>(failure);

        public ClusterMembership GetMembership() => new();

        public bool IsInitialized => true;
        public bool Joined => true;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public RaftConfiguration Configuration { get; } = new();
        public string GetLocalEndpoint() => "localhost:8080";

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
        public long GetCommitIndex(int partitionId) => throw new NotImplementedException();
        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => throw new NotImplementedException();
        public int GetPartitionKey(string partitionKey) => throw new NotImplementedException();
        public int GetPrefixPartitionKey(string prefixPartitionKey) => throw new NotImplementedException();
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => throw new NotImplementedException();
        public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) => throw new NotImplementedException();

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
