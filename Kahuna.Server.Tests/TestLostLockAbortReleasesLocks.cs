using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
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

namespace Kahuna.Server.Tests;

/// <summary>
/// A pessimistic transaction protects a read with a Shared range lock on a partition leader. At commit the
/// leader cannot confirm its leadership to a quorum — the window a node is in right after a peer fails — so
/// the lock proof fails and the commit is refused as a lost lock. The leader did not change, and the lock is
/// still in its memory.
///
/// <para>The refusal must release that lock. A release drops this node's own belief-only state and needs no
/// quorum confirmation, so the window that refuses the commit must not refuse the cleanup. When it did, the
/// lock stayed until its lease ran out, and every writer into the range waited for it.</para>
/// </summary>
public sealed class TestLostLockAbortReleasesLocks : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int LockExpiresMs = 60_000;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLostLockAbortReleasesLocks(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string BucketOf(string key) => key[..key.LastIndexOf('/')];

    private static async Task<int> LeaderIndexOf(int partition, IRaft[] rafts, CancellationToken ct)
    {
        int index = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (!await rafts[i].AmILeaderIfHosted(partition, ct))
                    continue;

                index = i;
                return true;
            }

            return false;
        }, timeoutMs: 30_000);

        return index;
    }

    private static KeyValuePair<int, string>[] FreshKeyPerPartition(KahunaManager probe, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        Dictionary<int, string> byPartition = [];

        for (int i = 0; i < 4_096 && byPartition.Count < Partitions; i++)
        {
            string candidate = $"{tag}{i}/{random}";
            int partition = probe.KeyValues.LocateDurablePartition(candidate).PartitionId;
            byPartition.TryAdd(partition, candidate);
        }

        return [.. byPartition.OrderBy(kv => kv.Key)];
    }

    private static async Task Seed(KahunaManager session, string key, CancellationToken ct)
    {
        (KeyValueResponseType seedType, _, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, seedType);
    }

    /// <summary>A pessimistic session whose commit releases its working set before it answers, as a client that
    /// does not ask for a detached release gets it.</summary>
    private static async Task<TransactionHandle> StartPessimistic(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                AsyncRelease = false,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    /// <summary>Acquires (or upgrades to) a range lock over the single key, registered with the coordinator.</summary>
    private static Task<KeyValueResponseType> TryPointRangeLock(
        KahunaManager session, TransactionHandle handle, string key, RangeLockMode mode, CancellationToken ct) =>
        RetryOnMustRetryAsync(async () =>
        {
            (KeyValueResponseType lockType, _) = await session.LocateAndTryAcquireRangeLock(
                handle.TransactionId, BucketOf(key), key, true, key, true, LockExpiresMs, KeyValueDurability.Persistent, mode, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            return lockType;
        }, t => t);

    [Fact]
    public async Task CommitRefusedAsLostLockOnAnUnchangedLeader_ReleasesTheRangeLockAtOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        List<ConfirmationOverridingRaft> confirmations = [];

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            wrapRaft: raft =>
            {
                ConfirmationOverridingRaft wrapped = new(raft);
                confirmations.Add(wrapped);
                return wrapped;
            });

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "llr");
            Assert.True(keys.Length >= 2, "the fixture needs a data partition and a coordinator partition");

            (int dataPartition, string dataKey) = (keys[0].Key, keys[0].Value);
            string coordinatorKey = keys[1].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            KahunaManager session = managers[leader];

            await Seed(session, dataKey, ct);

            TransactionHandle holder = await StartPessimistic(session, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(session, holder, dataKey, RangeLockMode.Shared, ct));

            List<KeyValueRangeLock> held = await session.KeyValues.GetRangeLocksAsync(BucketOf(dataKey));
            Assert.Contains(held, l => l.TransactionId == holder.TransactionId);

            // The leader's ack round stops completing. It still leads by its own belief, the partition's term
            // does not move, and the lock is still in its memory: only the proof of the lock is gone.
            confirmations[leader].RefuseConfirmation(dataPartition);

            long lostLockAborts = DurableTransactionMetrics.LostLockAbortsCount;

            TransactionCommitAnswer commit;
            try
            {
                commit = await session.LocateAndCommitTransaction(holder, ct);
            }
            finally
            {
                confirmations[leader].ConfirmAgain();
            }

            Assert.Equal(KeyValueResponseType.Aborted, commit.Type);
            Assert.Equal(CommitRefusalClass.LostLock, commit.Refusal);
            Assert.True(DurableTransactionMetrics.LostLockAbortsCount > lostLockAborts,
                "the commit must be refused because the lock could not be proven held, and counted as such");

            // The refused transaction holds nothing any more: the leader's lock table has no entry of it, and a
            // writer into the range is admitted at once instead of waiting for the lease to end.
            List<KeyValueRangeLock> remaining = await session.KeyValues.GetRangeLocksAsync(BucketOf(dataKey));
            Assert.DoesNotContain(remaining, l => l.TransactionId == holder.TransactionId);

            TransactionHandle writer = await StartPessimistic(session, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(session, writer, dataKey, RangeLockMode.Exclusive, ct));
            await session.LocateAndRollbackTransaction(writer, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The same window, seen from the releases themselves: an unconfirmed leader drops the lock it holds of the
    /// transaction, and answers MustRetry for a release that finds nothing of it. The second answer is what keeps
    /// a deposed node, which believes it leads just the same, from vouching for a lock that lives on the live
    /// leader.
    /// </summary>
    [Fact]
    public async Task UnconfirmedLeader_ReleasesWhatItHolds_AndRefusesToVouchForTheRest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        List<ConfirmationOverridingRaft> confirmations = [];

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            wrapRaft: raft =>
            {
                ConfirmationOverridingRaft wrapped = new(raft);
                confirmations.Add(wrapped);
                return wrapped;
            });

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "llh");
            Assert.True(keys.Length >= 2, "the fixture needs a data partition and a coordinator partition");

            (int dataPartition, string dataKey) = (keys[0].Key, keys[0].Value);
            string coordinatorKey = keys[1].Value;

            // Under the same bucket as the data key, so a range lock over it routes to the same partition.
            string unlockedKey = dataKey + "-unlocked";

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            KahunaManager session = managers[leader];

            await Seed(session, dataKey, ct);

            TransactionHandle holder = await StartPessimistic(session, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(session, holder, dataKey, RangeLockMode.Shared, ct));

            confirmations[leader].RefuseConfirmation(dataPartition);

            try
            {
                // Nothing of the transaction is on the range it never locked, nor on the key as a point lock:
                // the node cannot vouch for either and asks the caller to retry.
                Assert.Equal(KeyValueResponseType.MustRetry, await session.LocateAndTryReleaseExclusiveRangeLock(
                    holder.TransactionId, BucketOf(dataKey), unlockedKey, true, unlockedKey, true, KeyValueDurability.Persistent, ct));

                (KeyValueResponseType pointRelease, _) = await session.LocateAndTryReleaseExclusiveLock(
                    holder.TransactionId, dataKey, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.MustRetry, pointRelease);

                // The lock it does hold is dropped.
                Assert.Equal(KeyValueResponseType.Unlocked, await session.LocateAndTryReleaseExclusiveRangeLock(
                    holder.TransactionId, BucketOf(dataKey), dataKey, true, dataKey, true, KeyValueDurability.Persistent, ct));
            }
            finally
            {
                confirmations[leader].ConfirmAgain();
            }

            List<KeyValueRangeLock> remaining = await session.KeyValues.GetRangeLocksAsync(BucketOf(dataKey));
            Assert.DoesNotContain(remaining, l => l.TransactionId == holder.TransactionId);

            await session.LocateAndRollbackTransaction(holder, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// An <see cref="IRaft"/> that delegates every member to a real <see cref="RaftManager"/> except
    /// <see cref="ConfirmLeadershipAsync"/>, which answers false for one chosen partition. The real instance
    /// keeps leading, heartbeating and replicating; only Kahuna's view of the quorum confirmation is faulted,
    /// which is the window a leader is in while its ack round waits on a peer that just failed.
    /// </summary>
    private sealed class ConfirmationOverridingRaft(IRaft inner) : IRaft
    {
        private int refusedPartition = -1;

        /// <summary>Leadership confirmations for <paramref name="partitionId"/> answer false until <see cref="ConfirmAgain"/>.</summary>
        public void RefuseConfirmation(int partitionId) => Volatile.Write(ref refusedPartition, partitionId);

        public void ConfirmAgain() => Volatile.Write(ref refusedPartition, -1);

        public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) =>
            Volatile.Read(ref refusedPartition) == partitionId
                ? ValueTask.FromResult(false)
                : inner.ConfirmLeadershipAsync(partitionId, cancellationToken);

        public bool Joined => inner.Joined;
        public IWAL WalAdapter => inner.WalAdapter;
        public ICommunication Communication => inner.Communication;
        public IDiscovery Discovery => inner.Discovery;
        public RaftConfiguration Configuration => inner.Configuration;
        public HybridLogicalClock HybridLogicalClock => inner.HybridLogicalClock;
        public IRaftReadScheduler ReadScheduler => inner.ReadScheduler;
        public IRaftWalScheduler WalScheduler => inner.WalScheduler;
        public bool IsInitialized => inner.IsInitialized;
        public ClusterMemberRole LocalRole => inner.LocalRole;

        public event Action<int>? OnRestoreStarted { add => inner.OnRestoreStarted += value; remove => inner.OnRestoreStarted -= value; }
        public event Action<int>? OnRestoreFinished { add => inner.OnRestoreFinished += value; remove => inner.OnRestoreFinished -= value; }
        public event Action<int, RaftLog>? OnReplicationError { add => inner.OnReplicationError += value; remove => inner.OnReplicationError -= value; }
        public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add => inner.OnLogRestored += value; remove => inner.OnLogRestored -= value; }
        public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add => inner.OnReplicationReceived += value; remove => inner.OnReplicationReceived -= value; }
        public event Func<int, string, Task<bool>>? OnLeaderChanged { add => inner.OnLeaderChanged += value; remove => inner.OnLeaderChanged -= value; }
        public event Func<int, long, Task>? OnLeadershipLost { add => inner.OnLeadershipLost += value; remove => inner.OnLeadershipLost -= value; }
        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add => inner.OnPartitionMapChanged += value; remove => inner.OnPartitionMapChanged -= value; }
        public event Action<ClusterMembership>? OnMembershipChanged { add => inner.OnMembershipChanged += value; remove => inner.OnMembershipChanged -= value; }

        public ClusterMembership GetMembership() => inner.GetMembership();
        public Task JoinCluster(CancellationToken cancellationToken = default) => inner.JoinCluster(cancellationToken);
        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => inner.JoinCluster(seeds, cancellationToken);
        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => inner.LeaveCluster(dispose, cancellationToken);
        public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default) => inner.RequestLeaveAsync(cancellationToken);
        public Task UpdateNodes() => inner.UpdateNodes();
        public IList<RaftNode> GetNodes() => inner.GetNodes();
        public HLCTimestamp GetLastNodeActivity(string endpoint) => inner.GetLastNodeActivity(endpoint);
        public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => inner.GetActiveNodes(within);
        public Task Handshake(HandshakeRequest request) => inner.Handshake(request);
        public void RequestVote(RequestVotesRequest request) => inner.RequestVote(request);
        public void Vote(VoteRequest request) => inner.Vote(request);
        public void AppendLogs(AppendLogsRequest request) => inner.AppendLogs(request);
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) => inner.CompleteAppendLogs(request);
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, long expectedTerm = 0, CancellationToken cancellationToken = default) => inner.ReplicateLogs(partitionId, type, data, autoCommit, expectedGeneration, expectedTerm, cancellationToken);
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, long expectedTerm = 0, CancellationToken cancellationToken = default) => inner.ReplicateLogs(partitionId, type, logs, autoCommit, expectedGeneration, expectedTerm, cancellationToken);
        public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default) => inner.ReplicateEntries(partitionId, entries, cancellationToken);
        public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => inner.ReplicateCheckpoint(partitionId, cancellationToken);
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => inner.CommitLogs(partitionId, ticketId, cancellationToken);
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => inner.RollbackLogs(partitionId, ticketId, cancellationToken);
        public void SetMinRetainIndex(int partitionId, long index) => inner.SetMinRetainIndex(partitionId, index);
        public IDisposable AcquireRetentionHold(int partitionId, long index) => inner.AcquireRetentionHold(partitionId, index);
        public string GetLocalEndpoint() => inner.GetLocalEndpoint();
        public int GetLocalNodeId() => inner.GetLocalNodeId();
        public string GetLocalNodeName() => inner.GetLocalNodeName();
        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) => inner.GetFollowerLagAsync(partitionId, followerEndpoint);
        public ValueTask<bool> AmILeaderQuick(int partitionId) => inner.AmILeaderQuick(partitionId);
        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => inner.AmILeader(partitionId, cancellationToken);
        public ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default) => inner.ConfirmLocalApplicationAsync(partitionId, cancellationToken);
        public IReadOnlyList<RaftReplica> GetPartitionReplicas(int partitionId) => inner.GetPartitionReplicas(partitionId);
        public int GetEffectiveReplicationFactor(int partitionId) => inner.GetEffectiveReplicationFactor(partitionId);
        public Task<RaftPartitionLifecycleResult> SetReplicationFactorAsync(int partitionId, int replicationFactor, CancellationToken ct = default) => inner.SetReplicationFactorAsync(partitionId, replicationFactor, ct);
        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => inner.WaitForLeader(partitionId, cancellationToken);
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => inner.WaitForLeaderStableAsync(partitionId, minStableFor, cancellationToken);
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, TimeSpan timeout, CancellationToken cancellationToken = default) => inner.WaitForLeaderStableAsync(partitionId, minStableFor, timeout, cancellationToken);
        public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => inner.ForceLeaderForTestingAsync(partitionId, cancellationToken);
        public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => inner.StepDownAsync(partitionId, cancellationToken);
        public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => inner.TransferLeadershipAsync(partitionId, targetEndpoint, cancellationToken);
        public Task<RaftOperationStatus> RequestReseedAsync(int partitionId, CancellationToken cancellationToken = default) => inner.RequestReseedAsync(partitionId, cancellationToken);
        public RaftOperationStatus SetCandidacyWithheld(int partitionId, bool withheld) => inner.SetCandidacyWithheld(partitionId, withheld);
        public bool IsCandidacyWithheld(int partitionId) => inner.IsCandidacyWithheld(partitionId);
        public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => inner.SuspendHeartbeatsAsync(partitionId, cancellationToken);
        public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => inner.ResumeHeartbeatsAsync(partitionId, cancellationToken);
        public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => inner.CreatePartitionAsync(partitionId, mode, hashRange, ct);
        public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => inner.RemovePartitionAsync(partitionId, ct);
        public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => inner.SplitPartitionAsync(sourcePartitionId, targetPartitionId, plan, ct);
        public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => inner.MergePartitionsAsync(survivorPartitionId, sourcePartitionId, plan, ct);
        public long GetPartitionGeneration(int partitionId) => inner.GetPartitionGeneration(partitionId);
        public long GetPartitionTerm(int partitionId) => inner.GetPartitionTerm(partitionId);
        public double GetPartitionLogOpsPerSecond(int partitionId) => inner.GetPartitionLogOpsPerSecond(partitionId);
        public int GetPartitionWalQueueDepth(int partitionId) => inner.GetPartitionWalQueueDepth(partitionId);
        public double GetPartitionCommitWaitMs(int partitionId) => inner.GetPartitionCommitWaitMs(partitionId);
        public long GetCommitIndex(int partitionId) => inner.GetCommitIndex(partitionId);
        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => inner.GetPartitionMap();
        public int GetPartitionKey(string partitionKey) => inner.GetPartitionKey(partitionKey);
        public int GetPrefixPartitionKey(string prefixPartitionKey) => inner.GetPrefixPartitionKey(prefixPartitionKey);
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => inner.RegisterStateMachineTransfer(transfer);
        public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) => inner.RegisterSystemStateTransfer(transfer);
        public void RegisterPartitionStateTransfer(IRaftPartitionStateTransfer? transfer) => inner.RegisterPartitionStateTransfer(transfer);
        public long GetStaleProposedSkippedCount(int partitionId) => inner.GetStaleProposedSkippedCount(partitionId);
        public IReadOnlyList<RaftSnapshotStatus> GetSnapshotStatuses(int partitionId) => inner.GetSnapshotStatuses(partitionId);
        public IReadOnlyList<RaftBackfillStatus> GetBackfillStatuses(int partitionId) => inner.GetBackfillStatuses(partitionId);
        public bool HostsPartition(int partitionId) => inner.HostsPartition(partitionId);
        public string? GetPartitionLeaderHint(int partitionId) => inner.GetPartitionLeaderHint(partitionId);
        public IDisposable HoldCommittedProposalRepliesForTesting(int partitionId, Action<HeldProposalReply> onHeld) => inner.HoldCommittedProposalRepliesForTesting(partitionId, onHeld);
        public IDisposable SetSnapshotInstallGateForTesting(int partitionId, SnapshotInstallPhase phase, Func<SnapshotInstallSignal, ValueTask> gate) => inner.SetSnapshotInstallGateForTesting(partitionId, phase, gate);
        public Task<RaftOperationStatus> HoldConsumerAppliesForTesting(int partitionId, CancellationToken cancellationToken = default) => inner.HoldConsumerAppliesForTesting(partitionId, cancellationToken);
        public Task<RaftOperationStatus> ResumeConsumerAppliesForTesting(int partitionId, CancellationToken cancellationToken = default) => inner.ResumeConsumerAppliesForTesting(partitionId, cancellationToken);
    }
}
