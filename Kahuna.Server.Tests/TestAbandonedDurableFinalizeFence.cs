using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end coverage of the presumed-abort fence for an abandoned durable finalize whose decision proposal
/// stalled in flight. A replication timeout is indeterminate — the proposal (for a single-participant
/// transaction, one atomic [record init + prepare + commit decision] bundle) can still commit after a partition
/// heals, and its propose-time attempt HLC passes the record deadline gate whenever it applies. The reaper must
/// therefore never report a definite rollback for such a session without first installing a durable Abort
/// through the record CAS: if the abort wins, the late bundle is permanently rejected; if the stalled commit
/// already won, the truthful outcome is Committed. Without the fence, the reaper's fabricated RolledBack is
/// later contradicted by the stalled commit materializing — an aborted-read (G1a) anomaly.
///
/// The stall is deterministic: the write-batch executor is decorated to capture-and-fail every batch carrying a
/// transaction-record entry, exactly what a coordinator sees when its proposal times out in a partition; the
/// captured batch is later replayed through the real executor to model the healed partition committing it.
/// </summary>
public sealed class TestAbandonedDurableFinalizeFence
{
    private readonly ILoggerFactory loggerFactory;

    public TestAbandonedDurableFinalizeFence(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// While armed, fails (and captures) every batch that carries a transaction-record entry — the record
    /// init/prepare/decision proposals of a durable finalize — modeling a proposal whose acknowledgement was
    /// lost to a partition. Everything else passes through. <see cref="ReplayStalledAsync"/> then proposes the
    /// captured batches through the real executor, modeling the healed partition committing the stalled entry.
    /// </summary>
    private sealed class StallingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private readonly List<(int Partition, RaftProposalEntry[] Entries)> stalled = [];
        private volatile bool armed;

        public StallingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public bool Armed { set => armed = value; }

        public int StalledBatches { get { lock (stalled) return stalled.Count; } }

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            if (armed && entries.Any(static e => e.Type == ReplicationTypes.TransactionRecord))
            {
                lock (stalled)
                    stalled.Add((partitionId, [.. entries]));

                List<RaftEntryResult> failed = new(entries.Count);
                for (int i = 0; i < entries.Count; i++)
                    failed.Add(new RaftEntryResult(RaftOperationStatus.Errored, -1, HLCTimestamp.Zero));

                return Task.FromResult(new RaftBatchReplicationResult(false, RaftOperationStatus.Errored, HLCTimestamp.Zero, failed));
            }

            return inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }

        public async Task ReplayStalledAsync(KahunaManager kahuna, CancellationToken ct)
        {
            List<(int Partition, RaftProposalEntry[] Entries)> toReplay;
            lock (stalled)
            {
                toReplay = [.. stalled];
                stalled.Clear();
            }

            foreach ((int partition, RaftProposalEntry[] entries) in toReplay)
            {
                await inner.ReplicateAsync(partition, entries, ct);

                // The leader applies scheduler-driven proposals on the scheduler's completion path, which this
                // raw replay bypasses — apply each committed delta to the stores exactly as the replication
                // callback / WAL replay does on a follower or a restarted node (where the stalled entry would
                // really surface).
                foreach (RaftProposalEntry entry in entries)
                {
                    RaftLog log = new() { LogType = entry.Type, LogData = entry.Data };
                    if (entry.Type == ReplicationTypes.TransactionRecord)
                        kahuna.DurableTransactionRecordStore.Replicate(partition, log);
                    else if (entry.Type == ReplicationTypes.PreparedIntent)
                        kahuna.DurablePreparedIntentStore.ApplyDeltaAckPrepares(log);
                }
            }
        }
    }

    private async Task<(EmbeddedKahunaNode Node, StallingExecutor Executor)> StartNode(string leaderKey, CancellationToken ct)
    {
        StallingExecutor? stalling = null;
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            // Synchronous settlement keeps the fence's resolution inline, so post-fence store state is
            // deterministic to assert.
            DurableDeferredSettlement = false,
            WriteBatchExecutorDecorator = inner => stalling = new StallingExecutor(inner)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(leaderKey, ct);
        return (node, stalling!);
    }

    private static async Task<TransactionHandle> StartSessionWithStalledCommit(
        EmbeddedKahunaNode node, StallingExecutor executor, string key, CancellationToken ct)
    {
        // A short session timeout so the reap deadline (Timeout + grace) is reachable within the test.
        (KeyValueResponseType started, TransactionHandle handle) = await ((KahunaManager)node.Kahuna).LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                Locking = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 1000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, started);

        (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "stalled-value"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, set);

        // Stall the durable decision: the commit's [init + prepare + commit] proposal is captured and failed,
        // exactly as a coordinator experiences a replication timeout. The commit must answer the retryable
        // MustRetry — the outcome is genuinely indeterminate — never a definite Aborted.
        executor.Armed = true;
        (KeyValueResponseType commit, _) = await ((KahunaManager)node.Kahuna).LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, commit);
        Assert.True(executor.StalledBatches > 0, "the durable decision proposal must have been stalled");

        return handle;
    }

    /// <summary>
    /// The G1a shape, prevented: the reaper reclaims the abandoned session, but while replication is still down
    /// it must stay indeterminate (MustRetry), and once it can fence it installs a durable Abort BEFORE reporting
    /// RolledBack — so when the stalled commit bundle finally applies, it is rejected by the tombstoned record
    /// and the write never becomes visible to any reader.
    /// </summary>
    [Fact]
    public async Task ReapedStalledCommit_FencesAbortFirst_SoTheLateBundleNeverBecomesVisible()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string key = "fence/abort/k1";

        (EmbeddedKahunaNode node, StallingExecutor executor) = await StartNode(key, ct);
        await using EmbeddedKahunaNode ownedNode = node;
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        TransactionHandle handle = await StartSessionWithStalledCommit(node, executor, key, ct);

        // Past the reap deadline, with replication still down: the reaper claims the session but cannot install
        // the fence, so nothing terminal may be reported — a concurrent commit sees MustRetry, not Aborted.
        await Task.Delay(TransactionCoordinator.ReapGraceMs + 2500, ct);
        await kahuna.ReapAbandonedSessions();

        (KeyValueResponseType whileUnfenced, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, whileUnfenced);

        // Replication heals (but the stalled proposal has NOT applied yet). The next sweep installs the durable
        // Abort tombstone and only then reports the rollback.
        executor.Armed = false;
        await kahuna.ReapAbandonedSessions();

        TransactionRecord? record = kahuna.DurableTransactionRecordStore.Get(handle.TransactionId, 1);
        Assert.NotNull(record);
        Assert.Equal(TransactionDecision.Abort, record!.Decision);

        (KeyValueResponseType afterFence, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.RolledBack, afterFence);

        // The healed partition now commits the stalled bundle — the moment that produced the Jepsen G1a. The
        // terminal abort must reject its late commit decision, and once recovery clears the orphan prepared
        // intent the write is not visible to any reader.
        await executor.ReplayStalledAsync(kahuna, ct);

        record = kahuna.DurableTransactionRecordStore.Get(handle.TransactionId, 1);
        Assert.NotNull(record);
        Assert.Equal(TransactionDecision.Abort, record!.Decision);

        await WaitUntil(async () =>
        {
            await kahuna.KeyValues.RecoverPreparedIntents(ct);
            return kahuna.DurablePreparedIntentStore.Count == 0;
        }, ct);

        (KeyValueResponseType read, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.True(read == KeyValueResponseType.DoesNotExist || entry?.Value is null,
            $"aborted transaction's write must not be visible (read={read})");
    }

    /// <summary>
    /// The truthful-commit side of the fence: when the stalled bundle applies BEFORE the reaper acts, the
    /// record is already durably Commit, the fence's abort loses the CAS, and the reaper must report — and
    /// retain — Committed, never a rollback the record contradicts. The committed value becomes visible.
    /// </summary>
    [Fact]
    public async Task ReapedStalledCommit_WhoseBundleAlreadyApplied_IsReportedCommitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string key = "fence/commit/k1";

        (EmbeddedKahunaNode node, StallingExecutor executor) = await StartNode(key, ct);
        await using EmbeddedKahunaNode ownedNode = node;
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        TransactionHandle handle = await StartSessionWithStalledCommit(node, executor, key, ct);

        // The partition heals and the stalled [init + prepare + commit] bundle applies — the transaction is now
        // durably committed, though its coordinator never learned it.
        executor.Armed = false;
        await executor.ReplayStalledAsync(kahuna, ct);

        TransactionRecord? record = kahuna.DurableTransactionRecordStore.Get(handle.TransactionId, 1);
        Assert.NotNull(record);
        Assert.Equal(TransactionDecision.Commit, record!.Decision);

        // The reaper reclaims the abandoned session: its fence abort is rejected by the committed record, so it
        // must retain Committed — a duplicate commit replays the truthful outcome.
        await Task.Delay(TransactionCoordinator.ReapGraceMs + 2500, ct);
        await kahuna.ReapAbandonedSessions();

        (KeyValueResponseType replay, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, replay);

        // And the committed value is (or becomes, once recovery settles the intent) visible.
        await WaitUntil(async () =>
        {
            await kahuna.KeyValues.RecoverPreparedIntents(ct);
            (KeyValueResponseType read, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            return read == KeyValueResponseType.Get && entry?.Value is not null &&
                   entry.Value.AsSpan().SequenceEqual("stalled-value"u8);
        }, ct);
    }

    private static async Task WaitUntil(Func<Task<bool>> predicate, CancellationToken ct, int timeoutMs = 30_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (await predicate()) return;
            await Task.Delay(100, ct);
        }
        Assert.True(await predicate(), "condition not met in time");
    }
}
