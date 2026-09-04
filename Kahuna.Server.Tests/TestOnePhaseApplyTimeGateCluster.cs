using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The one-phase bundled commit with apply-time validation on a three-node in-memory cluster: which
/// transactions take the bundle under <c>OnePhaseApplyTimeValidation</c>, and what a bundle that stalls and
/// applies after a competitor moved its base or a read decides — on every replica, deterministically, with a
/// truthful outcome for the client.
///
/// <para>The stall and the heal are modeled as in <see cref="TestOnePhaseBundledCommitGate"/>: a per-node tap
/// over the write aggregator's batch executor captures-and-fails the record-carrying batches while armed (a
/// proposal whose acknowledgement was lost), then replays them through the real executor immediately ahead of
/// the next record-carrying batch (the healed partition delivering the stalled entry in log order). The tap
/// also counts the shapes of the batches it sees, which is how a test tells a one-phase bundle
/// ([record init, prepare, decision]) from a two-phase anchor bundle ([record init, prepare]) without reading
/// process-global counters that concurrent test classes also move.</para>
/// </summary>
public sealed class TestOnePhaseApplyTimeGateCluster : BaseCluster
{
    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestOnePhaseApplyTimeGateCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// Per-node executor tap. Counts one-phase bundles and two-phase anchor bundles; while <see cref="Armed"/>,
    /// captures and fails every record-carrying batch; with <see cref="InjectStalledBeforeNext"/> set, replays
    /// every captured batch through the real executor ahead of the next record-carrying batch and applies the
    /// replayed entries to this node's stores exactly as the replication callback would (the raw replay bypasses
    /// the scheduler's completion path, which is the leader's apply owner).
    /// </summary>
    private sealed class BatchTap : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private readonly List<(int Partition, RaftProposalEntry[] Entries)> stalled = [];
        private volatile bool armed;
        private volatile bool injectBeforeNext;
        private int onePhaseBundles;
        private int twoPhaseAnchorBundles;

        public BatchTap(IPartitionBatchExecutor inner) => this.inner = inner;

        public KahunaManager? Kahuna { get; set; }

        public bool Armed { set => armed = value; }

        public bool InjectStalledBeforeNext { set => injectBeforeNext = value; }

        public int StalledBatches { get { lock (stalled) return stalled.Count; } }

        public int OnePhaseBundles => Volatile.Read(ref onePhaseBundles);

        public int TwoPhaseAnchorBundles => Volatile.Read(ref twoPhaseAnchorBundles);

        public void ResetCounts()
        {
            Interlocked.Exchange(ref onePhaseBundles, 0);
            Interlocked.Exchange(ref twoPhaseAnchorBundles, 0);
        }

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            bool carriesRecord = false;
            foreach (RaftProposalEntry entry in entries)
                carriesRecord |= entry.Type == ReplicationTypes.TransactionRecord;

            if (entries.Count == 3 && entries[0].Type == ReplicationTypes.TransactionRecord
                && entries[1].Type == ReplicationTypes.PreparedIntent && entries[2].Type == ReplicationTypes.TransactionRecord)
                Interlocked.Increment(ref onePhaseBundles);
            else if (entries.Count == 2 && entries[0].Type == ReplicationTypes.TransactionRecord && entries[1].Type == ReplicationTypes.PreparedIntent)
                Interlocked.Increment(ref twoPhaseAnchorBundles);

            if (armed && carriesRecord)
            {
                lock (stalled)
                    stalled.Add((partitionId, [.. entries]));

                List<RaftEntryResult> failed = new(entries.Count);
                for (int i = 0; i < entries.Count; i++)
                    failed.Add(new RaftEntryResult(RaftOperationStatus.Errored, -1, HLCTimestamp.Zero));

                return new RaftBatchReplicationResult(false, RaftOperationStatus.Errored, HLCTimestamp.Zero, failed);
            }

            if (injectBeforeNext && carriesRecord)
            {
                injectBeforeNext = false;
                await ReplayStalledAsync(cancellationToken);
            }

            return await inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }

        private async Task ReplayStalledAsync(CancellationToken ct)
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

                foreach (RaftProposalEntry entry in entries)
                {
                    RaftLog log = new() { LogType = entry.Type, LogData = entry.Data };
                    if (entry.Type == ReplicationTypes.TransactionRecord)
                        Kahuna!.DurableTransactionRecordStore.Replicate(partition, log);
                    else if (entry.Type == ReplicationTypes.PreparedIntent)
                        Kahuna!.DurablePreparedIntentStore.ApplyDeltaAckPrepares(partition, log);
                }
            }
        }
    }

    private sealed class Cluster
    {
        public required IRaft[] Rafts;
        public required KahunaManager[] Managers;
        public required BatchTap[] Taps;

        public async Task<int> LeaderIndexOf(int partition, CancellationToken ct)
        {
            while (true)
            {
                for (int i = 0; i < Rafts.Length; i++)
                    if (await Rafts[i].AmILeaderIfHosted(partition, ct))
                        return i;

                await Task.Delay(50, ct);
            }
        }

        public Task Leave() => LeaveCluster(Rafts[0], Rafts[1], Rafts[2]);
    }

    private async Task<Cluster> Assemble(bool applyTimeValidation, Action<Configuration.KahunaConfiguration>? configure = null)
    {
        BatchTap?[] taps = new BatchTap?[3];

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, raftLogger, kahunaLogger,
                configure: config =>
                {
                    config.OnePhaseApplyTimeValidation = applyTimeValidation;
                    // Short staged-write intent lease so a competitor can slip in behind a stalled proposal
                    // after a small delay — the same lapse a paused or killed coordinator suffers.
                    config.StagedWriteIntentLeaseMs = 200;
                    configure?.Invoke(config);
                },
                decorateWriteBatchExecutor: nodeId => inner => taps[nodeId - 1] = new BatchTap(inner));

        Cluster cluster = new()
        {
            Rafts = [raft1, raft2, raft3],
            Managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3],
            Taps = [taps[0]!, taps[1]!, taps[2]!]
        };

        for (int i = 0; i < 3; i++)
            cluster.Taps[i].Kahuna = cluster.Managers[i];

        return cluster;
    }

    private static int PartitionOf(KahunaManager manager, string key) => manager.KeyValues.LocateDurablePartition(key).PartitionId;

    /// <summary>A fresh key routed to (or, with <paramref name="on"/> false, away from) <paramref name="partition"/>.
    /// Hash routing hashes the key space (the prefix before the last '/'), so the candidates vary the key
    /// space itself: <c>{prefix}{i}/{random}</c>.</summary>
    private static string KeyRoutedTo(KahunaManager manager, string prefix, int partition, bool on = true)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        for (int i = 0; i < 4_096; i++)
        {
            string candidate = $"{prefix}{i}/{random}";
            if ((PartitionOf(manager, candidate) == partition) == on)
                return candidate;
        }

        throw new InvalidOperationException($"no key space under {prefix} routes {(on ? "to" : "away from")} partition {partition}");
    }

    private static async Task SeedDurable(Cluster cluster, string key, string value)
    {
        KeyValueTransactionResult seeded = await RetryOnMustRetry(
            cluster.Managers[0], Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '{value}' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, seeded.Type);

        int partition = PartitionOf(cluster.Managers[0], key);
        foreach (KahunaManager manager in cluster.Managers)
            await WaitUntilAsync(() => manager.DurablePreparedIntentStore.TryGetLedgerHead(partition, key, out _, out _, out _));
    }

    private static async Task<long> LedgerHeadRevision(KahunaManager manager, int partition, string key)
    {
        long revision = -1;
        await WaitUntilAsync(() => manager.DurablePreparedIntentStore.TryGetLedgerHead(partition, key, out revision, out _, out _));
        return revision;
    }

    private static async Task<(TransactionHandle Handle, long BaseRevision)> StartAndRead(
        KahunaManager coordinator, string coordinatorKey, string readKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await coordinator.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await coordinator.LocateAndTryGetValue(
            handle.TransactionId, readKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, readType);

        return (handle, entry?.Revision ?? -1);
    }

    private static async Task Write(KahunaManager coordinator, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await coordinator.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    /// <summary>Drives a retryable commit to its terminal answer, letting recovery settle stalled intents.</summary>
    private static async Task<KeyValueResponseType> CommitToTerminal(KahunaManager coordinator, TransactionHandle handle, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await coordinator.LocateAndCommitTransaction(handle, ct);
        for (int attempt = 0; attempt < 50 && type == KeyValueResponseType.MustRetry; attempt++)
        {
            await Task.Delay(200, ct);
            await coordinator.KeyValues.RecoverPreparedIntents(ct);
            (type, _) = await coordinator.LocateAndCommitTransaction(handle, ct);
        }

        return type;
    }

    private static async Task<string> ReadValue(KahunaManager manager, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await manager.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        return Encoding.UTF8.GetString(entry!.Value!);
    }

    private static async Task AssertRecordOnEveryReplica(Cluster cluster, TransactionHandle handle, TransactionDecision decision, int rejectedBundles)
    {
        foreach (KahunaManager manager in cluster.Managers)
        {
            await WaitUntilAsync(() =>
                manager.DurableTransactionRecordStore.Get(handle.TransactionId, 1) is { } record
                && record.Decision == decision
                && (record.RejectedBundledCommitOpIds?.Count ?? 0) >= rejectedBundles);
        }
    }

    /// <summary>Every replica applied the stalled bundle and memoed its rejection, and none recorded Commit. The
    /// record may already be Abort: a replica whose advisory fence flagged the same stale base drives a
    /// best-effort abort at the anchor the moment the prepare applies, racing this observation.</summary>
    private static async Task AssertBundleRejectedOnEveryReplica(Cluster cluster, TransactionHandle handle)
    {
        foreach (KahunaManager manager in cluster.Managers)
        {
            await WaitUntilAsync(() =>
                manager.DurableTransactionRecordStore.Get(handle.TransactionId, 1) is { } record
                && (record.RejectedBundledCommitOpIds?.Count ?? 0) >= 1);

            Assert.NotEqual(TransactionDecision.Commit, manager.DurableTransactionRecordStore.Get(handle.TransactionId, 1)!.Decision);
        }
    }

    // ── eligibility ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task ReadModifyWrite_WithApplyTimeValidation_CommitsOnePhase_OnEveryReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: true);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-rmw/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];
                cluster.Taps[leader].ResetCounts();

                (TransactionHandle handle, long baseRevision) = await StartAndRead(coordinator, KeyRoutedTo(coordinator, "atv-coord", partition), key, ct);
                await Write(coordinator, handle, key, "150", ct);

                (KeyValueResponseType commitType, _) = await coordinator.LocateAndCommitTransaction(handle, ct);
                Assert.Equal(KeyValueResponseType.Committed, commitType);

                Assert.True(cluster.Taps[leader].OnePhaseBundles >= 1, "a read-modify-write must take the one-phase bundle with apply-time validation on");
                Assert.Equal(0, cluster.Taps[leader].TwoPhaseAnchorBundles);

                await AssertRecordOnEveryReplica(cluster, handle, TransactionDecision.Commit, rejectedBundles: 0);

                // Settlement is deferred: every replica's ledger reaches the committed revision once it applies.
                foreach (KahunaManager manager in cluster.Managers)
                    await WaitUntilAsync(() =>
                        manager.DurablePreparedIntentStore.TryGetLedgerHead(partition, key, out long head, out _, out _) && head == baseRevision + 1);

                Assert.Equal("150", await ReadValue(cluster.Managers[1], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }

    [Fact]
    public async Task ReadModifyWrite_WithoutTheOption_TakesTwoPhase()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: false);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-off/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];
                cluster.Taps[leader].ResetCounts();

                (TransactionHandle handle, _) = await StartAndRead(coordinator, KeyRoutedTo(coordinator, "atv-coord", partition), key, ct);
                await Write(coordinator, handle, key, "150", ct);

                (KeyValueResponseType commitType, _) = await coordinator.LocateAndCommitTransaction(handle, ct);
                Assert.Equal(KeyValueResponseType.Committed, commitType);

                Assert.Equal(0, cluster.Taps[leader].OnePhaseBundles);
                Assert.True(cluster.Taps[leader].TwoPhaseAnchorBundles >= 1, "without the option a read-modify-write in a multi-process group runs two-phase");
                Assert.Equal("150", await ReadValue(cluster.Managers[0], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }

    [Fact]
    public async Task OffPartitionRead_AndRangeLock_KeepTheBundleClosed_OnPartitionRead_Opens()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: true);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-elig/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];
                string coordinatorKey = KeyRoutedTo(coordinator, "atv-coord", partition);

                // A read-only key routed to another partition: no deterministic apply-time check exists for
                // it, so the bundle stays closed.
                string offPartitionRead = KeyRoutedTo(coordinator, "atv-read", partition, on: false);
                await SeedDurable(cluster, offPartitionRead, "1");
                cluster.Taps[leader].ResetCounts();
                (TransactionHandle offHandle, _) = await StartAndRead(coordinator, coordinatorKey, offPartitionRead, ct);
                await Write(coordinator, offHandle, key, "101", ct);
                Assert.Equal(KeyValueResponseType.Committed, (await coordinator.LocateAndCommitTransaction(offHandle, ct)).Item1);
                Assert.Equal(0, cluster.Taps[leader].OnePhaseBundles);
                Assert.True(cluster.Taps[leader].TwoPhaseAnchorBundles >= 1);

                // A range lock is a predicate, not a key: closed.
                cluster.Taps[leader].ResetCounts();
                (TransactionHandle rangeHandle, _) = await StartAndRead(coordinator, coordinatorKey, key, ct);
                (KeyValueResponseType lockType, _) = await coordinator.LocateAndTryAcquireRangeLock(
                    rangeHandle.TransactionId, "atv-elig", key, true, key + "~", false, 60_000,
                    KeyValueDurability.Persistent, RangeLockMode.Shared, ct,
                    coordinatorKey: rangeHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Locked, lockType);
                await Write(coordinator, rangeHandle, key, "102", ct);
                Assert.Equal(KeyValueResponseType.Committed, (await coordinator.LocateAndCommitTransaction(rangeHandle, ct)).Item1);
                Assert.Equal(0, cluster.Taps[leader].OnePhaseBundles);
                Assert.True(cluster.Taps[leader].TwoPhaseAnchorBundles >= 1);

                // A read-only key on the anchor partition rides into the bundle as a checked dependency: open.
                string onPartitionRead = KeyRoutedTo(coordinator, "atv-read", partition);
                await SeedDurable(cluster, onPartitionRead, "1");
                cluster.Taps[leader].ResetCounts();
                (TransactionHandle onHandle, _) = await StartAndRead(coordinator, coordinatorKey, onPartitionRead, ct);
                await Write(coordinator, onHandle, key, "103", ct);
                Assert.Equal(KeyValueResponseType.Committed, (await coordinator.LocateAndCommitTransaction(onHandle, ct)).Item1);
                Assert.True(cluster.Taps[leader].OnePhaseBundles >= 1, "an on-partition point read must keep the bundle open");

                Assert.Equal("103", await ReadValue(cluster.Managers[0], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }

    // ── the stall interleavings ───────────────────────────────────────────────────

    /// <summary>Stalls the coordinator's next record-carrying proposal and returns the tap that holds it.</summary>
    private static async Task<BatchTap> StallCommit(Cluster cluster, KahunaManager coordinator, TransactionHandle handle, CancellationToken ct)
    {
        foreach (BatchTap tap in cluster.Taps)
            tap.Armed = true;

        (KeyValueResponseType commitType, _) = await coordinator.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, commitType);

        foreach (BatchTap tap in cluster.Taps)
            tap.Armed = false;

        BatchTap holder = cluster.Taps.Single(static t => t.StalledBatches > 0);

        // Let the stalled transaction's in-memory write intent lapse, as a paused coordinator's would.
        await Task.Delay(400, ct);
        return holder;
    }

    /// <summary>Replays the stalled batches ahead of a throwaway durable commit on the same partition (the heal:
    /// the stalled entry surfaces in log order ahead of the next proposal).</summary>
    private static async Task Heal(Cluster cluster, BatchTap holder, KahunaManager coordinator, int partition)
    {
        holder.InjectStalledBeforeNext = true;
        string trigger = KeyRoutedTo(coordinator, "atv-trigger", partition);
        KeyValueTransactionResult triggered = await RetryOnMustRetry(
            coordinator, Encoding.UTF8.GetBytes($"BEGIN SET `{trigger}` '1' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, triggered.Type);
        Assert.Equal(0, holder.StalledBatches);
    }

    /// <summary>
    /// The lost-update shape of a stalled bundle: the competitor commits and fully settles the same base while
    /// the read-modify-write's bundle is in flight, and the bundle applies afterwards. The apply-time base
    /// check rejects it on every replica — the record stays Undecided, memoed — and the retry is told the truth:
    /// a conflict abort, the competitor's write surviving.
    /// </summary>
    [Fact]
    public async Task StalledBundle_CompetitorCommittedTheBase_IsRejectedOnEveryReplica_AndAbortsTruthfully()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: true);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-lost/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];

                (TransactionHandle victim, long baseRevision) = await StartAndRead(coordinator, KeyRoutedTo(coordinator, "atv-coord", partition), key, ct);
                await Write(coordinator, victim, key, "99", ct);

                BatchTap holder = await StallCommit(cluster, coordinator, victim, ct);

                // The competitor commits the same base and settles everywhere.
                KeyValueTransactionResult competitor = await RetryOnMustRetry(
                    coordinator, Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '101' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, competitor.Type);
                foreach (KahunaManager manager in cluster.Managers)
                    await WaitUntilAsync(() =>
                        manager.DurablePreparedIntentStore.TryGetLedgerHead(partition, key, out long head, out _, out _) && head > baseRevision);

                long staleBaseBefore = DurableTransactionMetrics.OnePhaseGatedCommitStaleBaseRejectionsCount;

                await Heal(cluster, holder, coordinator, partition);

                // Every replica applied the stalled bundle and refused its commit for the same reason.
                await AssertBundleRejectedOnEveryReplica(cluster, victim);
                Assert.True(DurableTransactionMetrics.OnePhaseGatedCommitStaleBaseRejectionsCount >= staleBaseBefore + 3,
                    "each of the three replicas must have counted the stale-base rejection");

                // The retry re-proposes the bundle (its own intent still holds the key), the gate refuses again,
                // and the coordinator drives the truthful conflict abort.
                KeyValueResponseType terminal = await CommitToTerminal(coordinator, victim, ct);
                Assert.Equal(KeyValueResponseType.Aborted, terminal);

                await AssertRecordOnEveryReplica(cluster, victim, TransactionDecision.Abort, rejectedBundles: 1);
                Assert.Equal("101", await ReadValue(cluster.Managers[0], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }

    /// <summary>
    /// The write-skew shape: the stalled bundle carried a read-only dependency on the anchor partition, and a
    /// competitor committed that key before the bundle applied. The apply-time read check rejects it on every
    /// replica; the retry's own validation then finds the moved read and aborts.
    /// </summary>
    [Fact]
    public async Task StalledBundle_ReadDependencyMoved_IsRejectedOnEveryReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: true);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-skew/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                string readKey = KeyRoutedTo(cluster.Managers[0], "atv-skew-read", partition);
                await SeedDurable(cluster, readKey, "1");
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];

                (TransactionHandle victim, long readRevision) = await StartAndRead(coordinator, KeyRoutedTo(coordinator, "atv-coord", partition), readKey, ct);
                await Write(coordinator, victim, key, "99", ct);

                BatchTap holder = await StallCommit(cluster, coordinator, victim, ct);

                KeyValueTransactionResult competitor = await RetryOnMustRetry(
                    coordinator, Encoding.UTF8.GetBytes($"BEGIN SET `{readKey}` '2' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, competitor.Type);
                foreach (KahunaManager manager in cluster.Managers)
                    await WaitUntilAsync(() =>
                        manager.DurablePreparedIntentStore.TryGetLedgerHead(partition, readKey, out long head, out _, out _) && head > readRevision);

                long staleReadBefore = DurableTransactionMetrics.OnePhaseGatedCommitStaleReadRejectionsCount;

                await Heal(cluster, holder, coordinator, partition);

                await AssertBundleRejectedOnEveryReplica(cluster, victim);
                Assert.True(DurableTransactionMetrics.OnePhaseGatedCommitStaleReadRejectionsCount >= staleReadBefore + 3);

                Assert.Equal(KeyValueResponseType.Aborted, await CommitToTerminal(coordinator, victim, ct));
                await AssertRecordOnEveryReplica(cluster, victim, TransactionDecision.Abort, rejectedBundles: 1);
                Assert.Equal("100", await ReadValue(cluster.Managers[0], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }

    /// <summary>
    /// A foreign undecided intent on the read key at the bundle's apply — a competitor's write this
    /// transaction's validation never saw — rejects it on every replica. Once that intent aborts and is
    /// removed, the read is current again and the retry commits.
    /// </summary>
    [Fact]
    public async Task StalledBundle_ReadDependencyUnderAForeignLiveIntent_IsRejected_ThenCommitsOnceItClears()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: true);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-intent/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                string readKey = KeyRoutedTo(cluster.Managers[0], "atv-intent-read", partition);
                await SeedDurable(cluster, readKey, "1");
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];

                (TransactionHandle victim, _) = await StartAndRead(coordinator, KeyRoutedTo(coordinator, "atv-coord", partition), readKey, ct);
                await Write(coordinator, victim, key, "99", ct);

                BatchTap holder = await StallCommit(cluster, coordinator, victim, ct);

                // A competitor's prepared, undecided intent on the read key, present on every replica.
                HLCTimestamp foreignTx = new(0, victim.TransactionId.L + 1, 0);
                PreparedIntent foreign = new(
                    foreignTx, 1, readKey, ManifestHash: 42, RecordAnchorKey: readKey, CommitTimestamp: new HLCTimestamp(0, foreignTx.L + 1, 0),
                    State: KeyValueState.Set, Value: "9"u8.ToArray(), Bucket: null, Revision: 5, Expires: HLCTimestamp.Zero,
                    NoRevision: false, BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
                    RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);
                foreach (KahunaManager manager in cluster.Managers)
                    manager.DurablePreparedIntentStore.ImportIntents([foreign]);

                long staleReadBefore = DurableTransactionMetrics.OnePhaseGatedCommitStaleReadRejectionsCount;

                await Heal(cluster, holder, coordinator, partition);

                await AssertBundleRejectedOnEveryReplica(cluster, victim);
                Assert.True(DurableTransactionMetrics.OnePhaseGatedCommitStaleReadRejectionsCount >= staleReadBefore + 3);

                // The competitor aborts and settles; the read is current again.
                foreach (KahunaManager manager in cluster.Managers)
                {
                    manager.DurablePreparedIntentStore.Apply(new ResolveIntentCommand(foreignTx, 1, readKey, Commit: false), partition);
                    manager.DurablePreparedIntentStore.Apply(new RemoveIntentCommand(foreignTx, 1, readKey), partition);
                }

                Assert.Equal(KeyValueResponseType.Committed, await CommitToTerminal(coordinator, victim, ct));
                await AssertRecordOnEveryReplica(cluster, victim, TransactionDecision.Commit, rejectedBundles: 1);
                Assert.Equal("99", await ReadValue(cluster.Managers[0], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }

    /// <summary>
    /// A transaction older than the ledger's retention horizon cannot have its base verified — a pruned head is
    /// indistinguishable from "no commit happened" — so the gate refuses it deterministically on every replica.
    /// </summary>
    [Fact]
    public async Task StalledBundle_OlderThanTheRetentionHorizon_IsRefusedOnEveryReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: true, config => config.StagedBaseFenceRetentionMs = 1_000);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-stale/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];

                (TransactionHandle victim, _) = await StartAndRead(coordinator, KeyRoutedTo(coordinator, "atv-coord", partition), key, ct);
                await Write(coordinator, victim, key, "99", ct);

                BatchTap holder = await StallCommit(cluster, coordinator, victim, ct);

                // Let the transaction age past the horizon, then move the partition's watermark past it with
                // an unrelated key's commit.
                await Task.Delay(1_200, ct);
                string other = KeyRoutedTo(coordinator, "atv-stale-other", partition);
                await SeedDurable(cluster, other, "1");
                foreach (KahunaManager manager in cluster.Managers)
                    await WaitUntilAsync(() => manager.DurablePreparedIntentStore.GetLedgerWatermark(partition).L > victim.TransactionId.L + 1_000);

                long staleBaseBefore = DurableTransactionMetrics.OnePhaseGatedCommitStaleBaseRejectionsCount;

                await Heal(cluster, holder, coordinator, partition);

                await AssertBundleRejectedOnEveryReplica(cluster, victim);
                Assert.True(DurableTransactionMetrics.OnePhaseGatedCommitStaleBaseRejectionsCount >= staleBaseBefore + 3);

                Assert.Equal(KeyValueResponseType.Aborted, await CommitToTerminal(coordinator, victim, ct));
                Assert.Equal("100", await ReadValue(cluster.Managers[0], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }

    /// <summary>
    /// A head behind the validated base means non-transactional writes advanced the key after that commit; the
    /// ledger can attest nothing newer, so the stalled bundle is admitted on every replica and the transaction
    /// commits — the two-phase fence's rule, kept.
    /// </summary>
    [Fact]
    public async Task StalledBundle_HeadBehindTheBase_IsAdmittedOnEveryReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: true);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-behind/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];
                long head = await LedgerHeadRevision(coordinator, partition, key);

                // A non-transactional write advances the key past the remembered head.
                (KeyValueResponseType setType, _, _) = await coordinator.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, "110"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Set, setType);

                (TransactionHandle handle, long baseRevision) = await StartAndRead(coordinator, KeyRoutedTo(coordinator, "atv-coord", partition), key, ct);
                Assert.True(baseRevision > head);
                await Write(coordinator, handle, key, "120", ct);

                BatchTap holder = await StallCommit(cluster, coordinator, handle, ct);
                await Heal(cluster, holder, coordinator, partition);

                await AssertRecordOnEveryReplica(cluster, handle, TransactionDecision.Commit, rejectedBundles: 0);

                Assert.Equal(KeyValueResponseType.Committed, await CommitToTerminal(coordinator, handle, ct));
                Assert.Equal("120", await ReadValue(cluster.Managers[0], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }

    /// <summary>
    /// The shipped bundled-prepare gate, unchanged under the option: a second read-modify-write let in by the
    /// first one's lapsed lease, whose bundle applies behind the first's healed bundle, has its prepare rejected
    /// against the first's live intent and its commit withheld with it; it is never told Committed.
    /// </summary>
    [Fact]
    public async Task SecondWriter_BehindAStalledBundle_IsNeverToldCommitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Cluster cluster = await Assemble(applyTimeValidation: true);

        try
        {
            await RunUnderStableLeadership(cluster.Rafts[0], Partitions, async () =>
            {
                string key = "atv-second/" + Guid.NewGuid().ToString("N")[..8];
                await SeedDurable(cluster, key, "100");
                int partition = PartitionOf(cluster.Managers[0], key);
                int leader = await cluster.LeaderIndexOf(partition, ct);
                KahunaManager coordinator = cluster.Managers[leader];
                string coordinatorKey = KeyRoutedTo(coordinator, "atv-coord", partition);

                (TransactionHandle first, _) = await StartAndRead(coordinator, coordinatorKey, key, ct);
                await Write(coordinator, first, key, "first", ct);
                BatchTap holder = await StallCommit(cluster, coordinator, first, ct);

                (TransactionHandle second, _) = await StartAndRead(coordinator, coordinatorKey, key, ct);
                await Write(coordinator, second, key, "second", ct);

                // The heal delivers the first bundle immediately ahead of the second's own.
                holder.InjectStalledBeforeNext = true;
                (KeyValueResponseType commitSecond, _) = await coordinator.LocateAndCommitTransaction(second, ct);
                Assert.NotEqual(KeyValueResponseType.Committed, commitSecond);

                await AssertRecordOnEveryReplica(cluster, first, TransactionDecision.Commit, rejectedBundles: 0);
                Assert.Equal(KeyValueResponseType.Aborted, await CommitToTerminal(coordinator, second, ct));
                Assert.Equal(KeyValueResponseType.Committed, await CommitToTerminal(coordinator, first, ct));
                Assert.Equal("first", await ReadValue(cluster.Managers[0], key, ct));
            });
        }
        finally
        {
            await cluster.Leave();
        }
    }
}
