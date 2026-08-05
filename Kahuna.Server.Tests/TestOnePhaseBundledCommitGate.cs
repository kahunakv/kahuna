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
/// End-to-end coverage of the bundled-prepare gate on the one-phase commit fast path, driving the lost-update
/// interleaving that produced Elle's incompatible-order fork: two read-modify-write transactions on one key both
/// reporting Committed, one of whose writes was silently discarded.
///
/// The one-phase path decides in a single atomic batch ([record init + prepare + commit decision]) on the safety
/// argument that in-memory write intents exclude any conflicting prepare from landing behind its pre-flight
/// check. That argument dies with the intents: a proposal stalled by a partition outlives the 15-second intent
/// lease (and a killed node's wiped memory), a second transaction reads the old base and proposes its own bundle
/// behind the stalled one, and when the log heals both apply in order — the first installs its intent and
/// commits; the second's prepare is rejected but, without the gate, its bundled decision still flips its record
/// to Commit. The second client is told Committed while its write exists nowhere durable.
///
/// The gate makes the bundled decision conditional on its own prepare at apply time, deterministically in log
/// order: the loser's record stays Undecided, its commit answers the retryable MustRetry, and the retry's 2PC
/// fallback drives a truthful abort. The stall and the heal are modeled exactly as in
/// <see cref="TestAbandonedDurableFinalizeFence"/>: the write-batch executor captures-and-fails the first
/// transaction's record batches, then replays them immediately ahead of the second transaction's bundle.
/// </summary>
public sealed class TestOnePhaseBundledCommitGate
{
    private readonly ILoggerFactory loggerFactory;

    public TestOnePhaseBundledCommitGate(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// While <see cref="Armed"/>, fails (and captures) every batch carrying a transaction-record entry — a
    /// proposal whose acknowledgement was lost to a partition. When <see cref="InjectStalledBeforeNext"/> is set,
    /// the next record-carrying batch is preceded by the replay of every captured batch (proposed through the
    /// real executor and applied to the stores, exactly as the healed partition would deliver the stalled entry
    /// immediately ahead of it in log order), firing once.
    /// </summary>
    private sealed class StallingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private readonly List<(int Partition, RaftProposalEntry[] Entries)> stalled = [];
        private volatile bool armed;
        private volatile bool injectBeforeNext;

        public StallingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        // The stores the replayed entries apply to; set once the node is started.
        public KahunaManager? Kahuna { get; set; }

        public bool Armed { set => armed = value; }

        public bool InjectStalledBeforeNext { set => injectBeforeNext = value; }

        public int StalledBatches { get { lock (stalled) return stalled.Count; } }

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            bool carriesRecord = entries.Any(static e => e.Type == ReplicationTypes.TransactionRecord);

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

                // This raw replay bypasses the scheduler's completion path, so apply each committed delta to the
                // stores exactly as the replication callback / WAL replay does — in entry order, so the bundled
                // commit's apply-time prepare check sees the same intent state every replica would.
                foreach (RaftProposalEntry entry in entries)
                {
                    RaftLog log = new() { LogType = entry.Type, LogData = entry.Data };
                    if (entry.Type == ReplicationTypes.TransactionRecord)
                        Kahuna!.DurableTransactionRecordStore.Replicate(partition, log);
                    else if (entry.Type == ReplicationTypes.PreparedIntent)
                        Kahuna!.DurablePreparedIntentStore.ApplyDeltaAckPrepares(log);
                }
            }
        }
    }

    /// <summary>
    /// The incompatible-order shape, prevented end to end: the second read-modify-write transaction — let in by
    /// the first one's expired in-memory intent lease while the first's decision proposal is still in flight —
    /// must never be told Committed once the healed log applies the stalled bundle ahead of its own: its bundled
    /// prepare is rejected there, so its bundled decision must be rejected with it. The commit answers MustRetry,
    /// the retry's 2PC fallback aborts truthfully on the conflict, and the surviving value is the first
    /// transaction's — one winner, no fork.
    /// </summary>
    [Fact]
    public async Task SecondWriter_BehindAStalledBundle_IsNeverToldCommittedForADiscardedWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string key = "gate/lostupdate/k1";

        StallingExecutor? stalling = null;
        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            // Synchronous settlement keeps resolution inline, so post-outcome store state is deterministic.
            DurableDeferredSettlement = false,
            WriteBatchExecutorDecorator = inner => stalling = new StallingExecutor(inner)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(key, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        StallingExecutor executor = stalling!;
        executor.Kahuna = kahuna;

        // Seed the committed base both transactions will read.
        (KeyValueResponseType seed, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "base"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, seed);

        // ── First transaction: read-modify-write whose durable decision stalls in flight ──
        (KeyValueResponseType startedA, TransactionHandle first) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                Locking = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                ReadValidation = ReadValidation.TrackAndValidate,
                Timeout = 60000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startedA);

        (KeyValueResponseType readA, _) = await node.Kahuna.LocateAndTryGetValue(
            first.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            first.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, readA);

        (KeyValueResponseType setA, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            first.TransactionId, key, "first-writer"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, 0, first.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setA);

        executor.Armed = true;
        (KeyValueResponseType commitA, _) = await kahuna.LocateAndCommitTransaction(first, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, commitA);
        Assert.True(executor.StalledBatches > 0, "the durable decision proposal must have been stalled");
        executor.Armed = false;

        // The in-memory write intent that excludes conflicting writers is a 15-second lease. The stalled
        // proposal outlives it — the coordinator holds the working set, but holding it does not extend the
        // lease, exactly as a killed coordinator could not. Once it lapses the second writer walks in.
        await Task.Delay(TimeSpan.FromSeconds(15.5), ct);

        // ── Second transaction: the same read-modify-write against the still-unchanged base ──
        (KeyValueResponseType startedB, TransactionHandle second) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                Locking = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                ReadValidation = ReadValidation.TrackAndValidate,
                Timeout = 60000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startedB);

        (KeyValueResponseType readB, ReadOnlyKeyValueEntry? baseEntry) = await node.Kahuna.LocateAndTryGetValue(
            second.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            second.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, readB);
        Assert.True(baseEntry!.Value.AsSpan().SequenceEqual("base"u8),
            "the second writer must observe the pre-stall base, not the stalled transaction's write");

        (KeyValueResponseType setB, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            second.TransactionId, key, "second-writer"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, 0, second.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setB);

        // The heal: the stalled bundle is delivered immediately ahead of the second transaction's own bundle in
        // log order. The first transaction durably commits; the second's prepare is rejected against its live
        // intent — and its bundled decision must be rejected with it.
        executor.InjectStalledBeforeNext = true;

        (KeyValueResponseType commitB, _) = await kahuna.LocateAndCommitTransaction(second, ct);
        Assert.NotEqual(KeyValueResponseType.Committed, commitB);

        // Drive the retryable outcome to its truthful terminal: the 2PC fallback meets the winner's settled
        // state and aborts on the read-set conflict. It must never surface Committed — the write was discarded.
        KeyValueResponseType terminalB = commitB;
        for (int attempt = 0; attempt < 50 && terminalB == KeyValueResponseType.MustRetry; attempt++)
        {
            await Task.Delay(200, ct);
            (terminalB, _) = await kahuna.LocateAndCommitTransaction(second, ct);
        }
        Assert.Equal(KeyValueResponseType.Aborted, terminalB);

        // The canonical records agree: the stalled transaction won; the second was never durably committed.
        TransactionRecord? firstRecord = kahuna.DurableTransactionRecordStore.Get(first.TransactionId, 1);
        Assert.NotNull(firstRecord);
        Assert.Equal(TransactionDecision.Commit, firstRecord!.Decision);

        TransactionRecord? secondRecord = kahuna.DurableTransactionRecordStore.Get(second.TransactionId, 1);
        if (secondRecord is not null)
            Assert.NotEqual(TransactionDecision.Commit, secondRecord.Decision);

        // One winner, one surviving value: the first transaction's write is (or becomes, once recovery settles
        // the intent) the visible revision, and the discarded write never surfaces.
        await WaitUntil(async () =>
        {
            await kahuna.KeyValues.RecoverPreparedIntents(ct);
            (KeyValueResponseType read, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            return read == KeyValueResponseType.Get && entry?.Value is not null &&
                   entry.Value.AsSpan().SequenceEqual("first-writer"u8);
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
