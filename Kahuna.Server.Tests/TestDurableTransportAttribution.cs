using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The inter-node calls one durable commit costs when the transaction's session node does not lead the anchor
/// partition (the written key's partition), read from the session node's own transport counters, and the typed
/// wire operations that keep that count down: a single-participant commit crosses whole as ONE one-phase bundle
/// ([record init, prepare, decision]) the receiving leader submits atomically and answers with the canonical
/// outcome; a multi-participant commit's anchor init and prepare cross as one bundle (whose refused prepare
/// comes back with its reason) and its decision comes back with the canonical outcome read on the anchor leader
/// after its apply, so the sender needs no separate record lookup. An older receiver that lacks the typed
/// operations is served by the per-entry forwards and the lookup exactly as before.
/// </summary>
public sealed class TestDurableTransportAttribution : BaseCluster
{
    private const int Partitions = 6;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestDurableTransportAttribution(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<int> LeaderIndexOf(int partition, IRaft[] rafts, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return i;

            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// Two fresh keys whose partitions are led by different nodes: the session lives on the first key's leader
    /// (the coordinator key), and the second key is the transaction's only write, so its partition anchors the
    /// record on a node other than the session's. Hash routing hashes the key space (the prefix before the last
    /// '/'), so the candidates vary that prefix. With one replica per partition the partitions are placed across
    /// the three nodes, so such a pair exists.
    /// </summary>
    private static async Task<(string CoordinatorKey, int SessionIndex, string Key, int AnchorIndex)> FindRemoteAnchorPair(
        IRaft[] rafts, KahunaManager[] managers, CancellationToken ct)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        Dictionary<int, (string Key, int Leader)> byPartition = [];

        for (int i = 0; i < 4_096 && byPartition.Count < Partitions; i++)
        {
            string candidate = $"xta{i}/{random}";
            int partition = managers[0].KeyValues.LocateDurablePartition(candidate).PartitionId;
            if (byPartition.ContainsKey(partition))
                continue;

            byPartition[partition] = (candidate, await LeaderIndexOf(partition, rafts, ct));
        }

        foreach ((string coordinatorKey, int sessionIndex) in byPartition.Values)
        {
            foreach ((string key, int anchorIndex) in byPartition.Values)
            {
                if (anchorIndex != sessionIndex)
                    return (coordinatorKey, sessionIndex, key, anchorIndex);
            }
        }

        throw new InvalidOperationException("every data partition is led by the same node; no remote-anchor pair exists");
    }

    private static async Task<TransactionHandle> CommitRemoteAnchorWrite(
        KahunaManager session, string coordinatorKey, string key, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType writeType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "1"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, writeType);

        (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);
        return handle;
    }

    [Fact]
    public async Task RemoteAnchorCommit_OneTypedOnePhaseForward_NoBundleNoDecisionNoLookup()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string coordinatorKey, int sessionIndex, string key, int anchorIndex) = await FindRemoteAnchorPair(rafts, managers, ct);
            KahunaManager session = managers[sessionIndex];

            // The path under test only exists when the session node does not lead the anchor partition.
            int anchorPartition = session.KeyValues.LocateDurablePartition(key).PartitionId;
            Assert.False(await rafts[sessionIndex].AmILeaderIfHosted(anchorPartition, ct));

            DurableTransportCounts before = session.KeyValues.DurableTransportCounts;

            await CommitRemoteAnchorWrite(session, coordinatorKey, key, ct);

            // Deferred resolution ends with the settle forward; the anchor leader drops the intent when the
            // settle applies, which happens before that forward returns and after it was counted.
            await WaitUntilAsync(() => managers[anchorIndex].DurablePreparedIntentStore.Get(key) is null);

            DurableTransportCounts after = session.KeyValues.DurableTransportCounts;

            // The single-participant commit crosses as ONE one-phase bundle carrying init, prepare and the
            // decision, answered with the canonical outcome: no two-phase bundle, no separate decision, no
            // record lookup.
            Assert.Equal(1, after.OnePhaseForwards - before.OnePhaseForwards);
            Assert.Equal(0, after.BundleForwards - before.BundleForwards);
            Assert.Equal(0, after.DecisionForwards - before.DecisionForwards);
            Assert.Equal(0, after.RecordLookupForwards - before.RecordLookupForwards);

            // The deferred resolution: materialization and settle, one delta forward each.
            Assert.Equal(2, after.ReplicateForwards - before.ReplicateForwards);

            // The committed value's leader-state apply: once inline before the commit returns (the one-phase
            // path's read-your-writes apply), once by the deferred resolution's idempotent re-apply.
            Assert.Equal(2, after.CommitForwards - before.CommitForwards);
            Assert.Equal(0, after.RollbackForwards - before.RollbackForwards);

            // Every forward went straight to the leader.
            Assert.Equal(0, after.Redirects - before.Redirects);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>An older receiver without the typed operations: the sender falls back to the per-entry forwards
    /// and the record lookup, commits all the same, and pays the old call count (two forwards for the bundle,
    /// one for the decision, one lookup).</summary>
    [Fact]
    public async Task RemoteAnchorCommit_OlderReceiver_FallsBackToPerEntryForwardsAndLookup()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string coordinatorKey, int sessionIndex, string key, int anchorIndex) = await FindRemoteAnchorPair(rafts, managers, ct);
            KahunaManager session = managers[sessionIndex];

            MemoryInterNodeCommmunication transport = Assert.IsType<MemoryInterNodeCommmunication>(session.KeyValues.InterNodeCommunication);
            transport.TypedDurableOperations = false;

            DurableTransportCounts before = session.KeyValues.DurableTransportCounts;

            await CommitRemoteAnchorWrite(session, coordinatorKey, key, ct);
            await WaitUntilAsync(() => managers[anchorIndex].DurablePreparedIntentStore.Get(key) is null);

            DurableTransportCounts after = session.KeyValues.DurableTransportCounts;

            // Init, prepare, decision, materialization, settle: five delta forwards, one lookup, as before the
            // typed wire existed. The typed attempts are counted where they were tried and refused.
            Assert.Equal(5, after.ReplicateForwards - before.ReplicateForwards);
            Assert.Equal(1, after.RecordLookupForwards - before.RecordLookupForwards);
            Assert.Equal(1, after.CommitForwards - before.CommitForwards);
            Assert.Equal(0, after.Redirects - before.Redirects);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    private static PreparedIntent RawIntent(HLCTimestamp txId, string key, HLCTimestamp commitTimestamp, HLCTimestamp recoveryDeadline, long baseRevision) =>
        new(txId, 1, key, ManifestHash: 0, RecordAnchorKey: key, CommitTimestamp: commitTimestamp,
            State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes("raw"), Bucket: null, Revision: baseRevision + 1, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: baseRevision, BaseState: KeyValueState.Set, RecoveryDeadline: recoveryDeadline,
            Resolution: PreparedIntentResolution.Pending);

    private static (string LogType, byte[] Payload)[] RawBundle(HLCTimestamp txId, string key, HLCTimestamp now, long baseRevision)
    {
        HLCTimestamp deadline = new(now.N, now.L + 60_000, now.C);
        byte[] init = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(
            txId, 1, key, key, now, deadline, 0, [new TransactionParticipantRef(key, KeyValueDurability.Persistent)], now, now)]);
        byte[] prepare = PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(RawIntent(txId, key, now, deadline, baseRevision))]);
        return [(ReplicationTypes.TransactionRecord, init), (ReplicationTypes.PreparedIntent, prepare)];
    }

    /// <summary>One transaction's raw one-phase bundle deltas ([record init, prepare, commit decision]) against
    /// <paramref name="key"/>, with the decision's attempt HLC and the record's frozen deadline supplied so a
    /// test can drive the deadline gate as well as the happy path. The commit carries its bundled prepare key,
    /// as the production bundle does, so the record store's bundled-commit gate judges it at apply.</summary>
    private static (byte[] Init, byte[] Prepare, byte[] Decision) RawOnePhase(
        HLCTimestamp txId, string key, HLCTimestamp now, HLCTimestamp opId, HLCTimestamp attemptHlc, HLCTimestamp deadline, long baseRevision)
    {
        byte[] init = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(
            txId, 1, key, key, now, deadline, 0, [new TransactionParticipantRef(key, KeyValueDurability.Persistent)], opId, now)]);
        byte[] prepare = PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(RawIntent(txId, key, now, deadline, baseRevision))]);
        byte[] decision = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(txId, 1, 0, opId, attemptHlc, [key])]);
        return (init, prepare, decision);
    }

    /// <summary>
    /// The receiving leader's side of the typed one-phase operation, driven directly: a clean bundle commits and
    /// answers the canonical Commit; a duplicate delivery replays idempotently and answers the same; a bundle
    /// whose prepare finds the key held by another transaction's live intent has its commit withheld by the
    /// bundled-commit gate and answers the held-key refusal with the PrepareMissing verdict; and a bundle whose
    /// attempt HLC passed the frozen deadline stays Undecided with an acknowledged prepare and no gate verdict —
    /// the shape the origin classifies as a late commit.
    /// </summary>
    [Fact]
    public async Task Receiver_OnePhaseAnswersTheCanonicalOutcome_AndNamesTheGateVerdict()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (_, _, string key, int anchorIndex) = await FindRemoteAnchorPair(rafts, managers, ct);
            KahunaManager leader = managers[anchorIndex];
            int partition = leader.KeyValues.LocateDurablePartition(key).PartitionId;
            HLCTimestamp now = rafts[anchorIndex].HybridLogicalClock.TrySendOrLocalEvent(rafts[anchorIndex].GetLocalNodeId());
            HLCTimestamp deadline = new(now.N, now.L + 60_000, now.C);

            // ── Clean bundle: the canonical Commit comes back with the batch signals ──
            HLCTimestamp txId = new(now.N, now.L - 5_000, now.C);
            HLCTimestamp opId = new(now.N, now.L - 4_999, now.C);
            (byte[] init, byte[] prepare, byte[] decision) = RawOnePhase(txId, key + "/1p", now, opId, now, deadline, 0);

            DurableOnePhaseWireReply? committed = await leader.DurableOnePhaseLocal(
                partition, init, prepare, decision, txId, 1, opId, null, 0, ct);
            Assert.NotNull(committed);
            Assert.True(committed!.Value.BatchCommitted);
            Assert.True(committed.Value.PrepareAcknowledged);
            Assert.True(committed.Value.DecisionKnown);
            Assert.Equal((int)TransactionDecision.Commit, committed.Value.Decision);

            // ── Duplicate delivery: idempotent, the same canonical answer ──
            DurableOnePhaseWireReply? duplicate = await leader.DurableOnePhaseLocal(
                partition, init, prepare, decision, txId, 1, opId, null, 0, ct);
            Assert.NotNull(duplicate);
            Assert.True(duplicate!.Value.DecisionKnown);
            Assert.Equal((int)TransactionDecision.Commit, duplicate.Value.Decision);

            // ── Key held by a live foreign intent: the gate withholds the commit and names the verdict ──
            string heldKey = key + "/1p-held";
            HLCTimestamp holder = new(now.N, now.L - 3_000, now.C);
            DurableBundleWireReply? held = await leader.DurableBundleLocal(partition, RawBundle(holder, heldKey, now, 0), terminal: false, null, 0, ct);
            Assert.True(held!.Value.PrepareAcknowledged);

            HLCTimestamp loser = new(now.N, now.L - 2_000, now.C);
            HLCTimestamp loserOp = new(now.N, now.L - 1_999, now.C);
            (byte[] loserInit, byte[] loserPrepare, byte[] loserDecision) = RawOnePhase(loser, heldKey, now, loserOp, now, deadline, 0);

            DurableOnePhaseWireReply? refused = await leader.DurableOnePhaseLocal(
                partition, loserInit, loserPrepare, loserDecision, loser, 1, loserOp, null, 0, ct);
            Assert.NotNull(refused);
            Assert.True(refused!.Value.BatchCommitted);
            Assert.False(refused.Value.PrepareAcknowledged);
            Assert.Equal((int)PrepareRejectionKind.KeyHeld, refused.Value.PrepareRejection);
            Assert.True(refused.Value.DecisionKnown);
            Assert.Equal((int)TransactionDecision.Undecided, refused.Value.Decision);
            Assert.Equal((int)BundledCommitVerdict.PrepareMissing, refused.Value.GatedVerdict);

            // ── Attempt past the frozen deadline: Undecided, prepare acknowledged, no gate verdict ──
            HLCTimestamp late = new(now.N, now.L - 1_000, now.C);
            HLCTimestamp lateOp = new(now.N, now.L - 999, now.C);
            HLCTimestamp shortDeadline = new(now.N, now.L + 1, now.C);
            HLCTimestamp pastDeadline = new(now.N, now.L + 10_000, now.C);
            (byte[] lateInit, byte[] latePrepare, byte[] lateDecision) = RawOnePhase(late, key + "/1p-late", now, lateOp, pastDeadline, shortDeadline, 0);

            DurableOnePhaseWireReply? withheld = await leader.DurableOnePhaseLocal(
                partition, lateInit, latePrepare, lateDecision, late, 1, lateOp, null, 0, ct);
            Assert.NotNull(withheld);
            Assert.True(withheld!.Value.BatchCommitted);
            Assert.True(withheld.Value.PrepareAcknowledged);
            Assert.True(withheld.Value.DecisionKnown);
            Assert.Equal((int)TransactionDecision.Undecided, withheld.Value.Decision);
            Assert.Equal((int)BundledCommitVerdict.Admit, withheld.Value.GatedVerdict);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The receiving leader's side of the typed operations, driven directly: a bundle whose prepare finds the
    /// key held by another transaction's live intent commits its record but answers the refusal as a held key; a
    /// bundle whose prepare was validated against a base the key's committed head moved past answers a stale
    /// base; and a decision answers the canonical winner after its apply — including a competing abort that won
    /// first, which the sender must see instead of the commit it asked for.
    /// </summary>
    [Fact]
    public async Task Receiver_BundleNamesTheRefusal_AndDecisionAnswersTheCanonicalWinner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string coordinatorKey, int sessionIndex, string key, int anchorIndex) = await FindRemoteAnchorPair(rafts, managers, ct);
            KahunaManager leader = managers[anchorIndex];
            int partition = leader.KeyValues.LocateDurablePartition(key).PartitionId;
            HLCTimestamp now = rafts[anchorIndex].HybridLogicalClock.TrySendOrLocalEvent(rafts[anchorIndex].GetLocalNodeId());

            // ── Held key ──
            string heldKey = key + "/held";
            HLCTimestamp holder = new(now.N, now.L - 3_000, now.C);
            HLCTimestamp loser = new(now.N, now.L - 2_000, now.C);

            DurableBundleWireReply? first = await leader.DurableBundleLocal(partition, RawBundle(holder, heldKey, now, 0), terminal: false, null, 0, ct);
            Assert.NotNull(first);
            Assert.True(first!.Value.BatchCommitted);
            Assert.True(first.Value.PrepareAcknowledged);

            DurableBundleWireReply? second = await leader.DurableBundleLocal(partition, RawBundle(loser, heldKey, now, 0), terminal: false, null, 0, ct);
            Assert.NotNull(second);
            Assert.True(second!.Value.BatchCommitted);          // the record is durable...
            Assert.False(second.Value.PrepareAcknowledged);     // ...the prepare is not
            Assert.Equal((int)PrepareRejectionKind.KeyHeld, second.Value.PrepareRejection);

            // ── Stale base ──
            // Two real commits settle a committed head at revision 1 for the key (the first write is revision 0);
            // a raw prepare validated against base 0 is then stale.
            for (int i = 0; i < 2; i++)
            {
                await CommitRemoteAnchorWrite(managers[sessionIndex], coordinatorKey, key, ct);
                await WaitUntilAsync(() => leader.DurablePreparedIntentStore.Get(key) is null);
            }

            HLCTimestamp stale = new(now.N, now.L - 1_000, now.C);
            DurableBundleWireReply? staleReply = await leader.DurableBundleLocal(partition, RawBundle(stale, key, now, 0), terminal: false, null, 0, ct);
            Assert.NotNull(staleReply);
            Assert.True(staleReply!.Value.BatchCommitted);
            Assert.False(staleReply.Value.PrepareAcknowledged);
            Assert.Equal((int)PrepareRejectionKind.StaleBase, staleReply.Value.PrepareRejection);

            // ── Decision: the canonical winner ──
            HLCTimestamp deadline = new(now.N, now.L + 60_000, now.C);
            byte[] commit = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(holder, 1, 0, now, now)]);
            DurableDecisionWireReply? decided = await leader.DurableDecisionLocal(partition, commit, holder, 1, null, 0, ct);
            Assert.NotNull(decided);
            Assert.True(decided!.Value.Replicated);
            Assert.True(decided.Value.Known);
            Assert.Equal((int)TransactionDecision.Commit, decided.Value.Decision);

            // A recovery abort that lands first wins; the commit that follows is answered with that abort.
            byte[] abort = TransactionRecordStore.SerializeDelta([new AbortTransactionCommand(
                loser, 1, 0, TransactionAbortClass.PresumedAbort, now, now, heldKey, now, deadline, now)]);
            DurableDecisionWireReply? aborted = await leader.DurableDecisionLocal(partition, abort, loser, 1, null, 0, ct);
            Assert.NotNull(aborted);
            Assert.Equal((int)TransactionDecision.Abort, aborted!.Value.Decision);

            byte[] lateCommit = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(loser, 1, 0, now, now)]);
            DurableDecisionWireReply? raced = await leader.DurableDecisionLocal(partition, lateCommit, loser, 1, null, 0, ct);
            Assert.NotNull(raced);
            Assert.True(raced!.Value.Replicated);
            Assert.True(raced.Value.Known);
            Assert.Equal((int)TransactionDecision.Abort, raced.Value.Decision);
            Assert.Equal((int)TransactionAbortClass.PresumedAbort, raced.Value.AbortClass);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
