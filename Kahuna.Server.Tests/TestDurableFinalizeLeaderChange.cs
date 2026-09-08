using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A finalize whose decision is durable on a quorum while the leader that proposed it changes underneath it.
/// The cluster must still name one canonical winner, every replica must agree on it, the value that survives
/// must be the one that winner names, and the caller must never be told an outcome the cluster did not reach.
///
/// <para>The window is unreachable without Kommander's committed-proposal reply hold: a finalize awaits the very
/// completion its answer rides on, so "durable at quorum" and "the coordinator learned it" are one event
/// otherwise. <see cref="HeldCommitReplies"/> separates them, and leadership is then moved with a deterministic
/// transfer rather than an election race.</para>
///
/// <para>Both commit shapes are covered, because they fail differently. The two-phase finalize proposes its
/// decision as a barrier of its own, so a leader change can sit between the prepare and the decision. The
/// one-phase bundle decides inside its prepare's atomic batch, so the interesting question is instead whether
/// the gate's verdict — judged at apply, in log order — is the same on the replica that inherits the partition
/// as on the one that proposed it.</para>
/// </summary>
public sealed class TestDurableFinalizeLeaderChange : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    /// <summary>Well above the time a transfer plus a recovery sweep needs, so a held reply never resolves
    /// underneath an assertion. Kommander bounds every hold by this.</summary>
    private static readonly TimeSpan HoldBound = TimeSpan.FromMinutes(2);

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestDurableFinalizeLeaderChange(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

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

    /// <summary>
    /// A fresh key per partition, so a caller can pick one key on a chosen partition or two keys on different
    /// partitions. Hash routing hashes the key space (the prefix before the last '/'), so the candidates vary
    /// that prefix rather than the leaf.
    /// </summary>
    private static Dictionary<int, string> FreshKeyPerPartition(KahunaManager probe, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        Dictionary<int, string> byPartition = [];

        for (int i = 0; i < 4_096 && byPartition.Count < Partitions; i++)
        {
            string candidate = $"{tag}{i}/{random}";
            int partition = probe.KeyValues.LocateDurablePartition(candidate).PartitionId;
            byPartition.TryAdd(partition, candidate);
        }

        return byPartition;
    }

    private static async Task<TransactionHandle> StartTransaction(
        KahunaManager session, string coordinatorKey, int timeoutMs, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = timeoutMs
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static async Task Write(
        KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, System.Text.Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    /// <summary>Moves the partition to a named peer and waits until that peer answers as its leader.</summary>
    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
    }

    /// <summary>
    /// Drives every node's recovery sweep until no node still holds a prepared intent for <paramref name="keys"/>.
    /// The sweep only acts on partitions the node leads, so every node is asked; settlement is idempotent, so
    /// asking a node that has nothing to do costs nothing.
    /// </summary>
    private static async Task SettleEverywhere(KahunaManager[] managers, string[] keys, CancellationToken ct)
    {
        await WaitUntilAsync(async () =>
        {
            foreach (KahunaManager manager in managers)
                await manager.KeyValues.RecoverPreparedIntents(ct);

            foreach (KahunaManager manager in managers)
                foreach (string key in keys)
                    if (manager.DurablePreparedIntentStore.Get(key) is not null)
                        return false;

            return true;
        }, timeoutMs: 60_000);
    }

    /// <summary>Every replica's local record for the transaction, so a caller can assert they name one winner.</summary>
    private static TransactionDecision?[] DecisionsOnEveryReplica(
        KahunaManager[] managers, HLCTimestamp transactionId)
    {
        TransactionDecision?[] decisions = new TransactionDecision?[managers.Length];
        for (int i = 0; i < managers.Length; i++)
            decisions[i] = managers[i].DurableTransactionRecordStore.Get(transactionId, 1)?.Decision;

        return decisions;
    }

    private static void AssertEveryReplicaAgrees(
        KahunaManager[] managers, HLCTimestamp transactionId, TransactionDecision expected)
    {
        TransactionDecision?[] decisions = DecisionsOnEveryReplica(managers, transactionId);
        for (int i = 0; i < decisions.Length; i++)
            Assert.Equal(expected, decisions[i]);
    }

    private static async Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> Read(
        KahunaManager reader, string key, CancellationToken ct) =>
        await RetryOnMustRetryAsync(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // The one-phase bundle: the whole transaction decides in one atomic batch.
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The bundled commit is durable at quorum and judged by the apply-time gate; the partition then changes
    /// leader before its proposer is answered. The verdict must be the same on the replica that inherits the
    /// partition as on the one that proposed it — the determinism the whole one-phase design rests on — the
    /// value must materialize exactly once, and the caller must never be told the transaction aborted.
    /// </summary>
    [Fact]
    public async Task HeldOnePhaseBundle_ThenLeadershipMoves_EveryReplicaKeepsTheSameCommitVerdict()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureRaft: config => config.ProposalTimeout = HoldBound,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = true);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "op1");
            (int partition, string key) = keys.First();
            int leader = await LeaderIndexOf(partition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];
            TransactionHandle handle = await StartTransaction(session, key, timeoutMs: 60_000, ct);
            await Write(session, handle, key, "alpha", ct);

            using HeldCommitReplies holds = new(rafts[leader], partition);

            Task<(KeyValueResponseType, string?)> commit = session.LocateAndCommitTransaction(handle, ct);

            // The record reaching Commit while the FIRST reply on this partition is still held is what proves
            // the bundle: a two-phase finalize's first proposal carries only the record initialize and the
            // prepare, so its record would still read Undecided here and its decision would never be proposed.
            await WaitUntilAsync(() =>
                managers[leader].DurableTransactionRecordStore.Get(handle.TransactionId, 1)?.Decision == TransactionDecision.Commit,
                timeoutMs: 30_000);

            Assert.Equal(1, holds.Count);
            Assert.False(commit.IsCompleted, "the finalize must still be waiting for the reply the hook is holding");

            // Durable at quorum means every replica judged it, not just the proposer.
            await WaitUntilAsync(() =>
            {
                foreach (TransactionDecision? decision in DecisionsOnEveryReplica(managers, handle.TransactionId))
                    if (decision != TransactionDecision.Commit)
                        return false;

                return true;
            }, timeoutMs: 30_000);

            await MoveLeadership(rafts, partition, leader, successor, ct);

            // Recovery on the new leader reaches the verdict the old leader's apply already reached.
            await SettleEverywhere(managers, [key], ct);
            AssertEveryReplicaAgrees(managers, handle.TransactionId, TransactionDecision.Commit);

            holds.ReleaseAll();

            (KeyValueResponseType commitType, _) = await commit;
            Assert.True(
                commitType is KeyValueResponseType.Committed or KeyValueResponseType.MustRetry,
                $"a committed transaction must never be answered {commitType}");

            // Exactly once: a first write settles at revision 0, so a re-applied settle would read as 1.
            foreach (KahunaManager reader in managers)
            {
                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await Read(reader, key, ct);
                Assert.Equal(KeyValueResponseType.Get, readType);
                Assert.Equal("alpha"u8.ToArray(), entry!.Value);
                Assert.Equal(0, entry.Revision);
            }

            foreach (KahunaManager manager in managers)
                Assert.Null(manager.DurablePreparedIntentStore.Get(key));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // The two-phase finalize: prepare, then a decision barrier of its own.
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The held commit wins. A two-phase decision is durable at quorum; leadership then moves, and recovery on
    /// the new leader arrives second. It must adopt the recorded commit rather than presume anything, every
    /// replica must name that commit, and both written values must materialize exactly once.
    /// </summary>
    [Fact]
    public async Task HeldTwoPhaseDecision_ThenLeadershipMoves_RecoveryAdoptsTheRecordedCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureRaft: config => config.ProposalTimeout = HoldBound);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            // Two keys on different partitions close the one-phase gate (multi-partition), which is what puts a
            // decision barrier of its own after the prepares. The first written key anchors the record.
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "tp1");
            Assert.True(keys.Count >= 2, "the fixture needs two partitions to force the two-phase path");

            (int anchorPartition, string anchorKey) = keys.First();
            string secondKey = keys.Skip(1).First().Value;

            int leader = await LeaderIndexOf(anchorPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];
            TransactionHandle handle = await StartTransaction(session, anchorKey, timeoutMs: 60_000, ct);
            await Write(session, handle, anchorKey, "alpha", ct);
            await Write(session, handle, secondKey, "beta", ct);

            // Let the anchor bundle (record initialize + prepare) answer, and hold the decision that follows it.
            using HeldCommitReplies holds = new(rafts[leader], anchorPartition, releaseFirst: 1);

            Task<(KeyValueResponseType, string?)> commit = session.LocateAndCommitTransaction(handle, ct);

            await WaitUntilAsync(() =>
                managers[leader].DurableTransactionRecordStore.Get(handle.TransactionId, 1)?.Decision == TransactionDecision.Commit,
                timeoutMs: 30_000);

            // The decision is the held proposal, and it is a proposal of its own: the anchor bundle was answered
            // before it (releaseFirst), which is exactly the two-phase shape.
            Assert.Equal(1, holds.Released);
            Assert.Equal(1, holds.Count);
            Assert.False(commit.IsCompleted, "the finalize must still be waiting for the decision's reply");

            await MoveLeadership(rafts, anchorPartition, leader, successor, ct);

            await SettleEverywhere(managers, [anchorKey, secondKey], ct);
            AssertEveryReplicaAgrees(managers, handle.TransactionId, TransactionDecision.Commit);

            holds.ReleaseAll();

            (KeyValueResponseType commitType, _) = await commit;
            Assert.True(
                commitType is KeyValueResponseType.Committed or KeyValueResponseType.MustRetry,
                $"a committed transaction must never be answered {commitType}");

            foreach (KahunaManager reader in managers)
            {
                (KeyValueResponseType anchorType, ReadOnlyKeyValueEntry? anchorEntry) = await Read(reader, anchorKey, ct);
                Assert.Equal(KeyValueResponseType.Get, anchorType);
                Assert.Equal("alpha"u8.ToArray(), anchorEntry!.Value);
                Assert.Equal(0, anchorEntry.Revision);

                (KeyValueResponseType secondType, ReadOnlyKeyValueEntry? secondEntry) = await Read(reader, secondKey, ct);
                Assert.Equal(KeyValueResponseType.Get, secondType);
                Assert.Equal("beta"u8.ToArray(), secondEntry!.Value);
                Assert.Equal(0, secondEntry.Revision);
            }

            foreach (KahunaManager manager in managers)
            {
                Assert.Null(manager.DurablePreparedIntentStore.Get(anchorKey));
                Assert.Null(manager.DurablePreparedIntentStore.Get(secondKey));
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The held commit loses. The prepare is durable at quorum and unanswered, so the finalize never reaches its
    /// decision; leadership moves, the frozen decision deadline passes, and recovery on the new leader drives the
    /// presumed abort first. When the prepare is finally answered the finalize must lose to that abort: the caller
    /// must never be told the transaction committed, no value may materialize, and no intent may be left behind.
    /// </summary>
    [Fact]
    public async Task HeldTwoPhasePrepare_ThenLeadershipMoves_RecoverysPresumedAbortIsTheCanonicalWinner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureRaft: config => config.ProposalTimeout = HoldBound);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "tp2");
            Assert.True(keys.Count >= 2, "the fixture needs two partitions to force the two-phase path");

            (int anchorPartition, string anchorKey) = keys.First();
            string secondKey = keys.Skip(1).First().Value;

            int leader = await LeaderIndexOf(anchorPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            // A short lifetime: the presumed abort is only allowed once the frozen decision deadline has passed,
            // and the fixture must reach that point rather than wait out a production-sized window.
            const int transactionTimeoutMs = 2_000;
            TransactionHandle handle = await StartTransaction(session, anchorKey, transactionTimeoutMs, ct);
            await Write(session, handle, anchorKey, "alpha", ct);
            await Write(session, handle, secondKey, "beta", ct);

            using HeldCommitReplies holds = new(rafts[leader], anchorPartition);

            Task<(KeyValueResponseType, string?)> commit = session.LocateAndCommitTransaction(handle, ct);

            // The anchor bundle applied — the intent is pending and the record exists — and its reply is held, so
            // the finalize is stuck before its decision. The record still reads Undecided, which is what
            // distinguishes this stage from the bundled-commit fixture above.
            await WaitUntilAsync(() =>
                managers[leader].DurablePreparedIntentStore.Get(anchorKey) is not null, timeoutMs: 30_000);

            Assert.Equal(TransactionDecision.Undecided,
                managers[leader].DurableTransactionRecordStore.Get(handle.TransactionId, 1)?.Decision);
            Assert.False(commit.IsCompleted, "the finalize must still be waiting for the prepare's reply");

            await MoveLeadership(rafts, anchorPartition, leader, successor, ct);

            // Past the frozen deadline the sweep owns the transaction and presumes abort.
            await Task.Delay(transactionTimeoutMs + 500, ct);
            await SettleEverywhere(managers, [anchorKey, secondKey], ct);
            AssertEveryReplicaAgrees(managers, handle.TransactionId, TransactionDecision.Abort);

            holds.ReleaseAll();

            (KeyValueResponseType commitType, _) = await commit;
            Assert.NotEqual(KeyValueResponseType.Committed, commitType);

            // The abort stands after the late answer, and nothing the transaction wrote survives.
            AssertEveryReplicaAgrees(managers, handle.TransactionId, TransactionDecision.Abort);

            foreach (KahunaManager reader in managers)
            {
                (KeyValueResponseType anchorType, _) = await Read(reader, anchorKey, ct);
                Assert.Equal(KeyValueResponseType.DoesNotExist, anchorType);

                (KeyValueResponseType secondType, _) = await Read(reader, secondKey, ct);
                Assert.Equal(KeyValueResponseType.DoesNotExist, secondType);
            }

            await SettleEverywhere(managers, [anchorKey, secondKey], ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
