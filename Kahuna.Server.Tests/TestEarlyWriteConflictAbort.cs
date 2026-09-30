using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// An optimistic transaction that read a key, and then re-reads or writes it after another transaction committed a
/// newer revision of that key, is answered <c>Aborted</c> at that statement rather than only at commit through
/// read-set validation. The early answer rests on the MVCC pin the first read leaves behind, so every first
/// transactional read must record one — including a read met by a committed durable intent that has not been
/// settled yet, which a transaction begun right after another commit on the same key runs into.
///
/// <para>The settlement window is built directly in the durable stores (a pending intent plus its committed
/// canonical record) so it stays open for the whole test instead of closing on whatever the background
/// resolution finishes first.</para>
/// </summary>
public sealed class TestEarlyWriteConflictAbort
{
    private readonly ILoggerFactory loggerFactory;

    public TestEarlyWriteConflictAbort(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("early/abort", ct);
        return node;
    }

    private static string NewKey() => "early/abort/" + Guid.NewGuid().ToString("N")[..8];

    private static async Task<TransactionHandle> StartOptimistic(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Timeout = 10_000, Locking = KeyValueTransactionLocking.Optimistic },
            ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    /// <summary>
    /// Starts optimistic transactions back to back until one is minted with the requested HLC counter shape, and
    /// rolls back every other one. Ids minted in one busy millisecond differ only in the counter.
    /// </summary>
    private static async Task<TransactionHandle> StartWithCounter(IKahuna kahuna, string coordinatorKey, bool nonZeroCounter, CancellationToken ct)
    {
        for (int i = 0; i < 10_000; i++)
        {
            TransactionHandle handle = await StartOptimistic(kahuna, coordinatorKey, ct);
            if ((handle.TransactionId.C > 0) == nonZeroCounter)
                return handle;

            await kahuna.LocateAndRollbackTransaction(handle, ct);
        }

        Assert.Fail($"could not mint a transaction id with nonZeroCounter={nonZeroCounter}");
        return default;
    }

    private static Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TxGet(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct) =>
        kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom());

    private static Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TxExists(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct) =>
        kahuna.LocateAndTryExistsValue(
            tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom());

    private static async Task<KeyValueResponseType> TxSet(IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        return type;
    }

    private static async Task<long> AutoCommitSet(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, long revision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return revision;
    }

    private static async Task<ReadOnlyKeyValueEntry> LatestCommitted(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        return entry!;
    }

    /// <summary>A second optimistic transaction writes the key and commits.</summary>
    private static async Task CommitCompetingWrite(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        TransactionHandle winner = await StartOptimistic(kahuna, key + "/winner", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, winner, key, value, ct));
        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(winner, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);
    }

    /// <summary>
    /// Leaves a committed transaction's value for <paramref name="key"/> in the settlement window: its prepared
    /// intent is still pending while its canonical record already says commit.
    /// </summary>
    private static async Task ImportCommittedUnsettledIntent(
        KahunaManager kahuna, string key, long revision, string value, CancellationToken ct)
    {
        string anchor = key + "/committer";
        TransactionHandle committer = await StartOptimistic(kahuna, anchor, ct);
        HLCTimestamp txId = committer.TransactionId;

        kahuna.DurablePreparedIntentStore.ImportIntents([new PreparedIntent(
            txId, Epoch: 0, key, ManifestHash: 0, RecordAnchorKey: anchor,
            CommitTimestamp: txId, State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes(value), Bucket: null,
            Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: revision - 1,
            BaseState: revision == 0 ? KeyValueState.Undefined : KeyValueState.Set, RecoveryDeadline: HLCTimestamp.Zero,
            Resolution: PreparedIntentResolution.Pending)]);

        kahuna.DurableTransactionRecordStore.ImportRecords([new TransactionRecord(
            txId, Epoch: 0, CoordinatorKey: anchor, RecordAnchorKey: anchor,
            CommitTimestamp: txId, DecisionDeadline: HLCTimestamp.Zero, ManifestHash: 0,
            Participants: [new TransactionParticipantRef(key, KeyValueDurability.Persistent)], ManifestPresent: true,
            Decision: TransactionDecision.Commit, AbortClass: TransactionAbortClass.None, WinningOpId: txId,
            CreatedAt: txId, DecidedAt: txId)]);
    }

    /// <summary>
    /// Control: with no durable intent on the key, the loser's re-read and its write are both aborted, whatever the
    /// shape of its transaction id.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task NoIntent_ReReadOrWriteAfterACompetingCommit_IsAborted(bool nonZeroCounter, bool write)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle loser = await StartWithCounter(kahuna, key, nonZeroCounter, ct);
        Assert.Equal(KeyValueResponseType.Get, (await TxGet(kahuna, loser, key, ct)).Item1);

        await CommitCompetingWrite(kahuna, key, "winner", ct);

        KeyValueResponseType answer = write
            ? await TxSet(kahuna, loser, key, "loser", ct)
            : (await TxGet(kahuna, loser, key, ct)).Item1;
        Assert.Equal(KeyValueResponseType.Aborted, answer);
    }

    /// <summary>
    /// The loser's first read meets a committed intent from an earlier transaction that is already reflected in the
    /// resident head but not settled yet — the state right after a commit on the key. The read must still pin, so a
    /// competing commit that lands afterwards aborts the loser's next read or write.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MaterializedUnsettledIntent_ReReadOrWriteAfterACompetingCommit_IsAborted(bool write)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        string key = NewKey();

        long head = await AutoCommitSet(kahuna, key, "seed", ct);
        await ImportCommittedUnsettledIntent(kahuna, key, head, "seed", ct);

        TransactionHandle loser = await StartOptimistic(kahuna, key, ct);
        (KeyValueResponseType firstRead, ReadOnlyKeyValueEntry? first) = await TxGet(kahuna, loser, key, ct);
        Assert.Equal(KeyValueResponseType.Get, firstRead);
        Assert.Equal(head, first!.Revision);

        await CommitCompetingWrite(kahuna, key, "winner", ct);
        Assert.True((await LatestCommitted(kahuna, key, ct)).Revision > head);

        KeyValueResponseType answer = write
            ? await TxSet(kahuna, loser, key, "loser", ct)
            : (await TxGet(kahuna, loser, key, ct)).Item1;
        Assert.Equal(KeyValueResponseType.Aborted, answer);
    }

    /// <summary>
    /// The loser's first read meets a committed intent that has not reached the resident head yet. It observes the
    /// committed value, keeps observing it on a re-read (no spurious abort while the intent is unsettled), and is
    /// aborted once a later commit supersedes it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnmaterializedCommittedIntent_ReadsItsValue_ThenAbortsAfterACompetingCommit(bool write)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        string key = NewKey();

        long head = await AutoCommitSet(kahuna, key, "seed", ct);
        await ImportCommittedUnsettledIntent(kahuna, key, head + 1, "committed", ct);

        TransactionHandle loser = await StartOptimistic(kahuna, key, ct);
        (KeyValueResponseType firstRead, ReadOnlyKeyValueEntry? first) = await TxGet(kahuna, loser, key, ct);
        Assert.Equal(KeyValueResponseType.Get, firstRead);
        Assert.Equal(head + 1, first!.Revision);
        Assert.Equal("committed"u8.ToArray(), first.Value);

        (KeyValueResponseType reRead, ReadOnlyKeyValueEntry? again) = await TxGet(kahuna, loser, key, ct);
        Assert.Equal(KeyValueResponseType.Get, reRead);
        Assert.Equal(head + 1, again!.Revision);

        await CommitCompetingWrite(kahuna, key, "winner", ct);
        Assert.True((await LatestCommitted(kahuna, key, ct)).Revision > head + 1);

        KeyValueResponseType answer = write
            ? await TxSet(kahuna, loser, key, "loser", ct)
            : (await TxGet(kahuna, loser, key, ct)).Item1;
        Assert.Equal(KeyValueResponseType.Aborted, answer);
    }

    /// <summary>
    /// A key that exists only as a committed-but-unsettled intent (nothing on disk, nothing resident), with the
    /// commit recorded in this node's committed-head memory as a real commit is. The first transactional read loads
    /// the empty backend row, which is behind that memory only because the intent has not settled: it must pin the
    /// intent rather than refuse the row as stale and retry until settlement. A later committed write then aborts
    /// the reader's next read.
    /// </summary>
    [Fact]
    public async Task ColdIntentOnlyKey_FirstTransactionalReadPinsTheCommittedIntent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        string key = NewKey();

        PreparedIntent committed = new(
            TransactionId: new HLCTimestamp(0, 100, 0), Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
            CommitTimestamp: new HLCTimestamp(0, 200, 0), State: KeyValueState.Set, Value: "committed"u8.ToArray(),
            Bucket: null, Revision: 0, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: -1,
            BaseState: KeyValueState.Undefined, RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0),
            Resolution: PreparedIntentResolution.Pending);
        kahuna.DurablePreparedIntentStore.Apply(new PrepareIntentCommand(committed));
        kahuna.DurablePreparedIntentStore.Apply(new ResolveIntentCommand(committed.TransactionId, committed.Epoch, key, Commit: true));

        TransactionHandle reader = await StartOptimistic(kahuna, key, ct);
        (KeyValueResponseType firstRead, ReadOnlyKeyValueEntry? first) = await TxGet(kahuna, reader, key, ct);
        Assert.Equal(KeyValueResponseType.Get, firstRead);
        Assert.Equal("committed"u8.ToArray(), first!.Value);
        Assert.Equal(0, first.Revision);

        Assert.Equal(KeyValueResponseType.Get, (await TxGet(kahuna, reader, key, ct)).Item1);

        await AutoCommitSet(kahuna, key, "newer", ct);

        Assert.Equal(KeyValueResponseType.Aborted, (await TxGet(kahuna, reader, key, ct)).Item1);
    }

    /// <summary>
    /// A resident head that already moved past a lingering committed intent is what a first read observes: the read
    /// neither serves the intent's older value nor aborts against its own pin.
    /// </summary>
    [Fact]
    public async Task HeadNewerThanALingeringIntent_FirstReadServesTheHead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        string key = NewKey();

        long first = await AutoCommitSet(kahuna, key, "old", ct);
        long head = await AutoCommitSet(kahuna, key, "new", ct);
        Assert.True(head > first);
        await ImportCommittedUnsettledIntent(kahuna, key, first, "old", ct);

        TransactionHandle reader = await StartOptimistic(kahuna, key, ct);
        (KeyValueResponseType read, ReadOnlyKeyValueEntry? entry) = await TxGet(kahuna, reader, key, ct);
        Assert.Equal(KeyValueResponseType.Get, read);
        Assert.Equal(head, entry!.Revision);
        Assert.Equal("new"u8.ToArray(), entry.Value);

        (KeyValueResponseType exists, ReadOnlyKeyValueEntry? existsEntry) = await TxExists(kahuna, reader, key, ct);
        Assert.Equal(KeyValueResponseType.Exists, exists);
        Assert.Equal(head, existsEntry!.Revision);
    }

    /// <summary>
    /// The existence check pins exactly as the read does, including through an unsettled committed intent.
    /// </summary>
    [Fact]
    public async Task Exists_ThroughAnUnsettledIntent_AbortsAfterACompetingCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        string key = NewKey();

        long head = await AutoCommitSet(kahuna, key, "seed", ct);
        await ImportCommittedUnsettledIntent(kahuna, key, head, "seed", ct);

        TransactionHandle loser = await StartOptimistic(kahuna, key, ct);
        Assert.Equal(KeyValueResponseType.Exists, (await TxExists(kahuna, loser, key, ct)).Item1);

        await CommitCompetingWrite(kahuna, key, "winner", ct);

        Assert.Equal(KeyValueResponseType.Aborted, (await TxExists(kahuna, loser, key, ct)).Item1);
    }

    /// <summary>
    /// A key the transaction observed as absent and another transaction then created aborts the next existence
    /// check instead of reporting the pinned absence.
    /// </summary>
    [Fact]
    public async Task Exists_OfAKeyCreatedByACompetingCommit_IsAborted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();

        TransactionHandle loser = await StartOptimistic(kahuna, key, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await TxExists(kahuna, loser, key, ct)).Item1);

        await CommitCompetingWrite(kahuna, key, "created", ct);

        Assert.Equal(KeyValueResponseType.Aborted, (await TxExists(kahuna, loser, key, ct)).Item1);
    }
}
