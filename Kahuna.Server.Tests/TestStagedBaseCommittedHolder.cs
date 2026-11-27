using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The staged-base check that the finalizer runs before its first proposal meets a committed foreign intent on a
/// written key. The intent stays on the key until its settlement applies. In that window a non-transactional
/// write installs the intent's value and writes the next revision over it without a wait, so a read-modify-write
/// can validate a head that is newer than the intent. A transaction whose validated base is newer than the
/// intent lost no update to it, and its commit must go through. A committed intent above the base, or at the
/// base revision in another state, is a write the transaction did not see, and the commit must abort as a moved
/// base.
///
/// <para>An absent base records no revision. There the intent is compared by its effect: a committed delete is the
/// absence the transaction read, and a committed value is a conflict, also when a later delete covers it, until
/// the intent settles.</para>
/// </summary>
public sealed class TestStagedBaseCommittedHolder
{
    private const int LockExpiresMs = 30_000;

    private readonly ILoggerFactory loggerFactory;

    public TestStagedBaseCommittedHolder(ITestOutputHelper outputHelper)
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
        await node.WaitForLeaderForKeyAsync("sbh/seed", ct);

        return node;
    }

    private static string Bucket(string tag) => $"sbh-{Guid.NewGuid().ToString("N")[..8]}-{tag}";

    /// <summary>
    /// A committed transaction leaves its intent on the key while its settlement is held. A non-transactional
    /// write then writes the next revision over the intent, and a read-modify-write validates that newer head. The
    /// check finds the older committed intent on the key, and the commit goes through: the transaction read past
    /// the intent, so the intent is not a write it missed. When the settlement lands, the transaction's value is
    /// the key's value.
    /// </summary>
    /// <param name="twoPartitions">The transaction also writes a key on another partition, which keeps its commit
    /// on the two-phase path; false is the one-phase bundle.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HeadWrittenOverACommittedUnsettledIntent_ReadModifyWriteCommits(bool twoPartitions)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("head-past");
        string key = bucket + "/k";
        string? companion = twoPartitions ? CompanionOnAnotherPartition(node, key) : null;

        await Seed(kahuna, key, "v0", ct);
        if (companion is not null)
            await Seed(kahuna, companion, "c0", ct);

        using ResolutionGate gate = new(manager);

        TransactionHandle t1 = await StartTransaction(kahuna, bucket + "/t1", KeyValueTransactionLocking.Pessimistic, ct);
        await LockAndSet(kahuna, t1, key, "v1", ct);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(t1, ct)).Type);

        PreparedIntent? holder = manager.DurablePreparedIntentStore.Get(key);
        Assert.NotNull(holder);
        Assert.Equal(t1.TransactionId, holder.TransactionId);

        await Seed(kahuna, key, "v2", ct);
        Assert.Equal(t1.TransactionId, manager.DurablePreparedIntentStore.Get(key)?.TransactionId);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", KeyValueTransactionLocking.Optimistic, ct);
        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? read) = await TxGet(kahuna, t2, key, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal("v2", Text(read));
        Assert.True(read!.Revision > holder.Revision, $"the head revision {read.Revision} must be above the intent revision {holder.Revision}");

        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, t2, key, "v2+t2", ct));
        if (companion is not null)
            Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, t2, companion, "c1", ct));

        TransactionCommitAnswer answer;
        using (CheckPasses passes = new(manager))
        {
            answer = await kahuna.LocateAndCommitTransaction(t2, ct);
            Assert.True(passes.Count > 0, "the staged-base check must pass over the older committed intent");
        }

        TestContext.Current.TestOutputHelper?.WriteLine($"commit: {answer.Type} ({answer.Refusal}: {answer.Reason})");

        Assert.Equal(KeyValueResponseType.Committed, answer.Type);
        Assert.Equal(CommitRefusalClass.None, answer.Refusal);

        gate.Release();
        await WaitUntil(() => manager.DurablePreparedIntentStore.Get(key) is null);

        Assert.Equal("v2+t2", await ReadCommitted(kahuna, key, ct));
        if (companion is not null)
            Assert.Equal("c1", await ReadCommitted(kahuna, companion, ct));
    }

    /// <summary>
    /// A committed intent one revision above the validated base is a write that the transaction did not see. The
    /// commit aborts as a moved base, on the one-phase and on the two-phase path.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CommittedHolderAboveTheBase_IsRefusedAsAMovedBase(bool twoPartitions)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("above");
        string key = bucket + "/k";
        string? companion = twoPartitions ? CompanionOnAnotherPartition(node, key) : null;

        await Seed(kahuna, key, "v0", ct);
        if (companion is not null)
            await Seed(kahuna, companion, "c0", ct);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", KeyValueTransactionLocking.Optimistic, ct);
        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? read) = await TxGet(kahuna, t2, key, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);

        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, t2, key, "v0+t2", ct));
        if (companion is not null)
            Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, t2, companion, "c1", ct));

        await ImportCommittedIntent(manager, key, KeyValueState.Set, revision: read!.Revision + 1, ct);

        TransactionCommitAnswer answer = await CommitRefusedByTheCheck(manager, t2, ct);
        TestContext.Current.TestOutputHelper?.WriteLine($"commit: {answer.Type} ({answer.Refusal}: {answer.Reason})");

        Assert.Equal(KeyValueResponseType.Aborted, answer.Type);
        Assert.Equal(CommitRefusalClass.StagedBaseMoved, answer.Refusal);
    }

    /// <summary>
    /// A committed tombstone at the revision of a validated value is not that value. The equal revision is no
    /// proof, and the commit aborts as a moved base.
    /// </summary>
    [Fact]
    public async Task CommittedTombstoneAtTheBaseRevision_IsRefusedAsAMovedBase()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("same-rev");
        string key = bucket + "/k";
        await Seed(kahuna, key, "v0", ct);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", KeyValueTransactionLocking.Optimistic, ct);
        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? read) = await TxGet(kahuna, t2, key, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, t2, key, "v0+t2", ct));

        await ImportCommittedIntent(manager, key, KeyValueState.Deleted, revision: read!.Revision, ct);

        TransactionCommitAnswer answer = await CommitRefusedByTheCheck(manager, t2, ct);
        TestContext.Current.TestOutputHelper?.WriteLine($"commit: {answer.Type} ({answer.Refusal}: {answer.Reason})");

        Assert.Equal(KeyValueResponseType.Aborted, answer.Type);
        Assert.Equal(CommitRefusalClass.StagedBaseMoved, answer.Refusal);
    }

    /// <summary>
    /// The transaction read the key as absent, and a committed value is then on it: an insert that the
    /// transaction did not see. The commit aborts as a moved base.
    /// </summary>
    [Fact]
    public async Task AbsentBase_CommittedValueHolder_IsRefusedAsAMovedBase()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("absent-value");
        string key = bucket + "/k";

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", KeyValueTransactionLocking.Optimistic, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await TxGet(kahuna, t2, key, ct)).Type);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, t2, key, "t2", ct));

        await ImportCommittedIntent(manager, key, KeyValueState.Set, revision: 0, ct);

        TransactionCommitAnswer answer = await CommitRefusedByTheCheck(manager, t2, ct);
        TestContext.Current.TestOutputHelper?.WriteLine($"commit: {answer.Type} ({answer.Refusal}: {answer.Reason})");

        Assert.Equal(KeyValueResponseType.Aborted, answer.Type);
        Assert.Equal(CommitRefusalClass.StagedBaseMoved, answer.Refusal);
    }

    /// <summary>
    /// A committed delete on the key is the absence that the transaction read. The insert commits over it.
    /// </summary>
    [Fact]
    public async Task AbsentBase_CommittedDeleteHolder_Commits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("absent-delete");
        string key = bucket + "/k";
        await Seed(kahuna, key, "v0", ct);
        long seeded = (await LatestCommitted(kahuna, key, ct)).Revision;

        await ImportCommittedIntent(manager, key, KeyValueState.Deleted, revision: seeded + 1, ct);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", KeyValueTransactionLocking.Optimistic, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await TxGet(kahuna, t2, key, ct)).Type);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, t2, key, "t2", ct));

        TransactionCommitAnswer answer;
        using (CheckPasses passes = new(manager))
        {
            answer = await kahuna.LocateAndCommitTransaction(t2, ct);
            Assert.True(passes.Count > 0, "the staged-base check must pass over the committed delete");
        }

        TestContext.Current.TestOutputHelper?.WriteLine($"commit: {answer.Type} ({answer.Refusal}: {answer.Reason})");

        Assert.Equal(KeyValueResponseType.Committed, answer.Type);
        await WaitUntil(() => manager.DurablePreparedIntentStore.Get(key) is null);
        Assert.Equal("t2", await ReadCommitted(kahuna, key, ct));
    }

    /// <summary>
    /// A committed value stays on the key while its settlement is held, and a non-transactional delete removes the
    /// key over it. A transaction reads the key as absent and inserts it. An absent base records no revision, so
    /// the check cannot tell this value from an insert that the transaction did not see, and the commit aborts as
    /// a moved base.
    /// </summary>
    [Fact]
    public async Task AbsentBase_CommittedValueUnderANewerDelete_IsRefused()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("absent-under-delete");
        string key = bucket + "/k";
        await Seed(kahuna, key, "v0", ct);

        using ResolutionGate gate = new(manager);

        TransactionHandle t1 = await StartTransaction(kahuna, bucket + "/t1", KeyValueTransactionLocking.Pessimistic, ct);
        await LockAndSet(kahuna, t1, key, "v1", ct);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(t1, ct)).Type);
        Assert.Equal(t1.TransactionId, manager.DurablePreparedIntentStore.Get(key)?.TransactionId);

        (KeyValueResponseType deleteType, _, _) = await kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Deleted, deleteType);
        Assert.Equal(t1.TransactionId, manager.DurablePreparedIntentStore.Get(key)?.TransactionId);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", KeyValueTransactionLocking.Optimistic, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await TxGet(kahuna, t2, key, ct)).Type);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, t2, key, "t2", ct));

        TransactionCommitAnswer refused = await CommitRefusedByTheCheck(manager, t2, ct);
        TestContext.Current.TestOutputHelper?.WriteLine($"commit: {refused.Type} ({refused.Refusal}: {refused.Reason})");

        Assert.Equal(KeyValueResponseType.Aborted, refused.Type);
        Assert.Equal(CommitRefusalClass.StagedBaseMoved, refused.Refusal);
    }

    /// <summary>
    /// Commits <paramref name="tx"/> and asserts that the staged-base check before the first proposal did not
    /// pass. The fence at the prepare's apply position refuses a moved base with the same refusal class, so the
    /// answer alone does not tell which check refused.
    /// </summary>
    private static async Task<TransactionCommitAnswer> CommitRefusedByTheCheck(KahunaManager manager, TransactionHandle tx, CancellationToken ct)
    {
        using CheckPasses passes = new(manager);
        TransactionCommitAnswer answer = await manager.LocateAndCommitTransaction(tx, ct);
        Assert.Equal(0, passes.Count);
        return answer;
    }

    /// <summary>
    /// Puts a committed transaction's intent on <paramref name="key"/> in the settlement window: the intent is
    /// pending and its canonical record says commit.
    /// </summary>
    private static async Task ImportCommittedIntent(
        KahunaManager manager, string key, KeyValueState state, long revision, CancellationToken ct)
    {
        string anchor = key + "/holder";
        TransactionHandle holder = await StartTransaction(manager, anchor, KeyValueTransactionLocking.Optimistic, ct);
        HLCTimestamp txId = holder.TransactionId;

        PreparedIntent intent = new(
            txId, Epoch: 0, key, ManifestHash: 0, RecordAnchorKey: anchor,
            CommitTimestamp: txId, State: state, Value: state == KeyValueState.Set ? "holder"u8.ToArray() : null,
            Bucket: null, Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: revision - 1, BaseState: revision == 0 ? KeyValueState.Undefined : KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

        manager.DurablePreparedIntentStore.ImportIntents([intent]);
        manager.DurableTransactionRecordStore.ImportRecords([new TransactionRecord(
            txId, Epoch: 0, CoordinatorKey: anchor, RecordAnchorKey: anchor,
            CommitTimestamp: txId, DecisionDeadline: HLCTimestamp.Zero, ManifestHash: 0,
            Participants: [new TransactionParticipantRef(key, KeyValueDurability.Persistent)], ManifestPresent: true,
            Decision: TransactionDecision.Commit, AbortClass: TransactionAbortClass.None, WinningOpId: txId,
            CreatedAt: txId, DecidedAt: txId)]);
    }

    private static string CompanionOnAnotherPartition(EmbeddedKahunaNode node, string key)
    {
        int keyPartition = node.Raft.GetPartitionKey(key);
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"{key}-companion{i}/k";
            if (node.Raft.GetPartitionKey(candidate) != keyPartition)
                return candidate;
        }

        throw new InvalidOperationException($"no companion key routes away from the partition of {key}");
    }

    private static async Task<TransactionHandle> StartTransaction(IKahuna kahuna, string coordinatorKey, KeyValueTransactionLocking locking, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = locking,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        return handle;
    }

    private static async Task LockAndSet(IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            tx.TransactionId, key, LockExpiresMs, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, lockType);

        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, tx, key, value, ct));
    }

    private static async Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> TxGet(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct) =>
        await kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

    private static async Task<KeyValueResponseType> TxSet(IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        return type;
    }

    private static async Task Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task<ReadOnlyKeyValueEntry> LatestCommitted(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, type);
        return entry!;
    }

    private static async Task<string> ReadCommitted(IKahuna kahuna, string key, CancellationToken ct) =>
        Text(await LatestCommitted(kahuna, key, ct));

    private static string Text(ReadOnlyKeyValueEntry? entry) =>
        entry?.Value is null ? "" : Encoding.UTF8.GetString(entry.Value);

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }

        Assert.True(predicate(), "condition not met in time");
    }

    /// <summary>Counts the finalizes whose staged-base check before the first proposal passed, while installed.
    /// The finalizer calls the hook only after that check answers valid.</summary>
    private sealed class CheckPasses : IDisposable
    {
        private readonly DurableTransactionFinalizer finalizer;

        private int count;

        public CheckPasses(KahunaManager manager)
        {
            finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestAfterPreValidationHook = _ =>
            {
                Interlocked.Increment(ref count);
                return Task.CompletedTask;
            };
        }

        public int Count => Volatile.Read(ref count);

        public void Dispose() => finalizer.TestAfterPreValidationHook = null;
    }

    /// <summary>Holds every deferred resolution scheduled while installed, so a commit's decision→settlement
    /// window stays open until <see cref="Release"/>. Disposal releases and uninstalls the hook.</summary>
    private sealed class ResolutionGate : IDisposable
    {
        private readonly DurableTransactionFinalizer finalizer;

        private readonly TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ResolutionGate(KahunaManager manager)
        {
            finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestBeforeDeferredResolutionHook = ct => open.Task.WaitAsync(ct);
        }

        public void Release()
        {
            finalizer.TestBeforeDeferredResolutionHook = null;
            open.TrySetResult();
        }

        public void Dispose() => Release();
    }
}
