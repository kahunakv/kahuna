using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Lock-acquisition behaviour across the deferred-settlement decision→settlement window. A durable commit returns
/// as soon as its decision is durable; the write intent its participants hold is cleared later, when the background
/// resolution applies the committed value. A transaction that locks one of those keys inside that window is not
/// contending with a live writer — the holder is already decided — so the acquire must treat it as the transient
/// condition it is instead of reporting the key as already locked, which aborts the caller for nothing.
///
/// The state is built directly in the durable stores so the window stays open for the whole test rather than
/// closing on whatever the background resolution manages to finish first. The companion
/// <see cref="TestDeferredSettlementWindowOperations"/> covers the value-visibility side (delete, unique insert);
/// this covers the lock the holder leaves behind.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDeferredSettlementLockWindow
{
    private readonly ILoggerFactory loggerFactory;

    public TestDeferredSettlementLockWindow(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("lockwin", ct);
        return node;
    }

    /// <summary>
    /// A holder whose transaction has no terminal decision is a genuine concurrent writer: the acquire is denied
    /// immediately so the caller fails fast instead of waiting on a transaction that may run to its full timeout.
    /// This is the control for <see cref="AcquireExclusiveLock_WhenHolderIsDurablyDecided_IsNotReportedAsLocked"/> —
    /// without it, granting the wait could be mistaken for correct behaviour when it is merely indiscriminate.
    /// </summary>
    [Fact]
    public async Task AcquireExclusiveLock_WhenHolderIsUndecided_IsReportedAsLocked()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "lockwin/undecided";
        HLCTimestamp holder = await LockAsNewTransaction(kahuna, key, ct);
        kahuna.DurablePreparedIntentStore.ImportIntents([PendingIntent(holder, key)]);

        Assert.Equal(KeyValueResponseType.AlreadyLocked, await AcquireAsNewTransaction(kahuna, key, "lockwin/undecided/second", ct));
    }

    /// <summary>
    /// The same holder, now with a committed canonical record: its intent survives only until resolution runs, so
    /// the acquire must not answer AlreadyLocked. Resolution never runs in this constructed state, so the acquire
    /// waits out its budget and reports the retryable MustRetry — the point being that the caller is told to retry
    /// rather than told it lost a lock race.
    /// </summary>
    [Fact]
    public async Task AcquireExclusiveLock_WhenHolderIsDurablyDecided_IsNotReportedAsLocked()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "lockwin/decided";
        HLCTimestamp holder = await LockAsNewTransaction(kahuna, key, ct);
        kahuna.DurablePreparedIntentStore.ImportIntents([PendingIntent(holder, key)]);
        kahuna.DurableTransactionRecordStore.ImportRecords([CommittedRecord(holder, key)]);

        KeyValueResponseType type = await AcquireAsNewTransaction(kahuna, key, "lockwin/decided/second", ct);

        Assert.NotEqual(KeyValueResponseType.AlreadyLocked, type);
        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    /// <summary>
    /// End to end through the script path: a pessimistic transaction locks every key it writes, so rewriting the
    /// key a durable commit just wrote must commit rather than abort on the lock that commit has not released yet.
    /// </summary>
    [Fact]
    public async Task LockingWrite_ImmediatelyAfterCommitOnSameKey_Commits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        for (int round = 0; round < 25; round++)
        {
            string key = $"lockwin/seq/{round}";

            Assert.Equal(KeyValueResponseType.Set, (await Run(node, $"BEGIN SET `{key}` 'v1' COMMIT END")).Type);
            Assert.Equal(KeyValueResponseType.Set, (await Run(node, $"BEGIN SET `{key}` 'v2' COMMIT END")).Type);
        }

        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Count == 0);

        KeyValueTransactionResult read = await Run(node, "GET `lockwin/seq/24`");
        Assert.Equal(KeyValueResponseType.Get, read.Type);
        Assert.Equal("v2"u8.ToArray(), read.Value);
    }

    private static Task<KeyValueTransactionResult> Run(EmbeddedKahunaNode node, string script) =>
        node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

    /// <summary>Starts a transaction and has it take the point lock on the key, returning its identity.</summary>
    private static async Task<HLCTimestamp> LockAsNewTransaction(KahunaManager kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = key + "/holder", Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            handle.TransactionId, key, 60_000, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Locked, lockType);

        return handle.TransactionId;
    }

    private static async Task<KeyValueResponseType> AcquireAsNewTransaction(
        KahunaManager kahuna, string key, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            handle.TransactionId, key, 10_000, KeyValueDurability.Persistent, ct);

        return lockType;
    }

    private static PreparedIntent PendingIntent(HLCTimestamp transactionId, string key) => new(
        transactionId, Epoch: 0, key, ManifestHash: 0, RecordAnchorKey: key + "/holder",
        CommitTimestamp: transactionId, State: KeyValueState.Set, Value: "v1"u8.ToArray(), Bucket: null,
        Revision: 0, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: -1,
        BaseState: KeyValueState.Undefined, RecoveryDeadline: HLCTimestamp.Zero,
        Resolution: PreparedIntentResolution.Pending);

    private static TransactionRecord CommittedRecord(HLCTimestamp transactionId, string key) => new(
        transactionId, Epoch: 0, CoordinatorKey: key + "/holder", RecordAnchorKey: key + "/holder",
        CommitTimestamp: transactionId, DecisionDeadline: HLCTimestamp.Zero, ManifestHash: 0,
        Participants: [new TransactionParticipantRef(key, KeyValueDurability.Persistent)], ManifestPresent: true,
        Decision: TransactionDecision.Commit, AbortClass: TransactionAbortClass.None, WinningOpId: transactionId,
        CreatedAt: transactionId, DecidedAt: transactionId);

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
}
