using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Operational-gate coverage for the deferred-settlement decision→settlement window: the write-side analogs of
/// read-your-writes. A durable transaction commits and its value lingers as a committed-but-unsettled prepared
/// intent until background settlement materializes it. An operation that touches that key in the window must resolve
/// the committed value correctly — delete must see it (and end absent), a unique insert (SET NX) must see it exists
/// (and be rejected), and after a committed delete the unique slot must be reusable. Read and update over the window
/// are covered by <see cref="TestDeferredSettlementReadYourWrite"/>; this covers delete and unique-index reuse.
/// </summary>
public sealed class TestDeferredSettlementWindowOperations
{
    private readonly ILoggerFactory loggerFactory;

    public TestDeferredSettlementWindowOperations(ITestOutputHelper outputHelper)
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
        await node.WaitForLeaderForKeyAsync("gate", ct);
        return node;
    }

    // A durable transaction that touches a key still occupied by a prior commit's committed-but-unsettled intent may
    // transiently return the retryable MustRetry (one live intent per key) until settlement frees it; the operation
    // lands on retry. Retry the whole self-contained script, keeping assertions strict.
    private static async Task<KeyValueTransactionResult> Run(EmbeddedKahunaNode node, string script)
    {
        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        for (int attempt = 1; result.Type == KeyValueResponseType.MustRetry && attempt < 40; attempt++)
        {
            await Task.Delay(Math.Min(5 * attempt, 50));
            result = await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        }

        return result;
    }

    [Fact]
    public async Task Delete_InDeferredWindow_SeesCommittedValue_ThenAbsent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        // Durable transaction commits gate/del=v1 at revision 0; under deferred settlement its value is a prepared
        // intent not yet materialized into MVCC.
        KeyValueTransactionResult first = await Run(node, "BEGIN SET `gate/del` 'v1' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, first.Type);

        // Delete the key while its commit may still be unsettled: the delete must observe the committed value and
        // end the key absent — never delete a nonexistent key or miss the committed value.
        KeyValueTransactionResult del = await Run(node, "BEGIN DELETE `gate/del` COMMIT END");
        Assert.Equal(KeyValueResponseType.Deleted, del.Type);

        KeyValueTransactionResult after = await Run(node, "GET `gate/del`");
        Assert.Equal(KeyValueResponseType.DoesNotExist, after.Type);

        // Settlement fully drains and the key stays absent.
        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Count == 0);
        KeyValueTransactionResult settled = await Run(node, "GET `gate/del`");
        Assert.Equal(KeyValueResponseType.DoesNotExist, settled.Type);
    }

    [Fact]
    public async Task UniqueInsert_InDeferredWindow_RejectedWhileCommittedKeyPending()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        // Commit gate/uniq=v1 (deferred: intent lingers).
        Assert.Equal(KeyValueResponseType.Set, (await Run(node, "BEGIN SET `gate/uniq` 'v1' COMMIT END")).Type);

        // A unique insert (SET ... NX) of the same key in the window must observe that the key already exists — the
        // committed-but-unsettled value counts as present — and be rejected, never overwriting it.
        KeyValueTransactionResult nx = await Run(node, "BEGIN SET `gate/uniq` 'v2' NX COMMIT END");
        Assert.Equal(KeyValueResponseType.NotSet, nx.Type);

        // The original committed value survives, and settlement leaves it intact.
        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Count == 0);
        KeyValueTransactionResult read = await Run(node, "GET `gate/uniq`");
        Assert.Equal(KeyValueResponseType.Get, read.Type);
        Assert.Equal("v1"u8.ToArray(), read.Value);
    }

    [Fact]
    public async Task UniqueReuse_AfterCommittedDeleteInDeferredWindow_InsertSucceeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        // Occupy the unique slot, then delete it — the delete itself commits durably (deferred).
        Assert.Equal(KeyValueResponseType.Set, (await Run(node, "BEGIN SET `gate/reuse` 'v1' COMMIT END")).Type);
        Assert.Equal(KeyValueResponseType.Deleted, (await Run(node, "BEGIN DELETE `gate/reuse` COMMIT END")).Type);

        // Reuse the slot with a unique insert while the delete may still be unsettled: the committed delete must be
        // visible so the key reads as absent and the NX insert succeeds.
        KeyValueTransactionResult reinsert = await Run(node, "BEGIN SET `gate/reuse` 'v3' NX COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, reinsert.Type);

        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Count == 0);
        KeyValueTransactionResult read = await Run(node, "GET `gate/reuse`");
        Assert.Equal(KeyValueResponseType.Get, read.Type);
        Assert.Equal("v3"u8.ToArray(), read.Value);
    }

    /// <summary>
    /// The window pinned open through the durable stores, so the finalize of the unique insert always meets the
    /// committed delete's intent still held on the key: a pessimistic script carries the base its lock observed,
    /// which is "absent" once the grant materialized the committed tombstone, and the finalize's pre-propose
    /// staged-base check must recognise that committed delete as exactly that base — not as a competitor that
    /// overtook it — so the insert commits instead of aborting as a conflict that never happened.
    /// </summary>
    [Fact]
    public async Task UniqueReuse_OverHeldCommittedDeleteIntent_InsertSucceeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "gate/reuse-held";

        (KeyValueResponseType startType, TransactionHandle holder) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = key + "/holder", Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        // A committed delete of the key, decided and not yet settled: its tombstone lives only in the intent.
        kahuna.DurablePreparedIntentStore.ImportIntents([CommittedDeleteIntent(holder.TransactionId, key)]);
        kahuna.DurableTransactionRecordStore.ImportRecords([CommittedRecord(holder.TransactionId, key)]);

        KeyValueTransactionResult reinsert = await Run(node, $"BEGIN SET `{key}` 'v3' NX COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, reinsert.Type);

        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Count == 0);
        KeyValueTransactionResult read = await Run(node, $"GET `{key}`");
        Assert.Equal(KeyValueResponseType.Get, read.Type);
        Assert.Equal("v3"u8.ToArray(), read.Value);
    }

    /// <summary>
    /// A pessimistic transaction that scans a bucket takes an exclusive range lock, which stamps its write intent
    /// on every resident key it covers, and then point-locks the key it writes. The range lock does not
    /// materialize a decided-but-unsettled predecessor (only an undecided one blocks it), so the point lock meets
    /// its own intent on an entry still one revision behind the committed head. The grant must report the
    /// committed head as its base and not the stale resident revision: a stale base is refused at finalize as
    /// overtaken by the very commit the lock was granted over.
    /// </summary>
    [Fact]
    public async Task PointLockAfterOwnRangeLock_ObservesCommittedHeadOverUnsettledIntent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "gate/relock";

        // A settled committed value makes the entry resident at revision 0.
        Assert.Equal(KeyValueResponseType.Set, (await Run(node, $"BEGIN SET `{key}` 'v1' COMMIT END")).Type);
        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Count == 0);

        (KeyValueResponseType startType, TransactionHandle holder) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = key + "/holder", Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        // The predecessor's committed update at revision 1, decided and not yet settled: the resident entry
        // still shows revision 0.
        kahuna.DurablePreparedIntentStore.ImportIntents([CommittedSetIntent(holder.TransactionId, key, revision: 1)]);
        kahuna.DurableTransactionRecordStore.ImportRecords([CommittedRecord(holder.TransactionId, key)]);

        (KeyValueResponseType writerType, TransactionHandle writer) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = key + "/writer", Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        Assert.Equal(KeyValueResponseType.Set, writerType);

        (KeyValueResponseType rangeType, _) = await kahuna.LocateAndTryAcquireExclusiveRangeLock(
            writer.TransactionId, "gate", null, true, null, true, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Locked, rangeType);

        (KeyValueResponseType lockType, _, _, _, long baseRevision) = await kahuna.LocateAndTryAcquireExclusiveLockObserved(
            writer.TransactionId, key, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Locked, lockType);
        Assert.Equal(1, baseRevision);
    }

    private static PreparedIntent CommittedSetIntent(HLCTimestamp transactionId, string key, long revision) => new(
        transactionId, Epoch: 0, key, ManifestHash: 0, RecordAnchorKey: key + "/holder",
        CommitTimestamp: transactionId, State: KeyValueState.Set, Value: "v2"u8.ToArray(), Bucket: "gate",
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: revision - 1,
        BaseState: KeyValueState.Set, RecoveryDeadline: HLCTimestamp.Zero,
        Resolution: PreparedIntentResolution.Pending);

    private static PreparedIntent CommittedDeleteIntent(HLCTimestamp transactionId, string key) => new(
        transactionId, Epoch: 0, key, ManifestHash: 0, RecordAnchorKey: key + "/holder",
        CommitTimestamp: transactionId, State: KeyValueState.Deleted, Value: null, Bucket: "gate",
        Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: 0,
        BaseState: KeyValueState.Set, RecoveryDeadline: HLCTimestamp.Zero,
        Resolution: PreparedIntentResolution.Pending);

    private static TransactionRecord CommittedRecord(HLCTimestamp transactionId, string key) => new(
        transactionId, Epoch: 0, CoordinatorKey: key + "/holder", RecordAnchorKey: key + "/holder",
        CommitTimestamp: transactionId, DecisionDeadline: HLCTimestamp.Zero, ManifestHash: 0,
        Participants: [new TransactionParticipantRef(key, KeyValueDurability.Persistent)], ManifestPresent: true,
        Decision: TransactionDecision.Commit, AbortClass: TransactionAbortClass.None, WinningOpId: transactionId,
        CreatedAt: transactionId, DecidedAt: transactionId);

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 5000)
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
