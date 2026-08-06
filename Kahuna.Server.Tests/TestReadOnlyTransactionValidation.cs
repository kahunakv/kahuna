using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Commit-time validation for transactions that only read. A read-only transaction's reads are each served
/// against the latest committed state at a different moment, so without a commit-time check its "snapshot" can
/// straddle concurrent commits — observing one transaction's write while missing an earlier one — and close an
/// anti-dependency cycle no serial order explains (write skew), even though every writer validated correctly.
/// Under TrackAndValidate the commit of a read-only transaction must therefore prove its reads still form one
/// consistent cut: a read overtaken by a committed write, or a read key carrying a live foreign write intent,
/// aborts the transaction instead of committing it.
/// </summary>
public sealed class TestReadOnlyTransactionValidation
{
    private readonly ILoggerFactory loggerFactory;

    public TestReadOnlyTransactionValidation(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
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
            InitialPartitions = 1,
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("rovalidate/a", ct);
        return node;
    }

    private static async Task SetPlain(EmbeddedKahunaNode node, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task<TransactionHandle> StartSession(EmbeddedKahunaNode node, string coordinatorKey, ReadValidation readValidation, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                ReadValidation = readValidation,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static async Task ReadInSession(EmbeddedKahunaNode node, TransactionHandle handle, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, type);
    }

    /// <summary>
    /// The write-skew reader shape: the transaction reads one key, a concurrent write to that key commits, and
    /// the transaction then reads another key — a cut of the database that never existed at any single point.
    /// Commit-time revalidation sees the first read's revision has moved and aborts.
    /// </summary>
    [Fact]
    public async Task ReadOnlyTransaction_ReadOvertakenByCommittedWrite_Aborts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await SetPlain(node, "rovalidate/a", "a1", ct);
        await SetPlain(node, "rovalidate/b", "b1", ct);

        TransactionHandle reader = await StartSession(node, "rovalidate/tx-overtaken", ReadValidation.TrackAndValidate, ct);
        await ReadInSession(node, reader, "rovalidate/a", ct);

        // A concurrent transaction commits a new revision of the key already read.
        await SetPlain(node, "rovalidate/a", "a2", ct);

        await ReadInSession(node, reader, "rovalidate/b", ct);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(reader, ct);
        Assert.Equal(KeyValueResponseType.Aborted, commit);
    }

    /// <summary>
    /// A read key carrying a live foreign write intent at commit is a concurrent writer mid-flight: its own
    /// validation already passed, so it can commit after this reader validates revisions — the reader's probe
    /// is the only guard that still sees it. The read-only commit must abort on the probe.
    /// </summary>
    [Fact]
    public async Task ReadOnlyTransaction_ReadKeyHeldByInFlightWriter_Aborts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await SetPlain(node, "rovalidate/held", "v1", ct);

        TransactionHandle writer = await StartSession(node, "rovalidate/tx-writer", ReadValidation.TrackAndValidate, ct);
        (KeyValueResponseType staged, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            writer.TransactionId, "rovalidate/held", Encoding.UTF8.GetBytes("v2"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: writer.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, staged);

        TransactionHandle reader = await StartSession(node, "rovalidate/tx-reader", ReadValidation.TrackAndValidate, ct);
        await ReadInSession(node, reader, "rovalidate/held", ct);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(reader, ct);
        Assert.Equal(KeyValueResponseType.Aborted, commit);

        Assert.Equal(KeyValueResponseType.RolledBack, await node.Kahuna.LocateAndRollbackTransaction(writer, ct));
    }

    /// <summary>Control: undisturbed reads validate cleanly and the read-only commit still succeeds.</summary>
    [Fact]
    public async Task ReadOnlyTransaction_UnchangedReads_Commits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await SetPlain(node, "rovalidate/a", "a1", ct);
        await SetPlain(node, "rovalidate/b", "b1", ct);

        TransactionHandle reader = await StartSession(node, "rovalidate/tx-clean", ReadValidation.TrackAndValidate, ct);
        await ReadInSession(node, reader, "rovalidate/a", ct);
        await ReadInSession(node, reader, "rovalidate/b", ct);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(reader, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);
    }

    /// <summary>
    /// Control: a session that never asked for read validation keeps its old semantics — the read-only commit
    /// succeeds even when a read was overtaken, because no read-stability promise was made.
    /// </summary>
    [Fact]
    public async Task ReadOnlyTransaction_WithoutValidation_CommitsDespiteOvertakenRead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await SetPlain(node, "rovalidate/a", "a1", ct);

        TransactionHandle reader = await StartSession(node, "rovalidate/tx-novalidate", ReadValidation.None, ct);
        await ReadInSession(node, reader, "rovalidate/a", ct);

        await SetPlain(node, "rovalidate/a", "a2", ct);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(reader, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);
    }
}
