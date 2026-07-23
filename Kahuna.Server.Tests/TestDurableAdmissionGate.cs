using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The durable admission gate: a node admits at most <c>DurableDecisionOutstandingMax</c> concurrent durable
/// transactions through finalize. A slot is reserved before a transaction prepares and released when its attempt
/// ends, so a burst backpressures with <c>MustRetry</c> (nothing prepared) instead of admitting unbounded
/// prepared state — and the slot frees for the next transaction once the first completes.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableAdmissionGate
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurableAdmissionGate(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    // Holds the first batch that carries a durable transaction record (a finalize's canonical-record replicate),
    // so the first durable transaction parks inside finalize — with its admission slot already reserved — until
    // released. Ordinary key/value batches pass straight through.
    private sealed class DurableRecordGate : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private readonly TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int held;

        public DurableRecordGate(IPartitionBatchExecutor inner) => this.inner = inner;

        public Task Entered => entered.Task;
        public void Release() => gate.TrySetResult();

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            bool carriesRecord = entries.Any(e => e.Type == ReplicationTypes.TransactionRecord);
            if (carriesRecord && Interlocked.Exchange(ref held, 1) == 0)
            {
                entered.TrySetResult();
                await gate.Task;
            }

            return await inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    [Fact]
    public async Task AtCapacity_SecondDurableTransaction_RefusedMustRetry_ThenAdmittedAfterRelease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        DurableRecordGate? gate = null;
        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableDecisionOutstandingMax = 1, // one outstanding durable transaction at a time
            WriteBatchExecutorDecorator = inner => gate = new DurableRecordGate(inner)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("adm/a", ct);
        await node.WaitForLeaderForKeyAsync("adm/b", ct);
        await node.WaitForLeaderForKeyAsync("adm/c", ct);

        // Transaction 1 parks inside finalize at its first record replicate — its admission slot is now held.
        Task<KeyValueTransactionResult> first = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `adm/a` 'v1' COMMIT END"), null, null);
        await gate!.Entered.WaitAsync(TimeSpan.FromSeconds(5), ct);

        // Transaction 2, while the single slot is held, is refused admission before it prepares — retryable.
        KeyValueTransactionResult second = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `adm/b` 'v2' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.MustRetry, second.Type);

        // Release the first transaction; it commits and frees the slot.
        gate.Release();
        Assert.Equal(KeyValueResponseType.Set, (await first).Type);

        // With the slot free, a subsequent durable transaction is admitted and commits.
        KeyValueTransactionResult third = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `adm/c` 'v3' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, third.Type);

        // The refused transaction wrote nothing (no prepared state leaked).
        (KeyValueResponseType bType, _) = await node.Kahuna.LocateAndTryGetValue(
            Kommander.Time.HLCTimestamp.Zero, "adm/b", -1, Kommander.Time.HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, bType);
    }

    [Fact]
    public async Task DisabledGate_AdmitsConcurrentDurableTransactions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        DurableRecordGate? gate = null;
        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableDecisionOutstandingMax = 0, // disabled → unbounded admission
            WriteBatchExecutorDecorator = inner => gate = new DurableRecordGate(inner)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("dis/a", ct);
        await node.WaitForLeaderForKeyAsync("dis/b", ct);

        Task<KeyValueTransactionResult> first = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `dis/a` 'v1' COMMIT END"), null, null);
        await gate!.Entered.WaitAsync(TimeSpan.FromSeconds(5), ct);

        // With the gate disabled, a second transaction is NOT refused even while the first is parked; it proceeds
        // (its own replicate coalesces behind the released first). Prove it is not a MustRetry admission refusal.
        Task<KeyValueTransactionResult> second = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `dis/b` 'v2' COMMIT END"), null, null);

        gate.Release();
        Assert.Equal(KeyValueResponseType.Set, (await first).Type);
        Assert.Equal(KeyValueResponseType.Set, (await second).Type);
    }

    [Fact]
    public async Task PreparedIntentCount_Exceeded_RefusesBeforePrepare_SmallerAdmitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurablePreparedIntentMaxCount = 1 // at most one resident prepared intent admitted
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("cnt/a", ct);

        // A two-key transaction would push resident prepared intents to 2 > cap 1 → refused before preparing.
        KeyValueTransactionResult twoKey = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `cnt/a` 'v1' SET `cnt/b` 'v2' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.MustRetry, twoKey.Type);

        // Neither key was written — the transaction prepared nothing.
        (KeyValueResponseType aType, _) = await node.Kahuna.LocateAndTryGetValue(
            Kommander.Time.HLCTimestamp.Zero, "cnt/a", -1, Kommander.Time.HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, aType);

        // A one-key transaction fits the bound and commits.
        KeyValueTransactionResult oneKey = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `cnt/c` 'v3' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, oneKey.Type);
    }

    [Fact]
    public async Task PreparedIntentBytes_Exceeded_RefusesBeforePrepare_SmallerAdmitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurablePreparedIntentMaxBytes = 8 // a value larger than 8 bytes cannot be admitted
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("byt/a", ct);

        // A 20-byte value exceeds the 8-byte resident bound → refused before preparing.
        KeyValueTransactionResult big = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `byt/a` 'value-of-twenty-byte' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.MustRetry, big.Type);

        // A small value fits and commits.
        KeyValueTransactionResult small = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `byt/b` 'hi' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, small.Type);
    }
}
