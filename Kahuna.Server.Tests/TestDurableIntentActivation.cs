using System.Collections.Concurrent;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
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
/// Verifies a real persistent transaction now takes the durable-intent finalize path — a canonical transaction
/// record is created and the durable records replicate through the shared partition write scheduler (one
/// heterogeneous <c>ReplicateEntries</c> proposal). Covers single-key and multi-key (SET) transactions.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableIntentActivation
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurableIntentActivation(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private sealed class TypeCapturingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        public readonly ConcurrentBag<string> SeenTypes = [];

        public TypeCapturingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries)
        {
            foreach (RaftProposalEntry entry in entries)
                SeenTypes.Add(entry.Type);

            return inner.ReplicateAsync(partitionId, entries);
        }
    }

    private async Task<(EmbeddedKahunaNode Node, TypeCapturingExecutor Executor)> StartNode(CancellationToken ct)
    {
        TypeCapturingExecutor? capturing = null;
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            EnableDurableIntentTransactions = true,
            WriteBatchExecutorDecorator = inner => capturing = new TypeCapturingExecutor(inner)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("act/row-1", ct);
        return (node, capturing!);
    }

    [Fact]
    public async Task SingleKeyPersistentTransaction_TakesDurablePath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (EmbeddedKahunaNode node, TypeCapturingExecutor exec) = await StartNode(ct);
        await using EmbeddedKahunaNode _ = node;

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `act/row-1` 'v1' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        int records = ((KahunaManager)node.Kahuna).DurableTransactionRecordStore.Count;
        List<string> seen = [.. exec.SeenTypes];

        // Activation, definitively: the transaction took the durable finalize path (a canonical record exists) and
        // its records replicated through the shared scheduler. Reading the value back is intentionally not asserted
        // here — it depends on the durable resolution materializing on the leader, a separate known bug tracked by
        // the multi-key test below.
        // Activation, definitively: the transaction took the durable finalize path (a canonical record exists) and
        // its records replicated through the shared scheduler. Reading the value back is intentionally not asserted
        // here — it depends on the durable resolution materializing on the leader, a separate known bug tracked by
        // the multi-key test below.
        Assert.True(records > 0, $"durable records={records}; seen: {string.Join(",", seen)}");
        Assert.Contains(ReplicationTypes.TransactionRecord, seen);
        Assert.Contains(ReplicationTypes.PreparedIntent, seen);

        // The committed value is durably readable on the leader after resolution settles (not just during the §6
        // Pending window): wait past settlement, then read.
        await Task.Delay(1500, ct);
        (KeyValueResponseType t, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "act/row-1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, t);
        Assert.Equal(Encoding.UTF8.GetBytes("v1"), entry!.Value);
    }

    [Fact]
    public async Task TtlPersistentTransaction_TakesDurablePath_ValueReadableWithFutureExpiry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (EmbeddedKahunaNode node, TypeCapturingExecutor exec) = await StartNode(ct);
        await using EmbeddedKahunaNode _ = node;

        // A persistent TTL set now takes the durable path instead of falling back to the ticket path: the value is
        // staged and its relative TTL is resolved to an absolute expiry at freeze. A long TTL avoids expiry racing
        // the read; the point is that a durable record exists and the committed value carries a future expiry.
        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `act/ttl-1` 'v1' EX 60000 COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        int records = ((KahunaManager)node.Kahuna).DurableTransactionRecordStore.Count;
        Assert.True(records > 0, $"durable records={records}; seen: {string.Join(",", exec.SeenTypes)}");
        Assert.Contains(ReplicationTypes.PreparedIntent, exec.SeenTypes);

        KeyValueResponseType t = KeyValueResponseType.DoesNotExist;
        ReadOnlyKeyValueEntry? entry = null;
        for (int attempt = 0; attempt < 100; attempt++)
        {
            (t, entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "act/ttl-1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            if (t == KeyValueResponseType.Get)
                break;
            await Task.Delay(25, ct);
        }

        Assert.Equal(KeyValueResponseType.Get, t);
        Assert.Equal(Encoding.UTF8.GetBytes("v1"), entry!.Value);
        Assert.NotEqual(HLCTimestamp.Zero, entry.Expires); // TTL resolved to an absolute expiry, not dropped
    }

    [Fact]
    public async Task MultiKeyPersistentTransaction_TakesDurablePath_AllValuesReadable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (EmbeddedKahunaNode node, TypeCapturingExecutor exec) = await StartNode(ct);
        await using EmbeddedKahunaNode _ = node;

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `act/row-1` 'v1' SET `act/row-2` 'v2' SET `act/row-3` 'v3' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        int records = ((KahunaManager)node.Kahuna).DurableTransactionRecordStore.Count;
        Assert.True(records > 0, $"durable records={records}; seen: {string.Join(",", exec.SeenTypes)}");
        Assert.Contains(ReplicationTypes.PreparedIntent, exec.SeenTypes);

        for (int i = 1; i <= 3; i++)
        {
            KeyValueResponseType t = KeyValueResponseType.DoesNotExist;
            ReadOnlyKeyValueEntry? entry = null;
            for (int attempt = 0; attempt < 100; attempt++)
            {
                (t, entry) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, $"act/row-{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                if (t == KeyValueResponseType.Get)
                    break;
                await Task.Delay(25, ct);
            }

            Assert.Equal(KeyValueResponseType.Get, t);
            Assert.Equal(Encoding.UTF8.GetBytes($"v{i}"), entry!.Value);
        }
    }
}
