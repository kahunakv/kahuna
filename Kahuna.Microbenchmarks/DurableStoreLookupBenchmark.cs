using BenchmarkDotNet.Attributes;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Cost of the point lookups of the three durable stores, each over a batch of 1,024 keys.
///
/// <para><b>Shape:</b> <see cref="PreparedIntentStore.Get"/> runs for every point read and write of an actor,
/// and in the common case the key has no intent: <see cref="IntentGetMiss"/> is that case. The store holds
/// <see cref="LiveIntents"/> intents: none (a workload with no durable transaction in flight), the few
/// thousand of a loaded node, and the backlog of a node that restarted under load. The receipt check and the
/// record lookup run for a transaction's keys at its commit, against the stores of a loaded node.</para>
/// </summary>
[MemoryDiagnoser]
public class DurableStoreLookupBenchmark
{
    private const int Batch = 1_024;

    private const int Receipts = 500_000;

    private const int Records = 250_000;

    [Params(0, 4_000, 170_000)]
    public int LiveIntents;

    private PreparedIntentStore intents = null!;
    private CompletionReceiptStore receipts = null!;
    private TransactionRecordStore records = null!;

    private string[] heldKeys = null!;
    private string[] freeKeys = null!;
    private string[] receiptKeys = null!;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    [GlobalSetup]
    public void Setup()
    {
        intents = new PreparedIntentStore();

        for (int i = 0; i < LiveIntents; i++)
        {
            string key = $"ranged1/live/k{i:D12}";
            intents.Apply(new PrepareIntentCommand(new PreparedIntent(
                Ts(9_000_000 + i), 1, key, 0, key, Ts(9_000_001 + i),
                KeyValueState.Set, [1], null, 1, HLCTimestamp.Zero, true,
                PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, HLCTimestamp.Zero, PreparedIntentResolution.Pending)), 3);
        }

        heldKeys = new string[Batch];
        freeKeys = new string[Batch];
        receiptKeys = new string[Batch];

        for (int i = 0; i < Batch; i++)
        {
            // A new string for each key, as a request has: the lookup cannot use a hash the fill left behind.
            heldKeys[i] = new string($"ranged1/live/k{i * Math.Max(1, LiveIntents / Batch):D12}".AsSpan());
            freeKeys[i] = $"ranged1/accounts/k{i * 37:D12}";
            receiptKeys[i] = $"ranged1/accounts/k{i * 401:D12}";
        }

        receipts = new CompletionReceiptStore();
        for (int i = 0; i < Receipts; i++)
            receipts.Record(Ts(5_000_000 + i / 2), $"ranged1/accounts/k{i:D12}", "ranged1/accounts/anchor0000000", KeyValueDurability.Persistent);

        records = new TransactionRecordStore();
        for (int i = 0; i < Records; i++)
        {
            HLCTimestamp txId = Ts(1_000_000 + i);
            string anchor = $"ranged1/accounts/k{i:D12}";
            List<TransactionParticipantRef> manifest = [new(anchor, KeyValueDurability.Persistent)];

            records.Apply(new InitializeTransactionCommand(txId, 1, "coordinator", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
            records.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100)));
        }
    }

    [Benchmark]
    public int IntentGetMiss()
    {
        int found = 0;
        foreach (string key in freeKeys)
        {
            if (intents.Get(key) is not null)
                found++;
        }

        return found;
    }

    [Benchmark]
    public int IntentGetHit()
    {
        int found = 0;
        foreach (string key in heldKeys)
        {
            if (intents.Get(key) is not null)
                found++;
        }

        return found;
    }

    [Benchmark]
    public int ReceiptContains()
    {
        int found = 0;
        for (int i = 0; i < receiptKeys.Length; i++)
        {
            if (receipts.Contains(Ts(5_000_000 + i * 401 / 2), receiptKeys[i], KeyValueDurability.Persistent))
                found++;
        }

        return found;
    }

    [Benchmark]
    public int RecordGet()
    {
        int found = 0;
        for (int i = 0; i < Batch; i++)
        {
            if (records.Get(Ts(1_000_000 + i * 211), 1) is not null)
                found++;
        }

        return found;
    }
}
