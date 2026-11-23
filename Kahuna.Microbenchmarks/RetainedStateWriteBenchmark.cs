using BenchmarkDotNet.Attributes;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Cost of the writes that leave the retained state of a committed transaction, as a follower makes them:
/// the apply of a decoded record entry (initialization and commit) and the record of a completion receipt.
/// Each benchmark runs a batch of 1,024 transactions and removes what it wrote, so the stores keep their size.
///
/// <para><b>Shape:</b> two keys of 30 characters for each transaction, from a space of 10,000 keys. The
/// strings that a benchmark passes are not the instances the stores hold: a follower decodes a new string
/// from each log entry.</para>
/// </summary>
[MemoryDiagnoser]
public class RetainedStateWriteBenchmark
{
    private const int Batch = 1_024;

    private const int KeySpace = 10_000;

    private const int Partition = 3;

    private TransactionRecordStore records = null!;
    private CompletionReceiptStore receipts = null!;

    private RaftLog[] recordEntries = null!;
    private PurgeTransactionCommand[] purges = null!;
    private HLCTimestamp[] transactions = null!;
    private string[] firstKeys = null!;
    private string[] secondKeys = null!;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static string Key(int n) => $"t/accounts/{n % KeySpace:D19}";

    // A new string with the characters of the argument, as a decoder makes for each log entry.
    private static string Copy(string value) => new(value.AsSpan());

    [GlobalSetup]
    public void Setup()
    {
        records = new TransactionRecordStore();
        receipts = new CompletionReceiptStore();

        // The stores of a loaded node: each key of the space is in several records and receipts.
        for (int i = 0; i < 100_000; i++)
        {
            HLCTimestamp txId = Ts(1_000_000 + i);
            records.Replicate(Partition, Entry(txId, i));
            receipts.Record(txId, Copy(Key(2 * i)), Copy(Key(2 * i)), KeyValueDurability.Persistent);
            receipts.Record(txId, Copy(Key(2 * i + 1)), Copy(Key(2 * i)), KeyValueDurability.Persistent);
        }

        recordEntries = new RaftLog[Batch];
        purges = new PurgeTransactionCommand[Batch];
        transactions = new HLCTimestamp[Batch];
        firstKeys = new string[Batch];
        secondKeys = new string[Batch];

        for (int i = 0; i < Batch; i++)
        {
            HLCTimestamp txId = Ts(9_000_000 + i);
            transactions[i] = txId;
            recordEntries[i] = Entry(txId, 50_000 + i);
            purges[i] = new PurgeTransactionCommand(txId, 1);
            firstKeys[i] = Copy(Key(2 * (50_000 + i)));
            secondKeys[i] = Copy(Key(2 * (50_000 + i) + 1));
        }
    }

    // One log entry with the initialization and the commit of a transaction. The bytes are a copy, so the
    // store decodes them as a follower does.
    private static RaftLog Entry(HLCTimestamp txId, int i)
    {
        string anchor = Key(2 * i);
        List<TransactionParticipantRef> manifest =
        [
            new(anchor, KeyValueDurability.Persistent),
            new(Key(2 * i + 1), KeyValueDurability.Persistent)
        ];

        byte[] delta = TransactionRecordStore.SerializeDelta(
        [
            new InitializeTransactionCommand(txId, 1, "0123456789abcdef0123456789abcdef", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId),
            new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100))
        ]);

        return new RaftLog { LogType = ReplicationTypes.TransactionRecord, LogData = (byte[])delta.Clone() };
    }

    [Benchmark]
    public int RecordApplyDecoded()
    {
        for (int i = 0; i < Batch; i++)
            records.Replicate(Partition, recordEntries[i]);

        for (int i = 0; i < Batch; i++)
            records.Apply(purges[i]);

        return records.Count;
    }

    [Benchmark]
    public int ReceiptRecordAndForget()
    {
        for (int i = 0; i < Batch; i++)
        {
            receipts.Record(transactions[i], firstKeys[i], firstKeys[i], KeyValueDurability.Persistent);
            receipts.Record(transactions[i], secondKeys[i], firstKeys[i], KeyValueDurability.Persistent);
        }

        for (int i = 0; i < Batch; i++)
        {
            receipts.Forget(transactions[i], firstKeys[i]);
            receipts.Forget(transactions[i], secondKeys[i]);
        }

        return receipts.Count;
    }
}
