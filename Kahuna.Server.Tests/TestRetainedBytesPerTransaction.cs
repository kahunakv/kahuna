using System.Globalization;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The managed heap a node keeps for each committed transaction after the transaction settled: its canonical
/// record, the completion receipts of its keys, and the settled identities of its keys in the committed-head
/// ledger. The retention windows of the three stores are fixed, so the heap they hold is the commit rate times
/// the bytes measured here.
///
/// <para>Each test commits real interactive transactions on running nodes and waits for their settlement. It
/// then empties one store at a time and reads the live heap after each step: the difference is what that store
/// alone kept alive. A string that two stores share is charged to the store emptied last. The keys come from a
/// space of 10,000, so each key is written many times: a string that the key-value state or a later transaction
/// also holds is charged to no store.</para>
///
/// <para>The tests on one node run with the suite and fail when a transaction keeps more than their bound. The
/// tests on three nodes report what the average replica keeps, where two of three replicas decode each log
/// entry for themselves; they run with <c>KAHUNA_BENCH=1</c>.</para>
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestRetainedBytesPerTransaction : BaseCluster
{
    private const int KeySpace = 10_000;

    private const int Concurrency = 32;

    // A commit can answer MustRetry or Aborted when a partition leader changes under it. A client then runs
    // the transaction again, and so does this workload.
    private const int MaxAttempts = 50;

    private readonly ITestOutputHelper output;

    public TestRetainedBytesPerTransaction(ITestOutputHelper output) => this.output = output;

    /// <summary>What each store alone kept alive for each committed transaction on each node.</summary>
    private readonly record struct StoreBytes(double Records, double Receipts, double Ledger)
    {
        public double Total => Records + Receipts + Ledger;
    }

    private static int TransactionCount =>
        int.TryParse(Environment.GetEnvironmentVariable("KAHUNA_RETAINED_BYTES_TRANSACTIONS"), NumberStyles.Integer, CultureInfo.InvariantCulture, out int count) && count > 0
            ? count
            : 20_000;

    // Hash-routed keys: the keys of one transaction spread over the partitions, so the commit is two-phase.
    private static string HashRoutedKey(int n) => string.Create(CultureInfo.InvariantCulture, $"bench:{n}");

    // Thirty characters in one key space: the keys of one transaction share a partition, so the commit is
    // one-phase.
    private static string KeySpaceKey(int n) => string.Create(CultureInfo.InvariantCulture, $"t/accounts/{n:D19}");

    /// <summary>Runs one transaction that locks and sets <paramref name="keysPerTransaction"/> keys, and returns
    /// Committed or the first answer that is not a success. Every call passes a new string for the key, as a
    /// request decoded from the wire does.</summary>
    private static async Task<KeyValueResponseType> TryCommitOne(IKahuna kahuna, int transaction, int keysPerTransaction, Func<int, string> keyOf, byte[] value, CancellationToken ct)
    {
        (KeyValueResponseType started, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 30_000 }, ct);

        if (started != KeyValueResponseType.Set)
            return started;

        KeyValueResponseType answer = KeyValueResponseType.Committed;

        for (int j = 0; j < keysPerTransaction; j++)
        {
            int n = (int)(((long)transaction * keysPerTransaction + j) % KeySpace);

            (KeyValueResponseType locked, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                handle.TransactionId, keyOf(n), 30_000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, TransactionOperationId.NewRandom());

            if (locked != KeyValueResponseType.Locked)
            {
                answer = locked;
                break;
            }

            (KeyValueResponseType set, _, _) = await kahuna.LocateAndTrySetKeyValue(
                handle.TransactionId, keyOf(n), value, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom());

            if (set != KeyValueResponseType.Set)
            {
                answer = set;
                break;
            }
        }

        if (answer == KeyValueResponseType.Committed)
            (answer, _) = await kahuna.LocateAndCommitTransaction(handle, ct);

        // A session that did not commit holds its locks until it ends.
        if (answer != KeyValueResponseType.Committed)
            await kahuna.LocateAndRollbackTransaction(handle, ct);

        return answer;
    }

    private static async Task CommitOne(IKahuna kahuna, int transaction, int keysPerTransaction, Func<int, string> keyOf, byte[] value, CancellationToken ct)
    {
        KeyValueResponseType answer = KeyValueResponseType.MustRetry;

        // A session whose locks a leader change dropped answers Aborted, and a client then starts a new
        // transaction, as it does after MustRetry.
        for (int attempt = 0; attempt < MaxAttempts && answer is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted; attempt++)
        {
            if (attempt > 0)
                await Task.Delay(50, ct);

            answer = await TryCommitOne(kahuna, transaction, keysPerTransaction, keyOf, value, ct);
        }

        Assert.Equal(KeyValueResponseType.Committed, answer);
    }

    private static async Task CommitMany(IKahuna kahuna, int first, int count, int keysPerTransaction, Func<int, string> keyOf, CancellationToken ct)
    {
        byte[] value = new byte[128];
        int next = first;
        int end = first + count;

        Task[] workers = new Task[Concurrency];
        for (int w = 0; w < workers.Length; w++)
        {
            workers[w] = Task.Run(async () =>
            {
                while (true)
                {
                    int transaction = Interlocked.Increment(ref next) - 1;
                    if (transaction >= end)
                        return;

                    await CommitOne(kahuna, transaction, keysPerTransaction, keyOf, value, ct);
                }
            }, ct);
        }

        await Task.WhenAll(workers);
    }

    /// <summary>Waits until no prepared intent is live on any node and the receipt count stopped moving: the
    /// settlement of a commit runs after the commit answered.</summary>
    private static async Task WaitForSettlement(KahunaManager[] nodes, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 60_000;
        long lastReceipts = -1;

        while (Environment.TickCount64 < deadline)
        {
            await Task.Delay(250, ct);

            long intents = 0;
            long receipts = 0;
            foreach (KahunaManager node in nodes)
            {
                intents += node.DurablePreparedIntentStore.Count;
                receipts += node.CompletionReceiptStore.Count;
            }

            if (intents == 0 && receipts == lastReceipts)
                return;

            lastReceipts = receipts;
        }

        Assert.Fail("the transactions did not settle in 60 s");
    }

    /// <summary>Empties the ledgers, the records and the receipts of every node in turn and returns what each
    /// step released, for each transaction and each node.</summary>
    private StoreBytes Measure(KahunaManager[] nodes, int transactions)
    {
        long records = 0, receipts = 0, settled = 0, heads = 0, recordEstimate = 0, receiptEstimate = 0, ledgerEstimate = 0;
        foreach (KahunaManager node in nodes)
        {
            records += node.DurableTransactionRecordStore.Count;
            receipts += node.CompletionReceiptStore.Count;
            settled += node.DurablePreparedIntentStore.SettledIdentityCount;
            heads += node.DurablePreparedIntentStore.CommittedHeadCount;
            recordEstimate += node.DurableTransactionRecordStore.EstimatedBytes;
            receiptEstimate += node.CompletionReceiptStore.EstimatedBytes;
            ledgerEstimate += node.DurablePreparedIntentStore.CommittedHeadLedgerBytes;
        }

        double perNode = (double)transactions * nodes.Length;

        ReportUnsharedRecordKeys(nodes);

        long before = StableHeap(out long drift);

        foreach (KahunaManager node in nodes)
        {
            PreparedIntentStore intents = node.DurablePreparedIntentStore;
            foreach ((int partitionId, _, _, _) in intents.SnapshotLedgerSizes())
                intents.PurgePartitionLedger(partitionId);
        }

        long afterLedger = LiveHeapMeasurement.Measure();

        foreach (KahunaManager node in nodes)
            node.DurableTransactionRecordStore.PurgeWhere(static _ => true);

        long afterRecords = LiveHeapMeasurement.Measure();

        foreach (KahunaManager node in nodes)
            node.CompletionReceiptStore.PurgeWhere(static _ => true);

        long afterReceipts = LiveHeapMeasurement.Measure();

        StoreBytes bytes = new(
            Records: (afterLedger - afterRecords) / perNode,
            Receipts: (afterRecords - afterReceipts) / perNode,
            Ledger: (before - afterLedger) / perNode);

        output.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"  heap before the first step: {before / (1024.0 * 1024.0):F1} MiB; drift between the last two reads of the idle heap: {drift / perNode:F1} bytes for each transaction and node"));
        output.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"  items for each transaction and node: {records / perNode:F2} records, {receipts / perNode:F2} receipts, {settled / perNode:F2} settled identities, {heads / perNode:F2} heads"));
        output.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"  measured bytes for each transaction and node: records {bytes.Records:F0}, receipts {bytes.Receipts:F0}, ledger {bytes.Ledger:F0}, total {bytes.Total:F0}"));
        output.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"  estimated bytes for each transaction and node: records {recordEstimate / perNode:F0}, receipts {receiptEstimate / perNode:F0}, ledger {ledgerEstimate / perNode:F0}, total {(recordEstimate + receiptEstimate + ledgerEstimate) / perNode:F0}"));

        return bytes;
    }

    /// <summary>Writes, for each node, how many participant keys of its records are not the string that the
    /// shared key strings of the node hold for that key: each of those is a string that the record alone
    /// keeps.</summary>
    private void ReportUnsharedRecordKeys(KahunaManager[] nodes)
    {
        for (int i = 0; i < nodes.Length; i++)
        {
            TransactionRecordStore store = nodes[i].DurableTransactionRecordStore;
            long keys = 0;
            long unshared = 0;

            foreach (TransactionRecord record in store.EnumerateRecords())
            {
                for (int p = 0; p < record.Participants.Count; p++)
                {
                    string key = record.Participants[p].Key;
                    keys++;

                    if (!ReferenceEquals(store.KeyStrings.Share(key), key))
                        unshared++;
                }
            }

            output.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"  node {i + 1}: {unshared} of {keys} participant keys of the records are strings that only the record holds"));
        }
    }

    /// <summary>Reads the live heap until two reads half a second apart differ by less than 64 KiB, so the work a
    /// node finishes after the load (flushes, log compaction, the release of sessions) is not charged to a store.
    /// Returns the last read, and the difference between the last two.</summary>
    private static long StableHeap(out long drift)
    {
        long previous = LiveHeapMeasurement.Measure();
        drift = 0;

        for (int attempt = 0; attempt < 60; attempt++)
        {
            Thread.Sleep(500);

            long current = LiveHeapMeasurement.Measure();
            drift = previous - current;
            previous = current;

            if (Math.Abs(drift) < 64 * 1024)
                break;
        }

        return previous;
    }

    private async Task<StoreBytes> MeasureOnOneNode(int keysPerTransaction, Func<int, string> keyOf)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int transactions = TransactionCount;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, NullLoggerFactory.Instance);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(keyOf(0), ct);

        KahunaManager[] nodes = [(KahunaManager)node.Kahuna];

        // The warm-up writes every key of the key space once, so the measured transactions add no key to the
        // key-value state. Its own records, receipts and identities are part of the measured stores; they are
        // counted with the measured transactions.
        int warmup = KeySpace / keysPerTransaction;
        await CommitMany(node.Kahuna, 0, warmup + transactions, keysPerTransaction, keyOf, ct);
        await WaitForSettlement(nodes, ct);

        return Measure(nodes, warmup + transactions);
    }

    private async Task<StoreBytes> MeasureOnThreeNodes(int keysPerTransaction, Func<int, string> keyOf)
    {
        if (Environment.GetEnvironmentVariable("KAHUNA_BENCH") != "1")
            Assert.Skip("Retained bytes on a three-node cluster. Set KAHUNA_BENCH=1 to run.");

        CancellationToken ct = TestContext.Current.CancellationToken;
        int transactions = TransactionCount;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) = await AssembleThreNodeCluster(
            "memory", 4, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance);

        // The nodes of this cluster share one process, and a node reuses the decoded form of an entry that
        // another node of the process proposed, once for each live node. A follower in a process of its own
        // decodes every entry, with strings of its own. A census of one node gives the proposer the only reuse,
        // so the two followers of each partition decode as they do in production.
        int census = LiveNodeCensus.Exchange(1);

        try
        {
            KahunaManager[] nodes = [(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3];

            int warmup = KeySpace / keysPerTransaction;
            await CommitMany(k1, 0, warmup + transactions, keysPerTransaction, keyOf, ct);
            await WaitForSettlement(nodes, ct);

            return Measure(nodes, warmup + transactions);
        }
        finally
        {
            LiveNodeCensus.Exchange(census);
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>The count of live nodes that the proposed-entry caches read as their reuse budget.</summary>
    private static class LiveNodeCensus
    {
        private static readonly System.Reflection.FieldInfo Field =
            typeof(KahunaManager).Assembly.GetType("Kahuna.Utils.InProcessNodeCensus", throwOnError: true)!
                .GetField("liveNodes", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?? throw new InvalidOperationException("InProcessNodeCensus has no liveNodes field");

        /// <summary>Sets the census and returns the value it had.</summary>
        public static int Exchange(int value)
        {
            int previous = (int)Field.GetValue(null)!;
            Field.SetValue(null, value);
            return previous;
        }
    }

    // The bounds are about 18% above what the stores keep. With a record that holds its timestamps in 24 bytes
    // and keeps its coordinator key, and with a string of its own for each key in each store, a transaction
    // keeps 1,726 bytes with four keys and 989 bytes with two.

    [Fact]
    [Trait("Category", "Performance")]
    public async Task OneNode_FourHashRoutedKeys_KeepAtMost1250BytesForEachTransaction()
    {
        output.WriteLine("one node, four hash-routed keys of 10 characters, two-phase commit:");
        StoreBytes bytes = await MeasureOnOneNode(4, HashRoutedKey);
        Assert.InRange(bytes.Total, 1, 1_250);
    }

    [Fact]
    [Trait("Category", "Performance")]
    public async Task OneNode_TwoKeysOfOneKeySpace_KeepAtMost780BytesForEachTransaction()
    {
        output.WriteLine("one node, two keys of 30 characters in one key space, one-phase commit:");
        StoreBytes bytes = await MeasureOnOneNode(2, KeySpaceKey);
        Assert.InRange(bytes.Total, 1, 780);
    }

    [Fact]
    [Trait("Category", "Performance")]
    public async Task ThreeNodes_FourHashRoutedKeys_ReportTheBytesForEachTransaction()
    {
        output.WriteLine("three nodes, four hash-routed keys of 10 characters, two-phase commit:");
        StoreBytes bytes = await MeasureOnThreeNodes(4, HashRoutedKey);
        Assert.True(bytes.Total > 0);
    }

    [Fact]
    [Trait("Category", "Performance")]
    public async Task ThreeNodes_TwoKeysOfOneKeySpace_ReportTheBytesForEachTransaction()
    {
        output.WriteLine("three nodes, two keys of 30 characters in one key space, one-phase commit:");
        StoreBytes bytes = await MeasureOnThreeNodes(2, KeySpaceKey);
        Assert.True(bytes.Total > 0);
    }
}
