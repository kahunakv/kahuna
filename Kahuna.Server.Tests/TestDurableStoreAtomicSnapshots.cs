using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The stores that hand a range to another partition, and the backup, take one copy of a map that is atomic
/// across its entries. The maps are stored in shards, which have no atomic copy of their own, so the copy is
/// taken under the gate that every writer of the map holds.
///
/// <para>The tests give the copy something to tear. A writer adds entries one at a time, in order, and then
/// removes all of them in one step under the gate. A copy at one instant is then a prefix of the order: the
/// first n entries, with none missing. A walk with no gate can meet the removal in its middle and return the
/// entries of the shards it read before the removal with the entries added after it, which is no prefix.</para>
/// </summary>
public sealed class TestDurableStoreAtomicSnapshots
{
    private const int Partition = 3;

    private const int Entries = 3_000;

    private static readonly TimeSpan RunFor = TimeSpan.FromSeconds(2);

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static string Key(int i) => $"cut/k{i:D6}";

    /// <summary>Asserts that <paramref name="indexes"/> are exactly 0 to n-1 for some n.</summary>
    private static void AssertPrefix(List<int> indexes, string what)
    {
        indexes.Sort();

        for (int i = 0; i < indexes.Count; i++)
        {
            if (indexes[i] != i)
                Assert.Fail($"{what} holds {indexes.Count} entries and is not the first {indexes.Count} of the order: position {i} holds entry {indexes[i]}");
        }
    }

    /// <summary>Runs <paramref name="write"/> on a thread in a loop, and <paramref name="check"/> on the calling
    /// thread, for <see cref="RunFor"/>. Returns how many checks ran.</summary>
    private static int RunAgainstWriter(Action write, Action check)
    {
        using CancellationTokenSource stop = new();
        Exception? writerFault = null;

        Thread writer = new(() =>
        {
            try
            {
                while (!stop.IsCancellationRequested)
                    write();
            }
            catch (Exception ex)
            {
                writerFault = ex;
            }
        })
        { IsBackground = true, Name = "atomic-snapshot-writer" };

        writer.Start();

        int checks = 0;
        long until = Environment.TickCount64 + (long)RunFor.TotalMilliseconds;

        try
        {
            while (Environment.TickCount64 < until)
            {
                check();
                checks++;
            }
        }
        finally
        {
            stop.Cancel();
            writer.Join();
        }

        Assert.Null(writerFault);
        return checks;
    }

    [Fact]
    public void ACopyOfTheLiveIntents_IsOneCutAcrossKeys()
    {
        PreparedIntentStore store = new();

        void FillThenPurge()
        {
            for (int i = 0; i < Entries; i++)
            {
                string key = Key(i);
                store.Apply(new PrepareIntentCommand(new PreparedIntent(
                    Ts(1_000 + i), 1, key, 0, key, Ts(1_001 + i),
                    KeyValueState.Set, [1], null, 1, HLCTimestamp.Zero, true,
                    PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, HLCTimestamp.Zero, PreparedIntentResolution.Pending)), Partition);
            }

            store.PurgeWhere(static _ => true);
        }

        static List<int> IndexesOf(IEnumerable<PreparedIntent> intents) =>
            [.. intents.Select(intent => int.Parse(intent.Key.AsSpan("cut/k".Length)))];

        int checks = RunAgainstWriter(FillThenPurge, () =>
        {
            AssertPrefix(IndexesOf(store.Snapshot()), "the snapshot of the intents");
            AssertPrefix(IndexesOf(store.SnapshotRange(null, null)), "the range snapshot of the intents");
            AssertPrefix(IndexesOf(store.WalkLiveIntents([Partition]).Intents), "the backup walk of the intents");
        });

        Assert.True(checks > 50, $"only {checks} checks ran");
    }

    [Fact]
    public void ACopyOfTheRecords_IsOneCutAcrossRecords()
    {
        TransactionRecordStore store = new();

        void FillThenPurge()
        {
            for (int i = 0; i < Entries; i++)
            {
                HLCTimestamp txId = Ts(1_000_000 + i);
                string anchor = Key(i);
                List<TransactionParticipantRef> manifest = [new(anchor, KeyValueDurability.Persistent)];

                store.Apply(new InitializeTransactionCommand(txId, 1, "coordinator", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
            }

            store.PurgeWhere(static _ => true);
        }

        static List<int> IndexesOf(IEnumerable<TransactionRecord> records) =>
            [.. records.Select(record => (int)(record.TransactionId.L - 1_000_000))];

        int checks = RunAgainstWriter(FillThenPurge, () =>
        {
            AssertPrefix(IndexesOf(store.Snapshot()), "the snapshot of the records");
            AssertPrefix(IndexesOf(store.SnapshotRange(null, null)), "the range snapshot of the records");
        });

        Assert.True(checks > 50, $"only {checks} checks ran");
    }

    [Fact]
    public void TheCounts_FollowTheWrites()
    {
        // The maps keep no count. Each store counts its own inserts and removes.
        PreparedIntentStore intents = new();
        TransactionRecordStore records = new();
        CompletionReceiptStore receipts = new();

        for (int i = 0; i < 500; i++)
        {
            string key = Key(i);
            PreparedIntent intent = new(
                Ts(1_000 + i), 1, key, 0, key, Ts(1_001 + i),
                KeyValueState.Set, [1], null, 1, HLCTimestamp.Zero, true,
                PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, HLCTimestamp.Zero, PreparedIntentResolution.Pending);

            intents.Apply(new PrepareIntentCommand(intent), Partition);
            intents.Apply(new PrepareIntentCommand(intent), Partition); // the same prepare again adds nothing

            HLCTimestamp txId = Ts(1_000_000 + i);
            List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
            records.Apply(new InitializeTransactionCommand(txId, 1, "coordinator", key, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
            records.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100))); // a replace, not an insert

            receipts.Record(Ts(1_000 + i), key, key, KeyValueDurability.Persistent);
            receipts.Record(Ts(1_000 + i), key, key, KeyValueDurability.Persistent); // idempotent
        }

        Assert.Equal(500, intents.Count);
        Assert.Equal(500, intents.LiveIntentCount);
        Assert.Equal(500, intents.Snapshot().Count);
        Assert.Equal(500, records.Count);
        Assert.Equal(500, records.Snapshot().Count);
        Assert.Equal(500, receipts.Count);
        Assert.Equal(500, receipts.SnapshotRange(null, null).Count);

        // A resolve replaces an intent and removes none; a remove takes one out.
        for (int i = 0; i < 200; i++)
        {
            intents.Apply(new ResolveIntentCommand(Ts(1_000 + i), 1, Key(i), Commit: true), Partition);
            Assert.True(receipts.Forget(Ts(1_000 + i), Key(i)));
            Assert.False(receipts.Forget(Ts(1_000 + i), Key(i)));
        }

        Assert.Equal(500, intents.Count);

        for (int i = 0; i < 200; i++)
            intents.Apply(new RemoveIntentCommand(Ts(1_000 + i), 1, Key(i)), Partition);

        Assert.Equal(300, intents.Count);
        Assert.Equal(300, intents.Snapshot().Count);
        Assert.Equal(300, receipts.Count);
        Assert.Null(intents.Get(Key(0)));
        Assert.NotNull(intents.Get(Key(499)));

        Assert.Equal(300, intents.PurgeWhere(static _ => true));
        Assert.Equal(500, records.PurgeWhere(static _ => true));
        Assert.Equal(300, receipts.PurgeWhere(static _ => true));

        Assert.Equal(0, intents.Count);
        Assert.Equal(0, records.Count);
        Assert.Equal(0, receipts.Count);

        // An empty store answers a lookup from its count, and a new intent is found again.
        Assert.Null(intents.Get(Key(499)));
        intents.Apply(new PrepareIntentCommand(new PreparedIntent(
            Ts(9_000), 1, Key(7), 0, Key(7), Ts(9_001),
            KeyValueState.Set, [1], null, 1, HLCTimestamp.Zero, true,
            PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, HLCTimestamp.Zero, PreparedIntentResolution.Pending)), Partition);
        Assert.NotNull(intents.Get(Key(7)));
        Assert.Equal(1, intents.Count);
    }
}
