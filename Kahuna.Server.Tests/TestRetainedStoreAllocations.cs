using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The work a loaded node does on a timer or at a restart, with stores large enough that a copy of a whole map
/// is an array above 1 MiB. A node at its heap limit cannot always make one allocation of megabytes, and it
/// does this work again and again: the checkpoint capture of each store, the walk of the retention sweep, the
/// load of the checkpoint file, and the install of a whole-partition section. Each must make no single
/// allocation above 1 MiB.
///
/// <para>The growth of the stores' maps is here too: the committed heads of a slice, the transaction records,
/// the completion receipts and the live intents are written on the apply path, where a failed allocation ends
/// the process. One concurrent dictionary of these sizes replaces a bucket array of megabytes at each
/// doubling.</para>
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestRetainedStoreAllocations : IDisposable
{
    private const int Partition = 3;

    // One reference for each item is 1.6 MiB: enough for a copy of the map to fail the test.
    private const int Records = 200_000;

    private const int LiveIntents = 200_000;

    // One decoded message reference for each identity grows a repeated field through 2 MiB and 4 MiB.
    private const int SettledIdentities = 300_000;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-store-allocations-" + Guid.NewGuid().ToString("N"));

    public TestRetainedStoreAllocations() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent Intent(string key, long tx) => new(
        TransactionId: Ts(tx), Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key, CommitTimestamp: Ts(tx + 1),
        State: KeyValueState.Set, Value: [1], Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: true,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>Runs <paramref name="work"/> under the probe and fails when it made one allocation above 1 MiB.</summary>
    private static void AssertNoLargeAllocation(string step, Action work)
    {
        IReadOnlyList<AllocationTick> ticks = LargeAllocationProbe.TicksOf(work);
        string large = LargeAllocationProbe.Describe(ticks);

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{step}: largest single allocation {LargeAllocationProbe.Largest(ticks).ObjectSize} B ({LargeAllocationProbe.Largest(ticks).TypeName})");

        Assert.True(large.Length == 0, $"{step} made allocations above 1 MiB: {large}");
    }

    private TransactionRecordStore RecordStore()
    {
        TransactionRecordStore records = new(dir, "rev", null);
        records.AttachAnchorResolver(_ => (Partition, 0));

        for (int i = 0; i < Records; i++)
        {
            HLCTimestamp txId = Ts(1_000_000 + i);
            string anchor = $"ranged1/accounts/k{i:D12}";
            List<TransactionParticipantRef> manifest = [new(anchor, KeyValueDurability.Persistent)];

            records.Apply(new InitializeTransactionCommand(txId, 1, "coordinator", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
            records.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100)));
        }

        return records;
    }

    private PreparedIntentStore IntentStore(int settledIdentities, int liveIntents)
    {
        PreparedIntentStore intents = new(dir, "rev", null);
        intents.AttachPartitionResolver(_ => Partition);

        for (int i = 0; i < settledIdentities; i++)
            intents.Apply(new RemoveIntentCommand(Ts(1_000 + i), 1, $"ranged1/accounts/k{i:D12}"), Partition);

        for (int i = 0; i < liveIntents; i++)
            intents.Apply(new PrepareIntentCommand(Intent($"ranged1/live/k{i:D12}", 9_000_000 + i)), Partition);

        return intents;
    }

    [Fact]
    public void TheHeadsOfASlice_GrowWithNoArrayAboveOneMebibyte()
    {
        const int heads = 300_000;
        PreparedIntentStore store = new();

        // One settled commit for each key: the apply path writes one head and one settled identity.
        AssertNoLargeAllocation($"apply of {heads} settled commits", () =>
        {
            for (int i = 0; i < heads; i++)
            {
                PreparedIntent intent = Intent($"ranged1/heads/k{i:D12}", 1_000_000 + i / 1_000);
                store.Apply(new PrepareIntentCommand(intent), Partition);
                store.Apply(new ResolveIntentCommand(intent.TransactionId, 1, intent.Key, Commit: true), Partition);
                store.Apply(new RemoveIntentCommand(intent.TransactionId, 1, intent.Key), Partition);
            }
        });

        Assert.Equal(heads, store.CommittedHeadCount);
        Assert.Equal(heads, store.CommittedHeadCountForPartition(Partition));
        Assert.Equal(heads, store.SnapshotLedger(Partition).Count);
        Assert.True(store.TryGetLedgerHead(Partition, "ranged1/heads/k000000123456", out long revision, out _, out _));
        Assert.Equal(1, revision);

        // One commit far ahead moves the watermark past the retention of every head: the prune that follows
        // drops all of them in one pass under the apply gate.
        store.ConfigureStagedBaseFence(retentionMs: 1_000);

        AssertNoLargeAllocation($"prune of {heads} heads", () =>
        {
            PreparedIntent last = Intent("ranged1/heads/last", 2_000_000);
            store.Apply(new PrepareIntentCommand(last), Partition);
            store.Apply(new ResolveIntentCommand(last.TransactionId, 1, last.Key, Commit: true), Partition);
        });

        Assert.Equal(1, store.CommittedHeadCount);
        Assert.Single(store.SnapshotLedger(Partition));
        Assert.False(store.TryGetLedgerHead(Partition, "ranged1/heads/k000000123456", out _, out _, out _));
    }

    [Fact]
    public void TheRecords_GrowAndLoad_WithNoArrayAboveOneMebibyte()
    {
        TransactionRecordStore? records = null;
        AssertNoLargeAllocation($"apply of {Records} records", () => records = RecordStore());

        Assert.Equal(Records, records!.Count);
        Assert.NotNull(records.Get(Ts(1_000_000 + Records - 1), 1));
        Assert.True(records.PersistSnapshot(Partition));

        // A restart fills the map again from the file.
        TransactionRecordStore? loaded = null;
        AssertNoLargeAllocation("load of the record file", () => loaded = new TransactionRecordStore(dir, "rev", null));

        Assert.Equal(Records, loaded!.Count);
        Assert.Equal(TransactionDecision.Commit, loaded.Get(Ts(1_000_000 + Records / 2), 1)!.Decision);
    }

    [Fact]
    public void TheReceipts_GrowLoadAndPurge_WithNoArrayAboveOneMebibyte()
    {
        const int receiptCount = 400_000;

        CompletionReceiptStore receipts = new(dir, "rev", NullLogger<IKahuna>.Instance);
        receipts.AttachPartitionResolver(_ => Partition);

        AssertNoLargeAllocation($"record of {receiptCount} receipts", () =>
        {
            for (int i = 0; i < receiptCount; i++)
                receipts.Record(Ts(5_000_000 + i / 2), $"ranged1/accounts/k{i:D12}", "ranged1/accounts/anchor0000000", KeyValueDurability.Persistent);
        });

        Assert.Equal(receiptCount, receipts.Count);
        Assert.True(receipts.Contains(Ts(5_000_000 + 123_456 / 2), "ranged1/accounts/k000000123456", KeyValueDurability.Persistent));
        Assert.True(receipts.PersistSnapshot(Partition));

        CompletionReceiptStore? loaded = null;
        AssertNoLargeAllocation("load of the receipt file", () => loaded = new CompletionReceiptStore(dir, "rev", NullLogger<IKahuna>.Instance));
        Assert.Equal(receiptCount, loaded!.Count);

        // The purge of a partition that the node no longer hosts, and of an install, removes every receipt of
        // the partition: it walks the map and collects nothing.
        int purged = 0;
        AssertNoLargeAllocation("purge of the receipts", () => purged = receipts.PurgeWhere(static _ => true));

        Assert.Equal(receiptCount, purged);
        Assert.Equal(0, receipts.Count);
        Assert.False(receipts.Contains(Ts(5_000_000 + 123_456 / 2), "ranged1/accounts/k000000123456", KeyValueDurability.Persistent));
    }

    [Fact]
    public void TheLiveIntents_GrowAndLoad_WithNoArrayAboveOneMebibyte()
    {
        PreparedIntentStore? intents = null;
        AssertNoLargeAllocation($"apply of {LiveIntents} prepares", () => intents = IntentStore(settledIdentities: 0, LiveIntents));

        Assert.Equal(LiveIntents, intents!.Count);
        Assert.NotNull(intents.Get($"ranged1/live/k{LiveIntents - 1:D12}"));
        Assert.True(intents.PersistSnapshot(Partition));

        // A restart loads every intent, and remembers the partition of the file each one came from.
        PreparedIntentStore? loaded = null;
        AssertNoLargeAllocation("load of the intents", () => loaded = new PreparedIntentStore(dir, "rev", null));

        Assert.Equal(LiveIntents, loaded!.Count);
        Assert.NotNull(loaded.Get("ranged1/live/k000000000000"));
    }

    [Fact]
    public void TheRecordCapture_AndTheSweepWalk_CopyNoMap()
    {
        TransactionRecordStore records = RecordStore();

        AssertNoLargeAllocation("capture of the records", () => Assert.True(records.PersistSnapshot(Partition)));

        AssertNoLargeAllocation("sweep walk of the records", () =>
        {
            int terminal = 0;
            foreach (TransactionRecord record in records.EnumerateRecords())
            {
                if (record.IsTerminal)
                    terminal++;
            }

            Assert.Equal(Records, terminal);
        });

        // The file of the walk holds every record.
        Assert.Equal(Records, new TransactionRecordStore(dir, "rev", null).Count);
    }

    [Fact]
    public void TheIntentCapture_AndTheSweepWalk_CopyNoMap()
    {
        PreparedIntentStore intents = IntentStore(settledIdentities: 1_000, liveIntents: LiveIntents);

        AssertNoLargeAllocation("capture of the intents", () => Assert.True(intents.PersistSnapshot(Partition)));

        AssertNoLargeAllocation("section of the intents", () =>
            Assert.Equal(LiveIntents, intents.WritePartitionSection(Stream.Null, Partition, static _ => true)));

        AssertNoLargeAllocation("sweep walk of the intents", () =>
        {
            int pending = 0;
            foreach (PreparedIntent intent in intents.EnumerateIntents())
            {
                if (intent.IsPending)
                    pending++;
            }

            Assert.Equal(LiveIntents, pending);
        });
    }

    [Fact]
    public void TheLoadOfACheckpointFile_DecodesOneEntryAtATime()
    {
        PreparedIntentStore source = IntentStore(SettledIdentities, liveIntents: 100);
        Assert.True(source.PersistSnapshot(Partition));

        PreparedIntentStore? loaded = null;
        AssertNoLargeAllocation("load of the intent file", () =>
        {
            loaded = new PreparedIntentStore(dir, "rev", null);
        });

        loaded!.AttachPartitionResolver(_ => Partition);
        Assert.Equal(SettledIdentities, loaded.SettledIdentityCount);
        Assert.Equal(100, loaded.Count);
    }

    [Fact]
    public void TheInstallOfASection_DecodesOneEntryAtATime()
    {
        PreparedIntentStore exporter = IntentStore(SettledIdentities, liveIntents: 100);

        using MemoryStream section = new();
        exporter.WritePartitionSection(section, Partition, static _ => true);

        // A receiver that holds a slice of the same size for the partition: the install releases it before it
        // builds the new one.
        PreparedIntentStore receiver = new();
        for (int i = 0; i < SettledIdentities; i++)
            receiver.Apply(new RemoveIntentCommand(Ts(5_000_000 + i), 1, $"ranged1/before/k{i:D12}"), Partition);

        AssertNoLargeAllocation("install of the intent section", () =>
        {
            section.Position = 0;
            PreparedIntentStore.SnapshotHeader header = PreparedIntentStore.VerifyPartitionSection(section);
            Assert.Equal(SettledIdentities, header.SettledIdentities);

            section.Position = 0;
            receiver.ReplacePartitionIntents(Partition, section, header, requireLedger: true, static _ => true);
        });

        Assert.Equal(SettledIdentities, receiver.SettledIdentityCount);
        Assert.Equal(100, receiver.Count);
        Assert.Equal(exporter.SnapshotSettledIdentities(Partition), receiver.SnapshotSettledIdentities(Partition));
    }
}
