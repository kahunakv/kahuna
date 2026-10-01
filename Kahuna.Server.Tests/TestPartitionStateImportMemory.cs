using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A whole-partition install used to hold the partition's durable-store payloads several times over at once —
/// the store section parsed into contiguous arrays, a copy of each, one protobuf object per record and the
/// decoded record list — before the first record reached the store. On a partition of a few hundred thousand
/// transaction records that was gigabytes on a node already near its heap limit, and the import ran it out of
/// memory. The install now streams every entry from the staged snapshot into its store, so the heap it needs
/// beyond the state it installs is a page of rows, independent of the partition's size.
///
/// <para>The peak is sampled by a thread that forces full collections while the import runs, so this measures
/// the process-wide live heap and shares the exclusive collection with the other allocation measurements.
/// Sampling can only under-read the true peak, so a pass is conservative in the direction that matters.</para>
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestPartitionStateImportMemory
{
    private const int HashPoolSize = 3;

    private const int Records = 80_000;

    private const int Receipts = 80_000;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private sealed record Node(PartitionStateTransfer Transfer, TransactionRecordStore Records, CompletionReceiptStore Receipts);

    private static Node MakeNode()
    {
        RangeMap map = new([new RangeDescriptor { KeySpace = "ranged1", PartitionId = 2, Generation = 1 }]);
        MemoryPersistenceBackend backend = new();
        CompletionReceiptStore receipts = new();
        TransactionRecordStore records = new();

        PartitionStateTransfer transfer = new(
            new PartitionDataEnumerator(backend, () => map, HashPoolSize), backend,
            receipts, records, new PreparedIntentStore(),
            () => map, HashPoolSize,
            () => Task.CompletedTask,
            storagePath: null, storageRevision: "rev", NullLogger<IKahuna>.Instance);

        return new Node(transfer, records, receipts);
    }

    private static void Populate(Node node)
    {
        for (int i = 0; i < Records; i++)
        {
            HLCTimestamp txId = Ts(1_000_000 + i);
            string anchor = $"ranged1/account{i:D7}";
            List<TransactionParticipantRef> manifest =
                [new(anchor, KeyValueDurability.Persistent), new($"ranged1/account{(i + 1) % Records:D7}", KeyValueDurability.Persistent)];

            node.Records.Apply(new InitializeTransactionCommand(txId, 1, "coordinator", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
            node.Records.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100)));
        }

        for (int i = 0; i < Receipts; i++)
            node.Receipts.Record(Ts(5_000_000 + i), $"ranged1/account{i:D7}", "ranged1/anchor", KeyValueDurability.Persistent);
    }

    [Fact]
    public async Task Import_PeakLiveHeap_StaysNearTheInstalledState_NotAMultipleOfTheSnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        Node source = MakeNode();
        Populate(source);

        byte[] snapshot;
        await using (Stream exported = await source.Transfer.ExportPartitionState(2, 42, ct))
        {
            using MemoryStream copy = new();
            await exported.CopyToAsync(copy, ct);
            snapshot = copy.ToArray();
        }

        source = null!;

        // Warm-up on a throwaway node, so JIT and first-use allocations are not charged to the measured import.
        await MakeNode().Transfer.ImportPartitionState(2, new MemoryStream(snapshot, writable: false), ct);

        Node target = MakeNode();

        long peak = 0;
        using CancellationTokenSource stop = new();
        Thread sampler = new(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                long live = GC.GetTotalMemory(forceFullCollection: true);
                if (live > peak)
                    peak = live;

                // Paced so the import makes progress between collections; a materialised section stays live
                // for the whole decode, far longer than this.
                Thread.Sleep(20);
            }
        }) { IsBackground = true, Name = "import-heap-sampler" };

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        sampler.Start();
        try
        {
            await target.Transfer.ImportPartitionState(2, new MemoryStream(snapshot, writable: false), ct);
        }
        finally
        {
            stop.Cancel();
            sampler.Join();
        }

        // The installed state, with the staged snapshot still referenced exactly as it was during the import.
        long installed = GC.GetTotalMemory(forceFullCollection: true);
        GC.KeepAlive(snapshot);

        Assert.Equal(Records, target.Records.Count);
        Assert.Equal(Receipts, target.Receipts.Count);

        long excess = peak - installed;
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"snapshot {snapshot.Length} B, installed live heap {installed} B, sampled peak {peak} B, excess {excess} B ({(double)excess / snapshot.Length:F2}x the snapshot)");

        // Materialising the store section costs several times the snapshot on top of the installed state (two
        // payload copies plus a protobuf object per entry); streaming costs a page of rows.
        Assert.True(excess < snapshot.Length / 2,
            $"the import's live heap peaked {excess} B above the installed state for a {snapshot.Length} B snapshot");
    }
}
