using System.Runtime.CompilerServices;

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
/// The peak live heap of one whole-partition install, set against the node's footprint before and after it and
/// against the size of the snapshot. The partition has the shape of the one a restarted node was re-seeded with in
/// the fault soaks: about 200,000 retained transaction records, twice as many completion receipts and a few
/// thousand large rows. The installing node already holds an older copy of the partition, as a node that restarts
/// from its own data does, and the snapshot is staged in memory, as Kommander stages it without a staging
/// directory. What the install adds on top of the larger of the two footprints is its own working set; it must stay
/// a small fraction of the snapshot, whatever the partition's size.
///
/// <para>The peak is sampled by a thread that forces full collections while the install runs, so this measures the
/// process-wide live heap and shares the exclusive collection with the other allocation measurements. Sampling can
/// only under-read the true peak.</para>
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestPartitionStateInstallPeakMemory
{
    private const int HashPoolSize = 3;

    private const int Records = 200_000;

    private const int Receipts = 400_000;

    private const int Rows = 2_000;

    private const int RowValueBytes = 16 * 1024;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private sealed record Node(PartitionStateTransfer Transfer, IPersistenceBackend Backend, TransactionRecordStore Records, CompletionReceiptStore Receipts);

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

        return new Node(transfer, backend, records, receipts);
    }

    /// <summary>Fills the partition; <paramref name="generation"/> shifts every timestamp and revision, so an older
    /// copy and the snapshot's copy describe the same keys at different points in time.</summary>
    private static void Populate(Node node, long generation)
    {
        long shift = generation * 100_000_000;

        for (int i = 0; i < Records; i++)
        {
            HLCTimestamp txId = Ts(shift + 1_000_000 + i);
            string anchor = $"ranged1/account{i:D7}";
            List<TransactionParticipantRef> manifest =
                [new(anchor, KeyValueDurability.Persistent), new($"ranged1/account{(i + 1) % Records:D7}", KeyValueDurability.Persistent)];

            node.Records.Apply(new InitializeTransactionCommand(txId, 1, "coordinator", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
            node.Records.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100)));
        }

        for (int i = 0; i < Receipts; i++)
            node.Receipts.Record(Ts(shift + 5_000_000 + i), $"ranged1/account{i % Records:D7}", "ranged1/anchor", KeyValueDurability.Persistent);

        const int Batch = 256;
        List<PersistenceRequestItem> rows = new(Batch);
        for (int i = 0; i < Rows; i++)
        {
            byte[] value = new byte[RowValueBytes];
            value.AsSpan().Fill((byte)(i + generation));
            long revision = generation * 10 + 1;

            rows.Add(new(
                $"ranged1/ledger{i:D5}", value, revision,
                expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
                lastModifiedNode: 0, lastModifiedPhysical: revision, lastModifiedCounter: 0,
                state: (int)KeyValueState.Set));

            if (rows.Count == Batch || i == Rows - 1)
            {
                Assert.True(node.Backend.StoreKeyValues(rows));
                rows = new(Batch);
            }
        }
    }

    private static long LiveHeap()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        return GC.GetTotalMemory(forceFullCollection: true);
    }

    // The source and the warm-up node each hold a full copy of the partition, so they live in methods of their own:
    // a copy still live when the node's footprint is measured would make the install's own peak look smaller than it is.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<byte[]> ExportAsync(CancellationToken ct)
    {
        Node source = MakeNode();
        Populate(source, generation: 2);

        await using Stream exported = await source.Transfer.ExportPartitionState(2, 42, ct);
        using MemoryStream copy = new();
        await exported.CopyToAsync(copy, ct);
        return copy.ToArray();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task WarmUpAsync(byte[] snapshot, CancellationToken ct)
    {
        Node warm = MakeNode();
        await warm.Transfer.ImportPartitionState(2, new MemoryStream(snapshot, writable: false), ct);
    }

    [Fact]
    public async Task Install_PeakLiveHeap_AboveTheNodesFootprint_IsASmallFractionOfTheSnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        byte[] snapshot = await ExportAsync(ct);

        // Warm-up on a throwaway node, so JIT and first-use allocations are not charged to the measured install.
        await WarmUpAsync(snapshot, ct);

        // The code after an await runs inside the completion of the awaited method, before that method's frame, and
        // with it the partition copy it held, is gone. Yield so it unwinds before anything is measured.
        await Task.Yield();

        Node target = MakeNode();
        Populate(target, generation: 1);

        long before = LiveHeap();
        long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        long peak = 0;

        using CancellationTokenSource stop = new();
        Thread sampler = new(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                long live = GC.GetTotalMemory(forceFullCollection: true);
                if (live > peak)
                    peak = live;

                Thread.Sleep(20);
            }
        }) { IsBackground = true, Name = "install-heap-sampler" };

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

        long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;

        // Measured with the staged snapshot still referenced, as it is until Kommander disposes it after the install.
        long after = LiveHeap();
        GC.KeepAlive(snapshot);

        Assert.Equal(Records, target.Records.Count);
        Assert.Equal(Receipts, target.Receipts.Count);
        Assert.Equal(21, target.Backend.GetKeyValue("ranged1/ledger00000")!.Revision);
        Assert.Equal(21, target.Backend.GetKeyValue($"ranged1/ledger{Rows - 1:D5}")!.Revision);

        long footprint = Math.Max(before, after);
        long excess = peak - footprint;

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"snapshot {snapshot.Length:N0} B; live heap before {before:N0} B, after {after:N0} B, sampled peak {peak:N0} B; " +
            $"peak above the larger footprint {excess:N0} B ({(double)excess / snapshot.Length:F2}x the snapshot); " +
            $"allocated during the install {allocated:N0} B ({(double)allocated / snapshot.Length:F2}x the snapshot)");

        Assert.True(excess < snapshot.Length / 4,
            $"the install's live heap peaked {excess:N0} B above the node's footprint for a {snapshot.Length:N0} B snapshot");
    }
}
