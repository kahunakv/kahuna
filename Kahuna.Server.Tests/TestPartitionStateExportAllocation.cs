
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Measures process-wide allocation, so it must not share the process with tests that allocate in
/// parallel: the collection disables parallelisation and runs on its own.
/// </summary>
[CollectionDefinition("ExclusiveAllocationMeasurement", DisableParallelization = true)]
public sealed class ExclusiveAllocationMeasurementCollection { }

/// <summary>
/// The whole-partition export used to materialise the snapshot in one <see cref="MemoryStream"/> that
/// grew by doubling: every doubling above 85 KB was a large-object-heap allocation and the total
/// allocated per export was at least twice the snapshot's size. The export now builds the snapshot in
/// pooled 64 KB segments, so the bytes allocated per export stay at about the snapshot's size (cold
/// pool) or well below it (warm pool). The export's page scan hops threads, so this is a process-wide
/// measurement and the test runs in an exclusive collection.
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestPartitionStateExportAllocation
{
    private const int HashPoolSize = 3;

    private static PartitionStateTransfer MakeTransfer(IPersistenceBackend backend)
    {
        RangeMap map = new([new RangeDescriptor { KeySpace = "ranged1", PartitionId = 2, Generation = 1 }]);

        return new PartitionStateTransfer(
            new PartitionDataEnumerator(backend, () => map, HashPoolSize), backend,
            new CompletionReceiptStore(), new TransactionRecordStore(), new PreparedIntentStore(),
            () => map, HashPoolSize,
            () => Task.CompletedTask,
            storagePath: null, storageRevision: "rev", NullLogger<IKahuna>.Instance);
    }

    [Fact]
    public async Task Export_AllocatesAboutTheSnapshotSize_NotADoublingLadder()
    {
        MemoryPersistenceBackend backend = new();

        List<PersistenceRequestItem> rows = [];
        byte[] value = new byte[8 * 1024];
        for (int i = 0; i < 1_024; i++)
        {
            value[0] = (byte)i;
            rows.Add(new PersistenceRequestItem(
                $"ranged1/k{i:D6}", (byte[])value.Clone(), i + 1,
                expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
                lastModifiedNode: 0, lastModifiedPhysical: i + 1, lastModifiedCounter: 0,
                state: (int)KeyValueState.Set));
        }
        Assert.True(backend.StoreKeyValues(rows));

        PartitionStateTransfer transfer = MakeTransfer(backend);
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Warm-up: JIT and the first rentals; disposing returns the segments to the pool.
        await using (Stream warmUp = await transfer.ExportPartitionState(2, 42, ct))
            Assert.True(warmUp.Length > 8 * 1024 * 1024);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        await using Stream stream = await transfer.ExportPartitionState(2, 42, ct);
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

        // A doubling buffer allocates at least 2x the final size (the sum of the ladder); the segmented build
        // allocates at most the size itself plus the per-page message objects, and less once the pool is warm.
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"exporting a {stream.Length} B snapshot allocated {allocated} B ({(double)allocated / stream.Length:F2}x)");

        Assert.IsType<SegmentedBufferStream>(stream);
        Assert.True(allocated < 1.6 * stream.Length,
            $"exporting a {stream.Length} B snapshot allocated {allocated} B ({(double)allocated / stream.Length:F2}x)");
    }
}
