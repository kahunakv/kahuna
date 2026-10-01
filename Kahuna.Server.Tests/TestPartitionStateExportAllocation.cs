
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

        // The number is process-wide: the export's page scan hops threads, so per-thread accounting cannot
        // isolate it, and anything else the process does meanwhile (a straggling teardown of an earlier
        // test's cluster, the finalizer thread) is charged to the export. That noise is bursty, not sustained,
        // so the export is measured several times and the smallest attempt is the one judged: a doubling
        // ladder costs at least 2x on every attempt, so the minimum still catches it, while one attempt hit
        // by unrelated allocation no longer fails the test. The ambient rate is sampled first and reported
        // with every attempt so a failure says whether the export or its neighbours allocated.
        long ambientBefore = GC.GetTotalAllocatedBytes(precise: true);
        await Task.Delay(100, ct);
        long ambientPerSecond = (GC.GetTotalAllocatedBytes(precise: true) - ambientBefore) * 10;

        const int attempts = 5;
        long[] allocatedPerAttempt = new long[attempts];
        long length = 0;

        for (int attempt = 0; attempt < attempts; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            long before = GC.GetTotalAllocatedBytes(precise: true);
            await using Stream stream = await transfer.ExportPartitionState(2, 42, ct);
            allocatedPerAttempt[attempt] = GC.GetTotalAllocatedBytes(precise: true) - before;

            Assert.IsType<SegmentedBufferStream>(stream);
            length = stream.Length;
        }

        long allocated = allocatedPerAttempt.Min();

        string report =
            $"exporting a {length} B snapshot allocated at best {allocated} B ({(double)allocated / length:F2}x); " +
            $"attempts: {string.Join(", ", allocatedPerAttempt.Select(a => $"{a} B ({(double)a / length:F2}x)"))}; " +
            $"ambient allocation before measuring: {ambientPerSecond} B/s";

        // A doubling buffer allocates at least 2x the final size (the sum of the ladder); the segmented build
        // allocates at most the size itself plus the per-page message objects, and less once the pool is warm.
        TestContext.Current.TestOutputHelper?.WriteLine(report);

        Assert.True(allocated < 1.6 * length, report);
    }
}
