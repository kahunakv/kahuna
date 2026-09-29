using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A restarting node reloads the durable transaction-record and completion-receipt sets from their per-partition
/// snapshot files. The load used to read each file into one array and parse it into one message holding every
/// entry before the first entry reached the store, so the node held the file, a protobuf object per entry and
/// the loaded set at once. It now decodes each entry straight off the file into the store, so the heap the load
/// needs beyond the set it loads is one entry, independent of the file's size.
///
/// <para>Measured with the same forced-collection sampler as the install's memory test, and therefore in the
/// same exclusive collection.</para>
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestDurableStoreLoadMemory : IDisposable
{
    private const int PartitionId = 1;

    private const int Entries = 80_000;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-load-mem-" + Guid.NewGuid().ToString("N"));

    public TestDurableStoreLoadMemory() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    /// <summary>Runs <paramref name="load"/> under a sampler that forces full collections, and returns the loaded
    /// object together with how far the sampled live heap peaked above the heap with the loaded object retained.</summary>
    private static (T Loaded, long Excess, long Installed) MeasureLoad<T>(Func<T> load) where T : class
    {
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
        }) { IsBackground = true, Name = "load-heap-sampler" };

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        T loaded;
        sampler.Start();
        try
        {
            loaded = load();
        }
        finally
        {
            stop.Cancel();
            sampler.Join();
        }

        long installed = GC.GetTotalMemory(forceFullCollection: true);
        GC.KeepAlive(loaded);
        return (loaded, peak - installed, installed);
    }

    [Fact]
    public void RecordStore_ColdRestart_PeakLiveHeap_StaysNearTheLoadedSet()
    {
        TransactionRecordStore source = new(dir, "rev", null);
        source.AttachAnchorResolver(_ => (PartitionId, 0));

        for (int i = 0; i < Entries; i++)
        {
            HLCTimestamp txId = Ts(1_000_000 + i);
            string anchor = $"ranged1/account{i:D7}";
            List<TransactionParticipantRef> manifest =
                [new(anchor, KeyValueDurability.Persistent), new($"ranged1/account{(i + 1) % Entries:D7}", KeyValueDurability.Persistent)];

            source.Apply(new InitializeTransactionCommand(txId, 1, "coordinator", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
            source.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100)));
        }

        Assert.True(source.PersistSnapshot(PartitionId));
        source = null!;

        long fileBytes = new FileInfo(Directory.GetFiles(dir, "transactionrecord_rev_p*.snapshot").Single()).Length;

        _ = new TransactionRecordStore(dir, "rev", null); // warm-up

        (TransactionRecordStore loaded, long excess, long installed) = MeasureLoad(() => new TransactionRecordStore(dir, "rev", null));

        Assert.Equal(Entries, loaded.Count);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"record file {fileBytes} B, loaded live heap {installed} B, excess {excess} B ({(double)excess / fileBytes:F2}x the file)");
        Assert.True(excess < fileBytes / 2, $"loading a {fileBytes} B record file peaked {excess} B above the loaded set");
    }

    [Fact]
    public void ReceiptStore_ColdRestart_PeakLiveHeap_StaysNearTheLoadedSet()
    {
        CompletionReceiptStore source = new(dir, "rev", NullLogger<IKahuna>.Instance);
        source.AttachPartitionResolver(_ => PartitionId);

        for (int i = 0; i < Entries * 2; i++)
            source.Record(Ts(5_000_000 + i), $"ranged1/account{i:D7}", "ranged1/anchor", KeyValueDurability.Persistent);

        Assert.True(source.PersistSnapshot(PartitionId));
        source = null!;

        long fileBytes = new FileInfo(Directory.GetFiles(dir, "completionreceipts_rev_p*.snapshot").Single()).Length;

        _ = new CompletionReceiptStore(dir, "rev", NullLogger<IKahuna>.Instance); // warm-up

        (CompletionReceiptStore loaded, long excess, long installed) = MeasureLoad(() => new CompletionReceiptStore(dir, "rev", NullLogger<IKahuna>.Instance));

        Assert.Equal(Entries * 2, loaded.Count);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"receipt file {fileBytes} B, loaded live heap {installed} B, excess {excess} B ({(double)excess / fileBytes:F2}x the file)");
        Assert.True(excess < fileBytes / 2, $"loading a {fileBytes} B receipt file peaked {excess} B above the loaded set");
    }
}
