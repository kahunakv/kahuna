using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;

namespace Kahuna.Server.Tests;

/// <summary>
/// A whole-partition export past its memory budget moves to a file. Its bytes must be exactly those of an export held in
/// memory, an install from it must seed the same rows, and nothing may stay on disk once it is released or after a crash.
/// </summary>
public sealed class TestPartitionStateExportSpill : IDisposable
{
    private const int HashPoolSize = 3;

    private const int Partition = 2;

    private const int Rows = 256;

    private readonly string storagePath = Directory.CreateTempSubdirectory("export-spill-").FullName;

    public void Dispose() => Directory.Delete(storagePath, recursive: true);

    private string SpillDirectory => Path.Combine(storagePath, "snapshot-export_rev");

    private string[] SpillFiles() => Directory.GetFiles(SpillDirectory, "*" + SpillableBufferStream.SpillFileExtension);

    private static PartitionStateTransfer MakeTransfer(IPersistenceBackend backend, string? path, ILogger<IKahuna>? logger = null)
    {
        RangeMap map = new([new RangeDescriptor { KeySpace = "ranged1", PartitionId = Partition, Generation = 1 }]);

        return new PartitionStateTransfer(
            new PartitionDataEnumerator(backend, () => map, new DataPartitionRouter(HashPoolSize)), backend,
            new CompletionReceiptStore(), new TransactionRecordStore(), new PreparedIntentStore(),
            () => map, new DataPartitionRouter(HashPoolSize),
            () => Task.CompletedTask,
            path, storageRevision: "rev", logger ?? NullLogger<IKahuna>.Instance);
    }

    private static MemoryPersistenceBackend Populated()
    {
        MemoryPersistenceBackend backend = new();
        List<PersistenceRequestItem> rows = [];

        for (int i = 0; i < Rows; i++)
        {
            byte[] value = new byte[8 * 1024];
            new Random(i).NextBytes(value);

            rows.Add(new PersistenceRequestItem(
                $"ranged1/k{i:D6}", value, i + 1,
                expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
                lastModifiedNode: 0, lastModifiedPhysical: i + 1, lastModifiedCounter: 0,
                state: (int)KeyValueState.Set));
        }

        Assert.True(backend.StoreKeyValues(rows));
        return backend;
    }

    private static async Task<byte[]> ReadAll(Stream stream)
    {
        using MemoryStream copy = new();
        await stream.CopyToAsync(copy, TestContext.Current.CancellationToken);
        return copy.ToArray();
    }

    [Fact]
    public async Task ASpilledExport_IsByteForByteTheExportHeldInMemory()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MemoryPersistenceBackend backend = Populated();

        await using Stream inMemory = await MakeTransfer(backend, path: null).ExportPartitionState(Partition, 42, ct);
        Assert.False(Assert.IsType<SpillableBufferStream>(inMemory).IsSpilled);

        PartitionStateTransfer spilling = MakeTransfer(backend, storagePath);
        spilling.ExportMemoryBudgetBytes = 64 * 1024;

        await using Stream spilled = await spilling.ExportPartitionState(Partition, 42, ct);
        Assert.True(Assert.IsType<SpillableBufferStream>(spilled).IsSpilled);

        Assert.Equal(await ReadAll(inMemory), await ReadAll(spilled));
    }

    [Fact]
    public async Task AnInstallFromASpilledExport_SeedsEveryRow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MemoryPersistenceBackend source = Populated();

        PartitionStateTransfer spilling = MakeTransfer(source, storagePath);
        spilling.ExportMemoryBudgetBytes = 64 * 1024;

        byte[] snapshot;
        await using (Stream exported = await spilling.ExportPartitionState(Partition, 42, ct))
            snapshot = await ReadAll(exported);

        MemoryPersistenceBackend target = new();
        await MakeTransfer(target, path: null).ImportPartitionState(Partition, new MemoryStream(snapshot, writable: false), ct);

        for (int i = 0; i < Rows; i++)
        {
            byte[] expected = new byte[8 * 1024];
            new Random(i).NextBytes(expected);

            ReadOnlyKeyValueEntry? row = target.GetKeyValue($"ranged1/k{i:D6}");
            Assert.NotNull(row);
            Assert.Equal(expected, row.Value);
        }
    }

    [Fact]
    public async Task ReleasingAnExport_LeavesNothingOnDisk()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        PartitionStateTransfer spilling = MakeTransfer(Populated(), storagePath);
        spilling.ExportMemoryBudgetBytes = 64 * 1024;

        Stream exported = await spilling.ExportPartitionState(Partition, 42, ct);
        Assert.NotEmpty(SpillFiles());

        await exported.DisposeAsync();
        Assert.Empty(SpillFiles());
    }

    [Fact]
    public async Task ACancelledExport_LeavesNothingOnDisk()
    {
        using CancellationTokenSource cancelled = new();
        await cancelled.CancelAsync();

        PartitionStateTransfer spilling = MakeTransfer(Populated(), storagePath);
        spilling.ExportMemoryBudgetBytes = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => spilling.ExportPartitionState(Partition, 42, cancelled.Token));
        Assert.Empty(SpillFiles());
    }

    [Fact]
    public void AFileLeftByACrash_IsRemovedAtStartup()
    {
        Directory.CreateDirectory(SpillDirectory);
        string leftover = Path.Combine(SpillDirectory, "dead" + SpillableBufferStream.SpillFileExtension);
        string unrelated = Path.Combine(SpillDirectory, "keep.txt");
        File.WriteAllText(leftover, "from a process that died");
        File.WriteAllText(unrelated, "not ours");

        MakeTransfer(new MemoryPersistenceBackend(), storagePath);

        Assert.False(File.Exists(leftover));
        Assert.True(File.Exists(unrelated));
    }

    [Fact]
    public async Task AnUnusableDirectory_LeavesExportsInMemory()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // A file where the storage path should be, so the directory cannot be created.
        string blocked = Path.Combine(storagePath, "a-file");
        File.WriteAllText(blocked, "");

        CapturingLogger logger = new();
        PartitionStateTransfer transfer = MakeTransfer(Populated(), blocked, logger);
        transfer.ExportMemoryBudgetBytes = 1024;

        await using Stream exported = await transfer.ExportPartitionState(Partition, 42, ct);

        Assert.False(Assert.IsType<SpillableBufferStream>(exported).IsSpilled);
        Assert.Contains(logger.Warnings, line => line.Contains("cannot spill"));
    }

    [Fact]
    public async Task AnExportAboveTheReceiveCap_NamesTheCap()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        CapturingLogger logger = new();
        PartitionStateTransfer transfer = MakeTransfer(Populated(), path: null, logger);
        transfer.AttachReceiveCap(1024 * 1024);

        await using Stream exported = await transfer.ExportPartitionState(Partition, 42, ct);

        Assert.True(exported.Length > 1024 * 1024);
        Assert.Contains(logger.Warnings, line => line.Contains("1048576") && line.Contains("RaftSnapshotMaxPendingBytes"));
    }

    [Fact]
    public async Task AnExportWithinTheReceiveCap_SaysNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        CapturingLogger logger = new();
        PartitionStateTransfer transfer = MakeTransfer(Populated(), path: null, logger);
        transfer.AttachReceiveCap(1024L * 1024 * 1024);

        await using Stream exported = await transfer.ExportPartitionState(Partition, 42, ct);

        Assert.Empty(logger.Warnings);
    }

    private sealed class CapturingLogger : ILogger<IKahuna>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
                lock (Warnings)
                    Warnings.Add(formatter(state, exception));
        }
    }
}
