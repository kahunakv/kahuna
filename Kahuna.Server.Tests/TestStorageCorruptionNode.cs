using System.Security.Cryptography;
using System.Text;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Storage damage seen through a whole node. A RocksDB-backed node whose SST was damaged on disk answers
/// the first read that reaches the damage with the terminal <c>StorageCorrupt</c> response — never
/// <c>MustRetry</c>, never a short page — and then takes itself out of consensus for the partitions it
/// hosts: candidacy withheld, leadership stepped down, readiness reporting the engine's message. A node
/// whose store directory was lost while its Raft log survived starts, reports the store as replaced, and
/// rebuilds the store from the log it still holds.
/// </summary>
public sealed class TestStorageCorruptionNode : IDisposable
{
    private const string Revision = "rocksdb-corruption";

    private const string Prefix = "corrupt/";

    private const int Keys = 256;

    private const int ValueBytes = 2048;

    private readonly ILoggerFactory loggerFactory;

    public TestStorageCorruptionNode(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    public void Dispose()
    {
        loggerFactory.Dispose();
    }

    [Fact]
    public async Task DamagedBlock_FirstReadAnswersStorageCorrupt_AndTheNodeStopsServing()
    {
        string storagePath = CreateTempDir("kahuna-node-corrupt-");
        try
        {
            byte[][] values = new byte[Keys][];
            for (int i = 0; i < Keys; i++)
            {
                values[i] = new byte[ValueBytes];
                RandomNumberGenerator.Fill(values[i]);
            }

            await using (EmbeddedKahunaNode first = new(Options(storagePath), loggerFactory))
            {
                await first.StartAsync(TestContext.Current.CancellationToken);

                for (int i = 0; i < Keys; i++)
                {
                    (KeyValueResponseType response, _, _) = await first.Kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, Key(i), values[i], null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                        TestContext.Current.CancellationToken);

                    Assert.Equal(KeyValueResponseType.Set, response);
                }

                await first.FlushAsync();
                Assert.True(ClusterHandlers.BuildHealthResponse(first.Raft, first.Kahuna).Ready);
            }

            // Every flushed row to an SST, then one byte of a data block flipped on disk.
            using (RocksDbPersistenceBackend closed = new(storagePath, Revision))
                closed.FlushMemTablesForTesting();

            FlipByteAtMiddle(LargestSst(storagePath));

            await using EmbeddedKahunaNode second = new(Options(storagePath), loggerFactory);
            await second.StartAsync(TestContext.Current.CancellationToken);

            Assert.True(ClusterHandlers.BuildHealthResponse(second.Raft, second.Kahuna).Ready);
            Assert.True(second.Kahuna.GetStorageHealth().IsHealthy);

            // A scan over every key reaches the damaged block: the answer is terminal, and nothing is served from it.
            KeyValueGetByRangeResult scan = await second.Kahuna.LocateAndGetByRange(
                HLCTimestamp.Zero, Prefix, null, true, null, true, Keys * 2, HLCTimestamp.Zero, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.StorageCorrupt, scan.Type);
            Assert.Empty(scan.Items);

            // The store is fail-stopped and the node says so at once.
            KahunaStorageHealth storage = second.Kahuna.GetStorageHealth();
            Assert.NotNull(storage.StoreFailStopReason);
            Assert.Contains("Corruption:", storage.StoreFailStopReason, StringComparison.Ordinal);

            KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(second.Raft, second.Kahuna);
            Assert.False(health.Ready);
            Assert.NotNull(health.StorageFault);
            Assert.Contains("Corruption:", health.StorageFault, StringComparison.Ordinal);

            // The containment follows: candidacy withheld and leadership gone for every hosted data partition.
            List<int> hosted = [.. second.Raft.GetPartitionMap()
                .Where(r => r.PartitionId != RaftSystemConfig.SystemPartition && second.Raft.HostsPartition(r.PartitionId))
                .Select(r => r.PartitionId)];
            Assert.NotEmpty(hosted);

            await WaitUntilAsync(() => hosted.All(second.Raft.IsCandidacyWithheld), 15_000);
            await WaitUntilAsync(async () =>
            {
                foreach (int partitionId in hosted)
                    if (await second.Raft.AmILeader(partitionId, TestContext.Current.CancellationToken))
                        return false;
                return true;
            }, 15_000);

            // A read of an intact key is refused the same way: the store serves nothing once damaged. Without
            // a leader the locator may answer MustRetry instead; either way nothing is served.
            (KeyValueResponseType later, ReadOnlyKeyValueEntry? entry) = await second.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, Key(0), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.True(later is KeyValueResponseType.StorageCorrupt or KeyValueResponseType.MustRetry or KeyValueResponseType.Errored,
                $"a damaged store served a read with {later}");
            Assert.True(entry?.Value is null, "a damaged store served a value");
        }
        finally
        {
            TryDeleteDir(storagePath);
        }
    }

    [Fact]
    public async Task LostStoreDirectory_IsReportedAsReplaced_AndRebuiltFromTheLog()
    {
        string storagePath = CreateTempDir("kahuna-node-wiped-store-");
        string walPath = CreateTempDir("kahuna-node-wiped-wal-");
        try
        {
            const int keys = 16;

            await using (EmbeddedKahunaNode first = new(Options(storagePath, walPath), loggerFactory))
            {
                await first.StartAsync(TestContext.Current.CancellationToken);

                for (int i = 0; i < keys; i++)
                {
                    (KeyValueResponseType response, _, _) = await first.Kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, $"wiped/k{i:D3}", Encoding.UTF8.GetBytes("v" + i), null, -1, KeyValueFlags.Set, 0,
                        KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

                    Assert.Equal(KeyValueResponseType.Set, response);
                }

                await first.FlushAsync();
                Assert.Null(first.Kahuna.GetStorageHealth().StoreReplacedNotice);
            }

            // The store directory is gone; the Raft log is not.
            Directory.Delete(Path.Combine(storagePath, Revision), recursive: true);

            await using EmbeddedKahunaNode second = new(Options(storagePath, walPath), loggerFactory);

            // Found at construction, before the log is replayed.
            KahunaStorageHealth storage = second.Kahuna.GetStorageHealth();
            Assert.NotNull(storage.StoreReplacedNotice);
            Assert.Contains("lost or replaced", storage.StoreReplacedNotice, StringComparison.Ordinal);

            await second.StartAsync(TestContext.Current.CancellationToken);

            KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(second.Raft, second.Kahuna);
            Assert.False(health.Ready);
            Assert.NotNull(health.StorageFault);
            Assert.Contains("lost or replaced", health.StorageFault, StringComparison.Ordinal);

            // The log was never compacted, so every row is rebuilt by replay and reads are whole again.
            for (int i = 0; i < keys; i++)
            {
                (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await second.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, $"wiped/k{i:D3}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

                Assert.Equal(KeyValueResponseType.Get, response);
                Assert.NotNull(entry);
                Assert.Equal(Encoding.UTF8.GetBytes("v" + i), entry.Value);
            }

            // The rebuilt store carries this node's stamp.
            await second.FlushAsync();
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    // ── Scaffolding ──────────────────────────────────────────────────────────────────────

    private static string Key(int i) => $"{Prefix}k{i:D4}";

    private static EmbeddedKahunaOptions Options(string storagePath, string? walPath = null) => new()
    {
        Storage = "rocksdb",
        StoragePath = storagePath,
        StorageRevision = Revision,
        WalStorage = walPath is null ? "memory" : "rocksdb",
        WalPath = walPath ?? "",
        WalRevision = walPath is null ? "" : "wal",
        InitialPartitions = 1,
        // A short writer tick, so the writer's observation of the backend's latch re-asserts the containment
        // quickly; the revision sweep stays off so no background scan reaches the damage before a read does.
        DirtyObjectsWriterDelay = 200,
        CheckpointInterval = TimeSpan.FromHours(1),
    };

    private static string LargestSst(string storagePath)
    {
        string[] files = Directory.GetFiles(Path.Combine(storagePath, Revision), "*.sst");
        Assert.NotEmpty(files);
        return files.OrderByDescending(f => new FileInfo(f).Length).First();
    }

    private static void FlipByteAtMiddle(string sst)
    {
        using FileStream stream = new(sst, FileMode.Open, FileAccess.ReadWrite);
        long offset = stream.Length / 2;
        stream.Position = offset;
        int original = stream.ReadByte();
        stream.Position = offset;
        stream.WriteByte((byte)(original ^ 0xFF));
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, int timeoutMs)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (!predicate())
        {
            Assert.True(Environment.TickCount64 < deadline, "condition not reached in time");
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> predicate, int timeoutMs)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (!await predicate())
        {
            Assert.True(Environment.TickCount64 < deadline, "condition not reached in time");
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }
}
