using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The SQLite backend serves concurrent reads of one shard from several threads (the backend read
/// scheduler runs reads of one partition in parallel). A <c>SqliteConnection</c> is not thread-safe: its
/// command-tracking list is mutated by every command created or disposed on it, so readers that share
/// one connection corrupt that list. The damage surfaces as a read that fails (and reports the key as
/// absent) or as a <see cref="NullReferenceException"/> in <c>SqliteConnection.Close()</c> at dispose.
/// </summary>
public sealed class TestSqliteBackendConcurrency : IDisposable
{
    private const string Bucket = "bucket";

    private const int KeyCount = 64;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_sqlite_concurrency_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private string NewDir()
    {
        string dir = Path.Combine(_tempRoot, Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static PersistenceRequestItem Item(string key, long revision) =>
        new(key, Encoding.UTF8.GetBytes("v" + revision), revision,
            0, 0, 0, 0, 0, 0,
            0, revision, 0, (int)KeyValueState.Set);

    // Every key lives under one bucket, so every key routes to the same shard and its connection.
    private static SqlitePersistenceBackend NewPopulatedBackend(string dir, ILogger? logger = null)
    {
        SqlitePersistenceBackend backend = new(dir, "cc", logger);

        List<PersistenceRequestItem> items = new(KeyCount);
        for (int i = 0; i < KeyCount; i++)
            items.Add(Item($"{Bucket}/{i:D3}", i + 1));

        Assert.True(backend.StoreKeyValues(items));
        return backend;
    }

    [Fact]
    public void ConcurrentReadsOfOneShard_NeverMissAStoredKey_AndDisposeIsClean()
    {
        const int rounds = 12;
        const int threads = 8;
        const int readsPerThread = 1500;

        for (int round = 0; round < rounds; round++)
        {
            ErrorLogger errors = new();
            SqlitePersistenceBackend backend = NewPopulatedBackend(NewDir(), errors);

            int misses = 0;
            using Barrier start = new(threads);

            Thread[] readers = new Thread[threads];
            for (int t = 0; t < threads; t++)
            {
                int seed = t;
                readers[t] = new Thread(() =>
                {
                    start.SignalAndWait();
                    for (int i = 0; i < readsPerThread; i++)
                    {
                        int k = (seed * 31 + i) % KeyCount;
                        if (i % 16 == 0)
                        {
                            if (backend.GetKeyValueByPrefix(Bucket).Count != KeyCount)
                                Interlocked.Increment(ref misses);
                        }
                        else if (backend.GetKeyValue($"{Bucket}/{k:D3}") is null)
                        {
                            Interlocked.Increment(ref misses);
                        }
                    }
                });
                readers[t].Start();
            }

            foreach (Thread reader in readers)
                reader.Join();

            Assert.True(misses == 0,
                $"round {round}: {misses} reads missed a stored key; first errors: " +
                string.Join(" | ", errors.Exceptions.Take(3)));
            Assert.Empty(errors.Exceptions);

            // The original failure: Close() walked a corrupted command list and threw.
            backend.Dispose();
        }
    }

    [Fact]
    public void CallsAfterDispose_ThrowObjectDisposed_AndASecondDisposeIsANoOp()
    {
        SqlitePersistenceBackend backend = NewPopulatedBackend(NewDir());
        backend.Dispose();

        // A disposed backend must not report a stored key as absent, nor reopen a shard.
        Assert.Throws<ObjectDisposedException>(() => backend.GetKeyValue($"{Bucket}/001"));
        Assert.Throws<ObjectDisposedException>(() => backend.GetKeyValueByPrefix(Bucket));
        Assert.Throws<ObjectDisposedException>(() => backend.StoreKeyValues([Item($"{Bucket}/999", 1)]));
        Assert.Throws<ObjectDisposedException>(() => backend.GetKeyValue("never-opened-bucket/key"));

        backend.Dispose();
    }

    [Fact]
    public void DisposeThatFailsOnOneShard_StillClosesEveryOtherShard()
    {
        string dir = NewDir();
        SqlitePersistenceBackend backend = new(dir, "cc");

        // Spread keys over many buckets so several shard files exist.
        List<PersistenceRequestItem> items = [];
        for (int b = 0; b < 32; b++)
            items.Add(Item($"bucket{b}/key", b + 1));
        Assert.True(backend.StoreKeyValues(items));

        List<int> opened = [];
        for (int shard = 0; shard < 8; shard++)
            if (File.Exists(ShardFile(dir, shard)))
                opened.Add(shard);
        Assert.True(opened.Count >= 2, $"expected several shards, got {opened.Count}");

        int pinned = opened[0];
        foreach (int shard in opened)
            Assert.True(File.Exists(ShardFile(dir, shard) + "-wal"), $"shard {shard} has no WAL file while open");

        // A read still in flight at dispose: the pinned shard's writer lock times out and that shard
        // fails to close. The reader/writer lock is thread-affine, so one thread holds and releases it.
        using ManualResetEventSlim held = new();
        using ManualResetEventSlim release = new();
        Thread reader = new(() =>
        {
            using SqlitePersistenceBackend.ShardReadLease lease = backend.EnterRead(pinned);
            held.Set();
            release.Wait();
        });
        reader.Start();
        Assert.True(held.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));

        try
        {
            AggregateException failure = Assert.Throws<AggregateException>(backend.Dispose);
            Exception single = Assert.Single(failure.InnerExceptions);
            Assert.Contains($"shard {pinned}", single.Message, StringComparison.Ordinal);
        }
        finally
        {
            release.Set();
            reader.Join();
        }

        // Closing the last connection of a WAL database checkpoints and removes its -wal file. Every
        // shard but the pinned one must be closed.
        foreach (int shard in opened)
        {
            if (shard == pinned)
                Assert.True(File.Exists(ShardFile(dir, shard) + "-wal"));
            else
                Assert.False(File.Exists(ShardFile(dir, shard) + "-wal"), $"shard {shard} was left open");
        }
    }

    private static string ShardFile(string dir, int shard) => Path.Combine(dir, $"kahuna{shard}_cc.db");

    private sealed class ErrorLogger : ILogger
    {
        public ConcurrentQueue<string> Exceptions { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Error)
                Exceptions.Enqueue(exception?.ToString() ?? formatter(state, exception));
        }
    }
}
