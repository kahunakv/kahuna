
using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Production-hardening tests for the backup / PITR path:
///   • restore/bootstrap fail closed when a persistence batch is not stored;
///   • incremental capture is streaming and bounded (and cancellable), legacy JSON arrays stream too;
///   • the exact bytes that passed digest verification are the bytes replayed (verify/use race);
///   • bootstrap and the restore API resolve a millisecond target to the same inclusive end-of-ms HLC.
/// </summary>
public sealed class TestPitrHardening : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_hardening_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────

    private string ArtifactsDir(string name) => Path.Combine(_tempRoot, "artifacts_" + name);
    private BackupCatalog NewCatalog(string name) => new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "cat_" + name)));
    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };
    private static HLCTimestamp T(long ms) => new(0, ms, 0);
    private static HLCTimestamp T(long ms, uint counter) => new(0, ms, counter);

    private static RaftLog KvLog(long id, long timeMs, string key, string value, long revision, uint counter = 0)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedPhysical = timeMs,
            LastModifiedCounter = counter,
        };
        return new RaftLog
        {
            Id = id, Term = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, timeMs, 0),
            LogType = ReplicationTypes.KeyValues, LogData = ReplicationSerializer.Serialize(msg),
        };
    }

    private static InMemoryWAL BuildWal(params (int partition, RaftLog log)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byPartition = [];
        foreach ((int partition, RaftLog log) in entries)
            (byPartition.TryGetValue(partition, out List<RaftLog>? l) ? l : byPartition[partition] = []).Add(log);
        foreach ((int p, List<RaftLog> logs) in byPartition)
            wal.Write([(p, logs)]);
        return wal;
    }

    private static void FlushWalToBackend(InMemoryWAL wal, int partitionId, MemoryPersistenceBackend backend)
    {
        foreach (RaftLog log in wal.ReadLogsRange(partitionId, 1))
        {
            if (log.Type != RaftLogType.Committed || log.LogType != ReplicationTypes.KeyValues || log.LogData is null)
                continue;
            KeyValueMessage msg = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);
            (KeyValueState state, byte[]? value) = KeyValueMessageDecoder.Decode(msg);
            if (state == KeyValueState.Undefined) continue;
            backend.StoreKeyValues([new PersistenceRequestItem(
                msg.Key, value, msg.Revision, msg.ExpireNode, msg.ExpirePhysical, msg.ExpireCounter,
                msg.LastUsedNode, msg.LastUsedPhysical, msg.LastUsedCounter,
                msg.LastModifiedNode, msg.LastModifiedPhysical, msg.LastModifiedCounter, (int)state)]);
        }
    }

    private static string? GetValue(MemoryPersistenceBackend backend, string key)
    {
        object? entry = backend.GetKeyValue(key);
        if (entry is null) return null;
        byte[]? bytes = entry.GetType().GetProperty("Value")?.GetValue(entry) as byte[];
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    private static WalSegmentEntry Seg(long id, long ms, byte[] data) => new()
    {
        Id = id, Term = 1, TimeNode = 0, TimePhysical = ms, TimeCounter = 0,
        LogType = ReplicationTypes.KeyValues, LogData = data,
    };

    // ── faf4cc4a: fail closed when a persistence batch is not stored ─────────────────────────────

    /// <summary>Wraps a real backend but forces <see cref="StoreKeyValues"/> to fail on a chosen call.</summary>
    private sealed class FailingBackend(MemoryPersistenceBackend inner, int failOnCall) : IPersistenceBackend
    {
        private int _calls;
        public int StoreCalls => _calls;

        public bool StoreKeyValues(List<PersistenceRequestItem> items)
        {
            _calls++;
            if (_calls == failOnCall)
                return false; // mimics a backend that caught its own write exception and returned false
            return inner.StoreKeyValues(items);
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    private async Task<(IReadOnlyList<BackupManifest> chain, string artifacts, string checkpointPath)>
        BuildFullPlusIncremental(string tag, int entryCount)
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "seed", "s", 1)));
        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);
        BackupCatalog catalog = NewCatalog(tag);
        string artifacts = ArtifactsDir(tag);

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], fullBackend, artifacts, catalog);

        List<RaftLog> more = [];
        for (int i = 0; i < entryCount; i++)
            more.Add(KvLog(2 + i, 200 + i, $"k{i}", $"v{i}", 1));
        wal.Write([(1, more)]);

        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        return (chain, artifacts, checkpointPath);
    }

    [Fact]
    public async Task Restore_FailsClosed_WhenFinalBatchNotStored()
    {
        // 300 entries → two apply batches (256 + 44). Fail the final one.
        (IReadOnlyList<BackupManifest> chain, string artifacts, string cp) = await BuildFullPlusIncremental("final_fail", 300);
        FailingBackend backend = new(MemoryPersistenceBackend.OpenCheckpoint(cp), failOnCall: 2);

        await Assert.ThrowsAsync<BackupDriverException>(() =>
            RestoreEngine.RestoreAsync(chain, artifacts, T(100_000), backend));
    }

    [Fact]
    public async Task Restore_FailsClosed_WhenMiddleBatchNotStored()
    {
        // 600 entries → three apply batches (256 + 256 + 88). Fail the middle one; earlier batch already stored.
        (IReadOnlyList<BackupManifest> chain, string artifacts, string cp) = await BuildFullPlusIncremental("middle_fail", 600);
        MemoryPersistenceBackend inner = MemoryPersistenceBackend.OpenCheckpoint(cp);
        FailingBackend backend = new(inner, failOnCall: 2);

        await Assert.ThrowsAsync<BackupDriverException>(() =>
            RestoreEngine.RestoreAsync(chain, artifacts, T(100_000), backend));

        // A failed batch aborts the restore even though the first batch was already applied.
        Assert.True(backend.StoreCalls >= 2);
    }

    // ── 9428c12b: streaming, bounded, cancellable incremental capture ────────────────────────────

    [Fact]
    public void StreamingWriter_WritesJsonLines_MetadataAndDigestMatchFile()
    {
        Directory.CreateDirectory(_tempRoot);
        string path = Path.Combine(_tempRoot, "seg_meta.wal");
        List<WalSegmentEntry> entries =
        [
            Seg(10, 100, Encoding.UTF8.GetBytes("first")),
            Seg(11, 200, Encoding.UTF8.GetBytes("second")),
            Seg(12, 300, Encoding.UTF8.GetBytes("third")),
        ];

        WalSegmentEntry.SegmentWriteResult r = WalSegmentEntry.WriteSegmentStreaming(path, entries);

        Assert.Equal(3, r.EntryCount);
        Assert.Equal(new FileInfo(path).Length, r.ByteLength);
        Assert.Equal(BackupArtifactVerifier.ComputeSha256(path), r.Sha256Hex);
        Assert.Equal(T(100), r.FromHlc);
        Assert.Equal(T(300), r.ToHlc);
        Assert.Equal(12, r.ToId);
        Assert.Equal(3, File.ReadAllLines(path).Length);

        // Round-trips through the streaming reader.
        List<WalSegmentEntry> back = WalSegmentEntry.ReadSegment(path).ToList();
        Assert.Equal(3, back.Count);
        Assert.Equal("second", Encoding.UTF8.GetString(back[1].LogData!));
    }

    [Fact]
    public void StreamingWriter_NoEntries_PublishesNothing()
    {
        Directory.CreateDirectory(_tempRoot);
        string path = Path.Combine(_tempRoot, "seg_empty.wal");

        WalSegmentEntry.SegmentWriteResult r = WalSegmentEntry.WriteSegmentStreaming(path, []);

        Assert.Equal(0, r.EntryCount);
        Assert.False(File.Exists(path));
        Assert.Empty(Directory.GetFiles(_tempRoot, "seg_empty.wal.tmp_*"));
    }

    [Fact]
    public void StreamingWriter_ConsumesLazily_AndCancels_WithoutPublishingOrLeavingTemp()
    {
        Directory.CreateDirectory(_tempRoot);
        string path = Path.Combine(_tempRoot, "seg_cancel.wal");
        using CancellationTokenSource cts = new();

        int produced = 0;
        IEnumerable<WalSegmentEntry> Generate()
        {
            for (int i = 0; i < 100_000; i++)
            {
                if (i == 5)
                    cts.Cancel();
                produced++;
                yield return Seg(i, 100 + i, Encoding.UTF8.GetBytes($"payload-{i}"));
            }
        }

        Assert.Throws<OperationCanceledException>(() =>
            WalSegmentEntry.WriteSegmentStreaming(path, Generate(), cts.Token));

        // Lazy: the writer never pulled all 100k entries into memory before writing.
        Assert.True(produced < 100, $"writer materialized {produced} entries — not streaming");
        // Fail-safe: no published file and no orphan temp left behind.
        Assert.False(File.Exists(path));
        Assert.Empty(Directory.GetFiles(_tempRoot, "seg_cancel.wal.tmp_*"));
    }

    [Fact]
    public async Task LegacyJsonArray_LargerThanBuffer_StreamsSameEntries_SyncAndAsync()
    {
        Directory.CreateDirectory(_tempRoot);

        // Build entries whose combined size exceeds the 32 KB parse buffer, including one single record
        // larger than the buffer to force the incremental parser to grow and cross record boundaries.
        List<WalSegmentEntry> entries = [];
        for (int i = 0; i < 20; i++)
            entries.Add(Seg(i, 100 + i, Encoding.UTF8.GetBytes(new string((char)('a' + (i % 26)), 4096))));
        entries.Add(Seg(999, 5000, Encoding.UTF8.GetBytes(new string('Z', 50_000)))); // > 32 KB single record

        // Write as a modern JSON-Lines segment, then rewrite it as one legacy JSON array.
        string jsonl = Path.Combine(_tempRoot, "legacy_src.wal");
        WalSegmentEntry.WriteSegmentStreaming(jsonl, entries);
        string arrayPath = Path.Combine(_tempRoot, "legacy_arr.wal");
        string asArray = "[" + string.Join(",", File.ReadLines(jsonl).Where(l => l.Length > 0)) + "]";
        File.WriteAllText(arrayPath, asArray);

        List<WalSegmentEntry> sync = WalSegmentEntry.ReadSegment(arrayPath).ToList();
        List<WalSegmentEntry> async = [];
        await foreach (WalSegmentEntry e in WalSegmentEntry.ReadSegmentAsync(arrayPath))
            async.Add(e);

        Assert.Equal(entries.Count, sync.Count);
        Assert.Equal(entries.Count, async.Count);
        Assert.Equal(50_000, sync[^1].LogData!.Length);
        Assert.Equal(50_000, async[^1].LogData!.Length);
        for (int i = 0; i < entries.Count; i++)
        {
            Assert.Equal(entries[i].Id, sync[i].Id);
            Assert.Equal(entries[i].LogData, sync[i].LogData);
            Assert.Equal(entries[i].LogData, async[i].LogData);
        }
    }

    // ── 61b1f46d: replay only the exact bytes that passed digest verification ────────────────────

    [Fact]
    public async Task Restore_RejectsSegmentReplacedAfterVerification_SameSizeDifferentBytes()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "a", "v1", 1)));
        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);
        BackupCatalog catalog = NewCatalog("tamper");
        string artifacts = ArtifactsDir("tamper");

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], fullBackend, artifacts, catalog);
        wal.Write([(1, [KvLog(2, 200, "b", "vvv", 1)])]);
        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);

        string seg = Path.Combine(artifacts, inc.BackupId.ToString("N"), "partition_1.wal");

        // Simulate a source swapped AFTER the manifest recorded its digest: flip a single base64 character
        // inside the record payload. Same byte length, still valid JSON-Lines, but a different byte sequence
        // whose digest no longer matches the manifest.
        string original = File.ReadAllText(seg);
        const string marker = "\"LogData\":\"";
        int at = original.IndexOf(marker, StringComparison.Ordinal) + marker.Length;
        char[] chars = original.ToCharArray();
        chars[at] = chars[at] == 'A' ? 'B' : 'A';
        string tampered = new(chars);
        Assert.NotEqual(original, tampered);
        Assert.Equal(original.Length, tampered.Length);
        File.WriteAllText(seg, tampered);

        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(
            Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint"));
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);

        // alreadyVerified:true isolates the point-of-use binding — even when the up-front verify is skipped,
        // staging re-hashes the bytes actually consumed and rejects the swap before anything is applied.
        await Assert.ThrowsAsync<BackupArtifactException>(() =>
            RestoreEngine.RestoreAsync(chain, artifacts, T(300), restored, alreadyVerified: true));

        // The tampered value never reached the backend.
        Assert.Null(GetValue(restored, "b"));
    }

    // ── 3dc5b77f: inclusive end-of-millisecond target semantics (bootstrap == restore) ───────────

    [Fact]
    public void TargetResolver_MapsMillisecondToInclusiveEndOfMillisecond()
    {
        Assert.Equal(new HLCTimestamp(0, 1000, uint.MaxValue), PitrTargetResolver.FromUnixMilliseconds(1000));
        Assert.Equal(HLCTimestamp.Zero, PitrTargetResolver.FromUnixMilliseconds(0));
        Assert.Equal(HLCTimestamp.Zero, PitrTargetResolver.FromUnixMilliseconds(-5));
    }

    [Fact]
    public async Task Restore_AtMillisecondTarget_IncludesSameMillisecondCommitWithNonzeroCounter()
    {
        // Two commits in the SAME millisecond (200): counter 0 and counter 5. A commit in the next ms (201).
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "a", "v1", 1)));
        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);
        BackupCatalog catalog = NewCatalog("eom");
        string artifacts = ArtifactsDir("eom");

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], fullBackend, artifacts, catalog);
        wal.Write([(1,
        [
            KvLog(2, 200, "b", "v2", 1, counter: 0),
            KvLog(3, 200, "c", "v3", 1, counter: 5),
            KvLog(4, 201, "d", "v4", 1, counter: 0),
        ])]);
        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        string cp = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");

        // Inclusive end-of-millisecond target for ms 200 (what bootstrap and the restore API both resolve to).
        HLCTimestamp target = PitrTargetResolver.FromUnixMilliseconds(200);

        MemoryPersistenceBackend inclusive = MemoryPersistenceBackend.OpenCheckpoint(cp);
        await RestoreEngine.RestoreAsync(chain, artifacts, target, inclusive);
        Assert.Equal("v2", GetValue(inclusive, "b"));
        Assert.Equal("v3", GetValue(inclusive, "c")); // counter-5 same-ms commit IS included
        Assert.Null(GetValue(inclusive, "d"));         // the next millisecond is excluded

        // The buggy bare (·, 200, 0) target would have dropped the counter-5 commit.
        MemoryPersistenceBackend bare = MemoryPersistenceBackend.OpenCheckpoint(cp);
        await RestoreEngine.RestoreAsync(chain, artifacts, T(200), bare);
        Assert.Equal("v2", GetValue(bare, "b"));
        Assert.Null(GetValue(bare, "c")); // demonstrates the excluded-commit bug the fix prevents
    }
}
