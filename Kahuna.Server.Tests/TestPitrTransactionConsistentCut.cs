
using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
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
/// A multi-partition transaction commits on each partition with one shared coordinator commit HLC,
/// but each partition stamps its own Raft WAL entry Time from its local clock, so the two halves can
/// carry WAL Times that straddle a chosen cut T. These tests prove that backup capture and restore
/// cut on the shared commit HLC (payload LastModified), not the per-partition WAL Time, so such a
/// transaction is always restored whole-or-not-at-all and never torn across partitions.
/// </summary>
public sealed class TestPitrTransactionConsistentCut : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_txcut_" + Guid.NewGuid().ToString("N"));

    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private string ArtifactsDir(string name) => Path.Combine(_tempRoot, "artifacts_" + name);
    private BackupCatalog NewCatalog(string name) =>
        new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "catalog_" + name)));

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };
    private static HLCTimestamp T(long ms) => new(0, ms, 0);

    /// <summary>
    /// Builds a committed KV WAL entry whose per-partition WAL entry Time (<paramref name="walTimeMs"/>)
    /// is decoupled from the transaction commit HLC carried in the payload
    /// (<paramref name="commitMs"/>/<paramref name="commitCounter"/> in LastModified) — exactly the
    /// shape a multi-partition transaction produces, where the shared commit HLC differs from each
    /// shard's local append clock.
    /// </summary>
    private static RaftLog KvLog(long id, long walTimeMs, long commitMs, string key, string value, long revision,
        uint commitCounter = 0, KeyValueRequestType type = KeyValueRequestType.TrySet)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)type,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedNode = 0,
            LastModifiedPhysical = commitMs,
            LastModifiedCounter = commitCounter
        };

        return new RaftLog
        {
            Id = id,
            Type = RaftLogType.Committed,
            Time = new HLCTimestamp(0, walTimeMs, 0),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(msg)
        };
    }

    private static InMemoryWAL BuildWal(params (int partition, RaftLog log)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byPartition = [];
        foreach ((int partition, RaftLog log) in entries)
            (byPartition.TryGetValue(partition, out List<RaftLog>? l) ? l : byPartition[partition] = []).Add(log);
        foreach ((int partition, List<RaftLog> logs) in byPartition)
            wal.Write([(partition, logs)]);
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
                msg.Key, value, msg.Revision,
                msg.ExpireNode, msg.ExpirePhysical, msg.ExpireCounter,
                msg.LastUsedNode, msg.LastUsedPhysical, msg.LastUsedCounter,
                msg.LastModifiedNode, msg.LastModifiedPhysical, msg.LastModifiedCounter,
                (int)state)]);
        }
    }

    private static string? GetValue(MemoryPersistenceBackend backend, string key)
    {
        object? entry = backend.GetKeyValue(key);
        if (entry is null) return null;
        byte[]? bytes = entry.GetType().GetProperty("Value")?.GetValue(entry) as byte[];
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    // Full backup of a single base key per partition (committed at t=100), so the transaction under
    // test lands in the incremental — the path where the WAL-Time cut used to tear it.
    private async Task<(BackupManifest full, InMemoryWAL wal, MemoryPersistenceBackend backend, BackupCatalog catalog, string artifacts)>
        FullWithBase(string name, params int[] partitions)
    {
        (int, RaftLog)[] baseEntries = partitions
            .Select(p => (p, KvLog(1, 100, 100, $"base{p}", "b", 1)))
            .ToArray();

        InMemoryWAL wal = BuildWal(baseEntries);
        MemoryPersistenceBackend backend = new();
        foreach (int p in partitions)
            FlushWalToBackend(wal, p, backend);

        BackupCatalog catalog = NewCatalog(name);
        string artifacts = ArtifactsDir(name);

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, partitions.Select(Part).ToArray(), backend, BackupTestStores.Artifacts(artifacts), catalog);

        return (full, wal, backend, catalog, artifacts);
    }

    private static MemoryPersistenceBackend OpenBase(string artifacts, BackupManifest full) =>
        MemoryPersistenceBackend.OpenCheckpoint(Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint"));

    // ── tests ────────────────────────────────────────────────────────────────────────────

    [Theory]
    // T strictly below the shared commit HLC (250): the whole transaction is excluded.
    [InlineData(245, false)]
    // T at the shared commit HLC: the whole transaction is included (cut is inclusive).
    [InlineData(250, true)]
    // T above both WAL Times: included.
    [InlineData(260, true)]
    public async Task MultiPartitionTxn_StraddlingWalTimes_RestoredAtomically(long restoreMs, bool present)
    {
        (BackupManifest full, InMemoryWAL wal, _, BackupCatalog catalog, string artifacts) =
            await FullWithBase("straddle_" + restoreMs, 1, 2);

        // One logical transaction, shared commit HLC = 250, but the two shards' WAL entry Times
        // straddle every candidate T: shard 1 appended at 240 (local clock behind), shard 2 at 260.
        wal.Write([(1, [KvLog(2, walTimeMs: 240, commitMs: 250, "txA", "va", 1)])]);
        wal.Write([(2, [KvLog(2, walTimeMs: 260, commitMs: 250, "txB", "vb", 1)])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1), Part(2)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        MemoryPersistenceBackend restored = OpenBase(artifacts, full);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, TestContext.Current.CancellationToken);
        await RestoreEngine.RestoreAsync(chain, BackupTestStores.Artifacts(artifacts), T(restoreMs), restored, ct: TestContext.Current.CancellationToken);

        // Both halves share the same fate at every T — never one present and the other absent.
        string? a = GetValue(restored, "txA");
        string? b = GetValue(restored, "txB");
        Assert.Equal(present ? "va" : null, a);
        Assert.Equal(present ? "vb" : null, b);
        Assert.Equal(a is null, b is null); // the core anti-tearing invariant
    }

    [Fact]
    public async Task CoordinatedIncremental_DoesNotTruncateStraddlerBelowSnapshotT()
    {
        // Coordinated incremental with the cut at 250. Shard 2's half has WAL Time 260 (> 250) but a
        // commit HLC of 250 (<= cut). The old capture truncated at WAL Time > snapshotT and dropped it
        // from the segment entirely — so restore could never recover it and the transaction tore. The
        // segment must now capture the full contiguous range, and restore-to-250 must include both.
        (BackupManifest full, InMemoryWAL wal, _, BackupCatalog catalog, string artifacts) =
            await FullWithBase("coord_notrunc", 1, 2);

        wal.Write([(1, [KvLog(2, walTimeMs: 240, commitMs: 250, "txA", "va", 1)])]);
        wal.Write([(2, [KvLog(2, walTimeMs: 260, commitMs: 250, "txB", "vb", 1)])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1), Part(2)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, snapshotT: T(250), ct: TestContext.Current.CancellationToken);

        // The full contiguous range is captured even though shard 2's WAL Time exceeds the cut.
        PartitionBackupRange p2 = inc.PartitionRanges.Single(r => r.PartitionId == 2);
        Assert.Equal(2L, p2.ToIndex);

        MemoryPersistenceBackend restored = OpenBase(artifacts, full);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, TestContext.Current.CancellationToken);
        await RestoreEngine.RestoreAsync(chain, BackupTestStores.Artifacts(artifacts), T(250), restored, ct: TestContext.Current.CancellationToken);

        Assert.Equal("va", GetValue(restored, "txA"));
        Assert.Equal("vb", GetValue(restored, "txB"));
    }

    [Fact]
    public async Task CommitHlcNotMonotonicWithIndex_LaterIndexBeforeCut_StillApplied()
    {
        // On one partition, a later-index entry can carry an EARLIER commit HLC than an earlier-index
        // entry (different coordinators, different local clocks). A cut must not stop at the first
        // past-target entry — it must keep scanning, or the earlier-committed later-index entry is lost.
        (BackupManifest full, InMemoryWAL wal, _, BackupCatalog catalog, string artifacts) =
            await FullWithBase("nonmonotonic", 1);

        // index 2: commit HLC 400 (after the cut). index 3: commit HLC 200 (before the cut).
        wal.Write([(1,
        [
            KvLog(2, walTimeMs: 410, commitMs: 400, "late", "vl", 1),
            KvLog(3, walTimeMs: 420, commitMs: 200, "early", "ve", 1)
        ])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        MemoryPersistenceBackend restored = OpenBase(artifacts, full);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, TestContext.Current.CancellationToken);
        await RestoreEngine.RestoreAsync(chain, BackupTestStores.Artifacts(artifacts), T(300), restored, ct: TestContext.Current.CancellationToken);

        // The past-cut entry at index 2 (commit 400) is excluded; the before-cut entry at index 3
        // (commit 200) is still applied despite sitting after it in the log.
        Assert.Null(GetValue(restored, "late"));
        Assert.Equal("ve", GetValue(restored, "early"));
    }

    [Theory]
    // Target at counter 0 of the millisecond: only the counter-0 commit is at or before it.
    [InlineData(0u, false)]
    // Inclusive end of the millisecond (max counter, as the Unix-ms mapping produces): both included.
    [InlineData(uint.MaxValue, true)]
    public async Task SameMillisecondCommits_ResolvedByCounter(uint targetCounter, bool bothIncluded)
    {
        // Two commits share physical ms 500 but differ by counter (0 and 3). The cut orders by
        // physical, then counter, then node — so the counter decides membership within a millisecond.
        (BackupManifest full, InMemoryWAL wal, _, BackupCatalog catalog, string artifacts) =
            await FullWithBase("samems", 1);

        wal.Write([(1,
        [
            KvLog(2, walTimeMs: 500, commitMs: 500, "c0", "v0", 1, commitCounter: 0),
            KvLog(3, walTimeMs: 500, commitMs: 500, "c3", "v3", 1, commitCounter: 3)
        ])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        MemoryPersistenceBackend restored = OpenBase(artifacts, full);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, TestContext.Current.CancellationToken);
        await RestoreEngine.RestoreAsync(chain, BackupTestStores.Artifacts(artifacts), new HLCTimestamp(0, 500, targetCounter), restored, ct: TestContext.Current.CancellationToken);

        Assert.Equal("v0", GetValue(restored, "c0"));                 // always at or before the cut
        Assert.Equal(bothIncluded ? "v3" : null, GetValue(restored, "c3"));
    }

    [Fact]
    public void Coverage_InclusiveEndTarget_AtNewestMillisecond_IsAccepted()
    {
        // Chain covers [baseCut (0,100,0) .. newest ToHlc (0,200,0)]. A wall-clock target at ms 200 maps
        // to the inclusive end of that millisecond (counter = max). It must resolve, not be rejected as
        // "outside coverage" for sorting after the newest captured entry within the same millisecond.
        BackupManifest full = BackupManifest.CreateFull(
            [PartitionBackupRange.Create(1, 1, default, 5, new HLCTimestamp(0, 200, 0), 1)]);
        full.SetBaseCut(new HLCTimestamp(0, 100, 0));
        IReadOnlyList<BackupManifest> chain = [full];

        HLCTimestamp inclusiveEnd = new(0, 200, uint.MaxValue);
        Assert.Equal(inclusiveEnd, BackupChainCoverage.Resolve(chain, inclusiveEnd));

        // A target in a strictly later millisecond is still beyond coverage and refused.
        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            BackupChainCoverage.Resolve(chain, new HLCTimestamp(0, 201, 0)));
        Assert.True(ex.TargetOutsideCoverage);

        // A target below the base cut is still refused.
        BackupDriverException below = Assert.Throws<BackupDriverException>(() =>
            BackupChainCoverage.Resolve(chain, new HLCTimestamp(0, 99, uint.MaxValue)));
        Assert.True(below.TargetOutsideCoverage);
    }
}
